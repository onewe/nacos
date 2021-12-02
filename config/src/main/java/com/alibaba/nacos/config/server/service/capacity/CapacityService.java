/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.config.server.service.capacity;

import com.alibaba.nacos.config.server.constant.CounterMode;
import com.alibaba.nacos.config.server.model.capacity.Capacity;
import com.alibaba.nacos.config.server.model.capacity.GroupCapacity;
import com.alibaba.nacos.config.server.model.capacity.TenantCapacity;
import com.alibaba.nacos.config.server.service.repository.PersistService;
import com.alibaba.nacos.config.server.utils.ConfigExecutor;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.alibaba.nacos.config.server.utils.PropertyUtil;
import com.alibaba.nacos.config.server.utils.TimeUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import javax.annotation.PostConstruct;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Capacity service.
 *
 * @author hexu.hxy
 * @date 2018/03/05
 */
@Service
public class CapacityService {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(CapacityService.class);
    
    private static final Integer ZERO = 0;
    
    private static final int INIT_PAGE_SIZE = 500;
    
    @Autowired
    private GroupCapacityPersistService groupCapacityPersistService;
    
    @Autowired
    private TenantCapacityPersistService tenantCapacityPersistService;
    
    @Autowired
    private PersistService persistService;
    
    /**
     * Init.
     */
    @PostConstruct
    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    public void init() {
        // All servers have jobs that modify usage, idempotent.
        ConfigExecutor.scheduleCorrectUsageTask(() -> {
            LOGGER.info("[capacityManagement] start correct usage");
            StopWatch watch = new StopWatch();
            watch.start();
            correctUsage();
            watch.stop();
            LOGGER.info("[capacityManagement] end correct usage, cost: {}s", watch.getTotalTimeSeconds());
            
        }, PropertyUtil.getCorrectUsageDelay(), PropertyUtil.getCorrectUsageDelay(), TimeUnit.SECONDS);
    }
    
    public void correctUsage() {
        correctGroupUsage();
        correctTenantUsage();
    }
    
    /**
     * Correct the usage of group capacity.
     */
    private void correctGroupUsage() {
        long lastId = 0;
        int pageSize = 100;
        while (true) {
            List<GroupCapacity> groupCapacityList = groupCapacityPersistService
                    .getCapacityList4CorrectUsage(lastId, pageSize);
            if (groupCapacityList.isEmpty()) {
                break;
            }
            lastId = groupCapacityList.get(groupCapacityList.size() - 1).getId();
            for (GroupCapacity groupCapacity : groupCapacityList) {
                String group = groupCapacity.getGroup();
                groupCapacityPersistService.correctUsage(group, TimeUtils.getCurrentTime());
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // ignore
                // set the interrupted flag
                Thread.currentThread().interrupt();
            }
        }
    }
    
    public void correctGroupUsage(String group) {
        groupCapacityPersistService.correctUsage(group, TimeUtils.getCurrentTime());
    }
    
    public void correctTenantUsage(String tenant) {
        tenantCapacityPersistService.correctUsage(tenant, TimeUtils.getCurrentTime());
    }
    
    /**
     * Correct the usage of group capacity.
     */
    private void correctTenantUsage() {
        long lastId = 0;
        int pageSize = 100;
        while (true) {
            List<TenantCapacity> tenantCapacityList = tenantCapacityPersistService
                    .getCapacityList4CorrectUsage(lastId, pageSize);
            if (tenantCapacityList.isEmpty()) {
                break;
            }
            lastId = tenantCapacityList.get(tenantCapacityList.size() - 1).getId();
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
            for (TenantCapacity tenantCapacity : tenantCapacityList) {
                String tenant = tenantCapacity.getTenant();
                tenantCapacityPersistService.correctUsage(tenant, TimeUtils.getCurrentTime());
            }
        }
    }
    
    public void initAllCapacity() {
        initAllCapacity(false);
        initAllCapacity(true);
    }
    
    private void initAllCapacity(boolean isTenant) {
        int page = 1;
        while (true) {
            List<String> list;
            if (isTenant) {
                list = persistService.getTenantIdList(page, INIT_PAGE_SIZE);
            } else {
                list = persistService.getGroupIdList(page, INIT_PAGE_SIZE);
            }
            for (String targetId : list) {
                if (isTenant) {
                    insertTenantCapacity(targetId);
                    autoExpansion(null, targetId);
                } else {
                    insertGroupCapacity(targetId);
                    autoExpansion(targetId, null);
                }
            }
            if (list.size() < INIT_PAGE_SIZE) {
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
            ++page;
        }
    }
    
    /**
     * To Cluster. 1.If the capacity information does not exist, initialize the capacity information. 2.Update capacity
     * usage, plus or minus one.
     *
     * @param counterMode      increase or decrease mode.
     * @param ignoreQuotaLimit ignoreQuotaLimit flag.
     * @return the result of update cluster usage.
     */
    public boolean insertAndUpdateClusterUsage(CounterMode counterMode, boolean ignoreQuotaLimit) {
        // 获取集群的容量配置 也就是查询 group_capacity 表
        // 通过 group_id 查询
        Capacity capacity = groupCapacityPersistService.getClusterCapacity();
        // 判断数据库中是否有相关的容量记录
        if (capacity == null) {
            // 如果没有相关的容量记录则插入初始化数据
            // 初始化数据中的 Quota、MaxSize、AggrCount、AggrSize默认为 0
            insertGroupCapacity(GroupCapacityPersistService.CLUSTER);
        }
        // 更新 group_capacity 使用数据
        return updateGroupUsage(counterMode, GroupCapacityPersistService.CLUSTER, PropertyUtil.getDefaultClusterQuota(),
                ignoreQuotaLimit);
    }
    
    public boolean updateClusterUsage(CounterMode counterMode) {
        return updateGroupUsage(counterMode, GroupCapacityPersistService.CLUSTER, PropertyUtil.getDefaultClusterQuota(),
                false);
    }
    
    /**
     * It is used for counting when the limit check function of capacity management is turned off. 1.If the capacity
     * information does not exist, initialize the capacity information. 2.Update capacity usage, plus or minus one.
     *
     * @param counterMode      increase or decrease mode.
     * @param group            tenant string value.
     * @param ignoreQuotaLimit ignoreQuotaLimit flag.
     * @return operate successfully or not.
     */
    public boolean insertAndUpdateGroupUsage(CounterMode counterMode, String group, boolean ignoreQuotaLimit) {
        GroupCapacity groupCapacity = getGroupCapacity(group);
        if (groupCapacity == null) {
            initGroupCapacity(group, null, null, null, null);
        }
        return updateGroupUsage(counterMode, group, PropertyUtil.getDefaultGroupQuota(), ignoreQuotaLimit);
    }
    
    public boolean updateGroupUsage(CounterMode counterMode, String group) {
        return updateGroupUsage(counterMode, group, PropertyUtil.getDefaultGroupQuota(), false);
    }
    
    private boolean updateGroupUsage(CounterMode counterMode, String group, int defaultQuota,
            boolean ignoreQuotaLimit) {
        final Timestamp now = TimeUtils.getCurrentTime();
        GroupCapacity groupCapacity = new GroupCapacity();
        groupCapacity.setGroup(group);
        groupCapacity.setQuota(defaultQuota);
        groupCapacity.setGmtModified(now);
        // 判断模式是否是 INCREMENT 模式, INCREMENT 模式会进行插入数据
        if (CounterMode.INCREMENT == counterMode) {
            // 判断是否忽略容量限制
            if (ignoreQuotaLimit) {
                // group_capacity usage 加一
                return groupCapacityPersistService.incrementUsage(groupCapacity);
            }
            // First update the quota according to the default value. In most cases, it is the default value.
            // The quota field in the default value table is 0
            //
            // 第一个条件执行的 sql 是 group_capacity usage 加一 并且 usage 小于 quota(总配额),并且默认 quota(数据库中的) 为0
            // 第二个条件执行的 sql 是 group_capacity usage 加一 并且 usage 小于 quota(总配额), 并且 quota 不是默认值 0
            // 这两句话的意思为先更新默认的数据,如果更新不到就更新非默认数据
            // 因为容量数据要么是默认数据要么就是非默认数据,有且只有一条
            return groupCapacityPersistService.incrementUsageWithDefaultQuotaLimit(groupCapacity)
                    || groupCapacityPersistService.incrementUsageWithQuotaLimit(groupCapacity);
        }
        // 更新数据如果模式为非 INCREMENT 则把 usage 减1,并且 usage 不能为0
        return groupCapacityPersistService.decrementUsage(groupCapacity);
    }
    
    public GroupCapacity getGroupCapacity(String group) {
        return groupCapacityPersistService.getGroupCapacity(group);
    }
    
    /**
     * Initialize the capacity information of the group. If the quota is reached, the capacity will be automatically
     * expanded to reduce the operation and maintenance cost.
     *
     * @param group group string value.
     * @return init result.
     */
    public boolean initGroupCapacity(String group) {
        return initGroupCapacity(group, null, null, null, null);
    }
    
    /**
     * Initialize the capacity information of the group. If the quota is reached, the capacity will be automatically
     * expanded to reduce the operation and maintenance cost.
     *
     * @param group        group string value.
     * @param quota        quota int value.
     * @param maxSize      maxSize int value.
     * @param maxAggrCount maxAggrCount int value.
     * @param maxAggrSize  maxAggrSize int value.
     * @return init result.
     */
    private boolean initGroupCapacity(String group, Integer quota, Integer maxSize, Integer maxAggrCount,
            Integer maxAggrSize) {
        boolean insertSuccess = insertGroupCapacity(group, quota, maxSize, maxAggrCount, maxAggrSize);
        if (quota != null) {
            return insertSuccess;
        }
        autoExpansion(group, null);
        return insertSuccess;
    }
    
    /**
     * Expand capacity automatically.
     *
     * @param group  group string value.
     * @param tenant tenant string value.
     */
    private void autoExpansion(String group, String tenant) {
        Capacity capacity = getCapacity(group, tenant);
        int defaultQuota = getDefaultQuota(tenant != null);
        Integer usage = capacity.getUsage();
        if (usage < defaultQuota) {
            return;
        }
        // Initialize the capacity information of the group. If the quota is reached,
        // the capacity will be automatically expanded to reduce the operation and maintenance cost.
        int initialExpansionPercent = PropertyUtil.getInitialExpansionPercent();
        if (initialExpansionPercent > 0) {
            int finalQuota = (int) (usage + defaultQuota * (1.0 * initialExpansionPercent / 100));
            if (tenant != null) {
                tenantCapacityPersistService.updateQuota(tenant, finalQuota);
                LogUtil.DEFAULT_LOG.warn("[capacityManagement] The usage({}) already reach the upper limit({}) when init the tenant({}), "
                        + "automatic upgrade to ({})", usage, defaultQuota, tenant, finalQuota);
            } else {
                groupCapacityPersistService.updateQuota(group, finalQuota);
                LogUtil.DEFAULT_LOG.warn("[capacityManagement] The usage({}) already reach the upper limit({}) when init the group({}), "
                        + "automatic upgrade to ({})", usage, defaultQuota, group, finalQuota);
            }
        }
    }
    
    private int getDefaultQuota(boolean isTenant) {
        if (isTenant) {
            return PropertyUtil.getDefaultTenantQuota();
        }
        return PropertyUtil.getDefaultGroupQuota();
    }
    
    public Capacity getCapacity(String group, String tenant) {
        if (tenant != null) {
            return getTenantCapacity(tenant);
        }
        return getGroupCapacity(group);
    }
    
    public Capacity getCapacityWithDefault(String group, String tenant) {
        Capacity capacity;
        // 判断是否是租户
        boolean isTenant = StringUtils.isNotBlank(tenant);
        if (isTenant) {
            // 获取租户的容量
            capacity = getTenantCapacity(tenant);
        } else {
            // 获取组的容量
            capacity = getGroupCapacity(group);
        }
        // 如果数据库中没有数据则返回空
        if (capacity == null) {
            return null;
        }
        // 获取配额
        Integer quota = capacity.getQuota();
        // 如果配额是0则使用默认值
        if (quota == 0) {
            if (isTenant) {
                // 设置租户默认的配额的值
                capacity.setQuota(PropertyUtil.getDefaultTenantQuota());
            } else {
                // 判断是否是集群组
                if (GroupCapacityPersistService.CLUSTER.equals(group)) {
                    // 设置默认的集群配额
                    capacity.setQuota(PropertyUtil.getDefaultClusterQuota());
                } else {
                    // 设置默认的组配额
                    capacity.setQuota(PropertyUtil.getDefaultGroupQuota());
                }
            }
        }
        // 获取内容最大限制
        Integer maxSize = capacity.getMaxSize();
        // 如果为0 则使用默认的内容最大值
        if (maxSize == 0) {
            capacity.setMaxSize(PropertyUtil.getDefaultMaxSize());
        }
        // 获取最大聚合数量
        Integer maxAggrCount = capacity.getMaxAggrCount();
        // 如果为0 则使用默认的最大聚合数量
        if (maxAggrCount == 0) {
            capacity.setMaxAggrCount(PropertyUtil.getDefaultMaxAggrCount());
        }
        // 获取最大聚合内容大小
        Integer maxAggrSize = capacity.getMaxAggrSize();
        // 如果为0 则使用默认的聚合内容的最大值
        if (maxAggrSize == 0) {
            capacity.setMaxAggrSize(PropertyUtil.getDefaultMaxAggrSize());
        }
        return capacity;
    }
    
    /**
     * Init capacity.
     *
     * @param group  group string value.
     * @param tenant tenant string value.
     * @return init result.
     */
    public boolean initCapacity(String group, String tenant) {
        if (StringUtils.isNotBlank(tenant)) {
            return initTenantCapacity(tenant);
        }
        if (GroupCapacityPersistService.CLUSTER.equals(group)) {
            return insertGroupCapacity(GroupCapacityPersistService.CLUSTER);
        }
        // Group can expand capacity automatically.
        return initGroupCapacity(group);
    }
    
    private boolean insertGroupCapacity(String group) {
        return insertGroupCapacity(group, null, null, null, null);
    }
    
    private boolean insertGroupCapacity(String group, Integer quota, Integer maxSize, Integer maxAggrCount,
            Integer maxAggrSize) {
        try {
            final Timestamp now = TimeUtils.getCurrentTime();
            GroupCapacity groupCapacity = new GroupCapacity();
            groupCapacity.setGroup(group);
            // When adding a new quota, quota = 0 means that the quota is the default value.
            // In order to update the default quota, only the Nacos configuration needs to be modified,
            // and most of the data in the table need not be updated.
            groupCapacity.setQuota(quota == null ? ZERO : quota);
            
            // When adding new data, maxsize = 0 means that the size is the default value.
            // In order to update the default size, you only need to modify the Nacos configuration without updating most of the data in the table.
            groupCapacity.setMaxSize(maxSize == null ? ZERO : maxSize);
            groupCapacity.setMaxAggrCount(maxAggrCount == null ? ZERO : maxAggrCount);
            groupCapacity.setMaxAggrSize(maxAggrSize == null ? ZERO : maxAggrSize);
            groupCapacity.setGmtCreate(now);
            groupCapacity.setGmtModified(now);
            return groupCapacityPersistService.insertGroupCapacity(groupCapacity);
        } catch (DuplicateKeyException e) {
            // this exception will meet when concurrent insert，ignore it
            LogUtil.DEFAULT_LOG.warn("group: {}, message: {}", group, e.getMessage());
        }
        return false;
    }
    
    /**
     * It is used for counting when the limit check function of capacity management is turned off. 1.If the capacity
     * information does not exist, initialize the capacity information. 2.Update capacity usage, plus or minus one.
     *
     * @param counterMode      increase or decrease mode.
     * @param tenant           tenant string value.
     * @param ignoreQuotaLimit ignoreQuotaLimit flag.
     * @return operate successfully or not.
     */
    public boolean insertAndUpdateTenantUsage(CounterMode counterMode, String tenant, boolean ignoreQuotaLimit) {
        TenantCapacity tenantCapacity = getTenantCapacity(tenant);
        if (tenantCapacity == null) {
            // Init capacity information.
            initTenantCapacity(tenant);
        }
        return updateTenantUsage(counterMode, tenant, ignoreQuotaLimit);
    }
    
    private boolean updateTenantUsage(CounterMode counterMode, String tenant, boolean ignoreQuotaLimit) {
        final Timestamp now = TimeUtils.getCurrentTime();
        TenantCapacity tenantCapacity = new TenantCapacity();
        tenantCapacity.setTenant(tenant);
        tenantCapacity.setQuota(PropertyUtil.getDefaultTenantQuota());
        tenantCapacity.setGmtModified(now);
        if (CounterMode.INCREMENT == counterMode) {
            if (ignoreQuotaLimit) {
                return tenantCapacityPersistService.incrementUsage(tenantCapacity);
            }
            // First update the quota according to the default value. In most cases, it is the default value.
            // The quota field in the default value table is 0.
            return tenantCapacityPersistService.incrementUsageWithDefaultQuotaLimit(tenantCapacity)
                    || tenantCapacityPersistService.incrementUsageWithQuotaLimit(tenantCapacity);
        }
        return tenantCapacityPersistService.decrementUsage(tenantCapacity);
    }
    
    public boolean updateTenantUsage(CounterMode counterMode, String tenant) {
        return updateTenantUsage(counterMode, tenant, false);
    }
    
    /**
     * Initialize the capacity information of the tenant. If the quota is reached, the capacity will be automatically
     * expanded to reduce the operation and maintenance cos.
     *
     * @param tenant tenant string value.
     * @return init result.
     */
    public boolean initTenantCapacity(String tenant) {
        return initTenantCapacity(tenant, null, null, null, null);
    }
    
    /**
     * Initialize the capacity information of the tenant. If the quota is reached, the capacity will be automatically
     * expanded to reduce the operation and maintenance cost
     *
     * @param tenant       tenant string value.
     * @param quota        quota int value.
     * @param maxSize      maxSize int value.
     * @param maxAggrCount maxAggrCount int value.
     * @param maxAggrSize  maxAggrSize int value.
     * @return
     */
    public boolean initTenantCapacity(String tenant, Integer quota, Integer maxSize, Integer maxAggrCount,
            Integer maxAggrSize) {
        boolean insertSuccess = insertTenantCapacity(tenant, quota, maxSize, maxAggrCount, maxAggrSize);
        if (quota != null) {
            return insertSuccess;
        }
        autoExpansion(null, tenant);
        return insertSuccess;
    }
    
    private boolean insertTenantCapacity(String tenant) {
        return insertTenantCapacity(tenant, null, null, null, null);
    }
    
    private boolean insertTenantCapacity(String tenant, Integer quota, Integer maxSize, Integer maxAggrCount,
            Integer maxAggrSize) {
        try {
            final Timestamp now = TimeUtils.getCurrentTime();
            TenantCapacity tenantCapacity = new TenantCapacity();
            tenantCapacity.setTenant(tenant);
            // When adding a new quota, quota = 0 means that the quota is the default value.
            // In order to update the default quota, only the Nacos configuration needs to be modified,
            // and most of the data in the table need not be updated.
            tenantCapacity.setQuota(quota == null ? ZERO : quota);
            
            // When adding new data, maxsize = 0 means that the size is the default value.
            // In order to update the default size, you only need to modify the Nacos configuration without updating most of the data in the table.
            tenantCapacity.setMaxSize(maxSize == null ? ZERO : maxSize);
            tenantCapacity.setMaxAggrCount(maxAggrCount == null ? ZERO : maxAggrCount);
            tenantCapacity.setMaxAggrSize(maxAggrSize == null ? ZERO : maxAggrSize);
            tenantCapacity.setGmtCreate(now);
            tenantCapacity.setGmtModified(now);
            return tenantCapacityPersistService.insertTenantCapacity(tenantCapacity);
        } catch (DuplicateKeyException e) {
            // this exception will meet when concurrent insert，ignore it
            LogUtil.DEFAULT_LOG.warn("tenant: {}, message: {}", tenant, e.getMessage());
        }
        return false;
    }
    
    public TenantCapacity getTenantCapacity(String tenant) {
        return tenantCapacityPersistService.getTenantCapacity(tenant);
    }
    
    /**
     * Support for API interface, Tenant: initialize if the record does not exist, and update the capacity quota or
     * content size directly if it exists.
     *
     * @param group        group string value.
     * @param tenant       tenant string value.
     * @param quota        quota int value.
     * @param maxSize      maxSize int value.
     * @param maxAggrCount maxAggrCount int value.
     * @param maxAggrSize  maxAggrSize int value.
     * @return operate successfully or not.
     */
    public boolean insertOrUpdateCapacity(String group, String tenant, Integer quota, Integer maxSize,
            Integer maxAggrCount, Integer maxAggrSize) {
        // 如果是租户则更新租户容量相关数据
        if (StringUtils.isNotBlank(tenant)) {
            Capacity capacity = tenantCapacityPersistService.getTenantCapacity(tenant);
            if (capacity == null) {
                // 如果租户没有容量的数据 则先进行初始化
                return initTenantCapacity(tenant, quota, maxSize, maxAggrCount, maxAggrSize);
            }
            // 更新租户数据
            return tenantCapacityPersistService.updateTenantCapacity(tenant, quota, maxSize, maxAggrCount, maxAggrSize);
        }
        // 获取组的容量数据
        Capacity capacity = groupCapacityPersistService.getGroupCapacity(group);
        if (capacity == null) {
            // 如果组没有容量的数据 则先进行初始化
            return initGroupCapacity(group, quota, maxSize, maxAggrCount, maxAggrSize);
        }
        // 更新组的数据
        return groupCapacityPersistService.updateGroupCapacity(group, quota, maxSize, maxAggrCount, maxAggrSize);
    }
}
