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

package com.alibaba.nacos.config.server.aspect;

import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.constant.CounterMode;
import com.alibaba.nacos.config.server.model.ConfigInfo;
import com.alibaba.nacos.config.server.model.capacity.Capacity;
import com.alibaba.nacos.config.server.service.capacity.CapacityService;
import com.alibaba.nacos.config.server.service.repository.PersistService;
import com.alibaba.nacos.config.server.utils.PropertyUtil;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.nio.charset.StandardCharsets;

import static com.alibaba.nacos.config.server.constant.Constants.LIMIT_ERROR_CODE;

/**
 * Capacity management aspect: batch write and update but don't process it.
 *
 * @author hexu.hxy
 * @date 2018/3/13
 */
@Aspect
public class CapacityManagementAspect {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(CapacityManagementAspect.class);
    
    private static final String SYNC_UPDATE_CONFIG_ALL =
            "execution(* com.alibaba.nacos.config.server.controller.ConfigController.publishConfig(..)) && args"
                    + "(request,response,dataId,group,content,appName,srcUser,tenant,tag,..)";
    
    private static final String DELETE_CONFIG =
            "execution(* com.alibaba.nacos.config.server.controller.ConfigController.deleteConfig(..)) && args"
                    + "(request,response,dataId,group,tenant,..)";
    
    @Autowired
    private CapacityService capacityService;
    
    @Autowired
    private PersistService persistService;
    
    /**
     * Need to judge the size of content whether to exceed the limitation.
     */
    @Around(SYNC_UPDATE_CONFIG_ALL)
    public Object aroundSyncUpdateConfigAll(ProceedingJoinPoint pjp, HttpServletRequest request,
            HttpServletResponse response, String dataId, String group, String content, String appName, String srcUser,
            String tenant, String tag) throws Throwable {
        if (!PropertyUtil.isManageCapacity()) {
            return pjp.proceed();
        }
        LOGGER.info("[capacityManagement] aroundSyncUpdateConfigAll");
        // 判断请求是否带有 betaIps 头
        String betaIps = request.getHeader("betaIps");
        // 如果没有 betaIps 头,则判断 tag 参数是否为空
        if (StringUtils.isBlank(betaIps)) {
            if (StringUtils.isBlank(tag)) {
                // tag 参数不为空
                // do capacity management limitation check for writing or updating config_info table.
                // 从数据库中根据 dataId,group,tenant 查询 config_info 表内容是否为空
                if (persistService.findConfigInfo(dataId, group, tenant) == null) {
                    // 如果没有查询到对应的内容则进行数据插入操作
                    // Write operation.
                    return do4Insert(pjp, request, response, group, tenant, content);
                }
                // 如果数据库中已经存在对应的数据则进行更新
                // Update operation.
                return do4Update(pjp, request, response, dataId, group, tenant, content);
            }
        }
        // 如果请求有带有 betaIps 的头直接执行
        // 或者请求没有带 betaIps 的头并且 tag 参数也为空时直接执行
        return pjp.proceed();
    }
    
    /**
     * Update operation: open the limitation of capacity management and it will check the size of content.
     *
     * @throws Throwable Throws Exception when actually operate.
     */
    private Object do4Update(ProceedingJoinPoint pjp, HttpServletRequest request, HttpServletResponse response,
            String dataId, String group, String tenant, String content) throws Throwable {
        if (!PropertyUtil.isCapacityLimitCheck()) {
            return pjp.proceed();
        }
        try {
            boolean hasTenant = hasTenant(tenant);
            Capacity capacity = getCapacity(group, tenant, hasTenant);
            // 判断是否超过 租户容量大小 或者 组容量大小
            if (isSizeLimited(group, tenant, getCurrentSize(content), hasTenant, false, capacity)) {
                return response4Limit(request, response, LimitType.OVER_MAX_SIZE);
            }
        } catch (Exception e) {
            LOGGER.error("[capacityManagement] do4Update ", e);
        }
        return pjp.proceed();
    }
    
    /**
     * Write operation. Step 1: count whether to open the limitation checking function for capacity management; Step 2:
     * open limitation checking capacity management and check size of content and quota;
     *
     * @throws Throwable Exception.
     */
    private Object do4Insert(ProceedingJoinPoint pjp, HttpServletRequest request, HttpServletResponse response,
            String group, String tenant, String content) throws Throwable {
        LOGGER.info("[capacityManagement] do4Insert");
        // 设置当前的模式为增加模式
        CounterMode counterMode = CounterMode.INCREMENT;
        // 判断是否具是有效的租户名称
        boolean hasTenant = hasTenant(tenant);
        // 判断是否打开容量限制检查
        if (PropertyUtil.isCapacityLimitCheck()) {
            // Write or update: usage + 1
            // 获得限制类型
            LimitType limitType = getLimitType(counterMode, group, tenant, content, hasTenant);
            if (limitType != null) {
                // 限制类型不为空,意味着超过 group 最大容量 或者 超过 tenant 最大容量
                // 响应 http 错误码
                return response4Limit(request, response, limitType);
            }
        } else {
            // Write or update: usage + 1
            // 不管容量限制直接更新:
            // 1. 更新集群使用容量
            // 2. 更新租户使用容量
            // 3. 更新组使用容量
            insertOrUpdateUsage(group, tenant, counterMode, hasTenant);
        }
        return getResult(pjp, response, group, tenant, counterMode, hasTenant);
    }
    
    private Object response4Limit(HttpServletRequest request, HttpServletResponse response, LimitType limitType) {
        response.setStatus(limitType.status);
        return String.valueOf(limitType.status);
    }
    
    private boolean hasTenant(String tenant) {
        return StringUtils.isNotBlank(tenant);
    }
    
    /**
     * The usage of capacity table for counting module will subtracte one whether open the limitation check of capacity
     * management.
     */
    @Around(DELETE_CONFIG)
    public Object aroundDeleteConfig(ProceedingJoinPoint pjp, HttpServletRequest request, HttpServletResponse response,
            String dataId, String group, String tenant) throws Throwable {
        if (!PropertyUtil.isManageCapacity()) {
            return pjp.proceed();
        }
        LOGGER.info("[capacityManagement] aroundDeleteConfig");
        // 查询数据库是否有相关的配置文件
        // 如果没有直接删除就是
        ConfigInfo configInfo = persistService.findConfigInfo(dataId, group, tenant);
        if (configInfo == null) {
            return pjp.proceed();
        }
        return do4Delete(pjp, response, group, tenant, configInfo);
    }
    
    /**
     * Delete Operation.
     *
     * @throws Throwable Exception.
     */
    private Object do4Delete(ProceedingJoinPoint pjp, HttpServletResponse response, String group, String tenant,
            ConfigInfo configInfo) throws Throwable {
        // 判断是否是租户
        boolean hasTenant = hasTenant(tenant);
        if (configInfo == null) {
            // "configInfo == null", has two possible points.
            // 1. Concurrently deletion.
            // 2. First, new sub configurations are added, and then all sub configurations are deleted.
            // At this time, the task (asynchronous) written to configinfo has not been executed.
            //
            // About 2 point, then it will execute to merge to write config_info's task orderly, and delete config_info's task.
            // Active modification of usage, when it happens to be in the above "merging to write config_info's task".
            // Modify usage when the task of info is finished, and usage = 1.
            // The following "delete config_info" task will not be executed with usage-1, because the request has already returned.
            // Therefore, it is necessary to modify the usage job regularly.
            // 修正 Usage 简单来说就是统计 config_info 表中 group 对应多少条数据
            // tenant 对应多少条数据, 然后更新到对应的表中 比如 group_capacity、tenant_capacity
            correctUsage(group, tenant, hasTenant);
            return pjp.proceed();
        }
        
        // The same record can be deleted concurrently. This interface can be deleted asynchronously(submit MergeDataTask
        // to MergeTaskProcessor for processing), It may lead to more than one decrease in usage.
        // Therefore, it is necessary to modify the usage job regularly.
        // 设置模式为减少模式
        CounterMode counterMode = CounterMode.DECREMENT;
        // 更新 cluster usage、group usage、tenant usage
        insertOrUpdateUsage(group, tenant, counterMode, hasTenant);
        return getResult(pjp, response, group, tenant, counterMode, hasTenant);
    }
    
    private void correctUsage(String group, String tenant, boolean hasTenant) {
        try {
            if (hasTenant) {
                LOGGER.info("[capacityManagement] correct usage, tenant: {}", tenant);
                capacityService.correctTenantUsage(tenant);
            } else {
                LOGGER.info("[capacityManagement] correct usage, group: {}", group);
                capacityService.correctGroupUsage(group);
            }
        } catch (Exception e) {
            LOGGER.error("[capacityManagement] correctUsage ", e);
        }
    }
    
    private Object getResult(ProceedingJoinPoint pjp, HttpServletResponse response, String group, String tenant,
            CounterMode counterMode, boolean hasTenant) throws Throwable {
        try {
            // Execute operation actually.
            Object result = pjp.proceed();
            // Execute whether to callback based on the sql operation result.
            doResult(counterMode, response, group, tenant, result, hasTenant);
            return result;
        } catch (Throwable throwable) {
            LOGGER.warn("[capacityManagement] inner operation throw exception, rollback, group: {}, tenant: {}", group,
                    tenant, throwable);
            rollback(counterMode, group, tenant, hasTenant);
            throw throwable;
        }
    }
    
    /**
     * Usage counting service: it will count whether the limitation check function will be open.
     */
    private void insertOrUpdateUsage(String group, String tenant, CounterMode counterMode, boolean hasTenant) {
        try {
            // 更新集群总使用大小
            capacityService.insertAndUpdateClusterUsage(counterMode, true);
            if (hasTenant) {
                // 更新租户总使用大小
                capacityService.insertAndUpdateTenantUsage(counterMode, tenant, true);
            } else {
                // 更新组总是用大小
                capacityService.insertAndUpdateGroupUsage(counterMode, group, true);
            }
        } catch (Exception e) {
            LOGGER.error("[capacityManagement] insertOrUpdateUsage ", e);
        }
    }
    
    private LimitType getLimitType(CounterMode counterMode, String group, String tenant, String content,
            boolean hasTenant) {
        try {
            // 插入或者更新数据
            boolean clusterLimited = !capacityService.insertAndUpdateClusterUsage(counterMode, false);
            // 判断是否操作成功
            if (clusterLimited) {
                LOGGER.warn("[capacityManagement] cluster capacity reaches quota.");
                // 返回限制类型为 超过配额限制
                return LimitType.OVER_CLUSTER_QUOTA;
            }
            // 判断配置内容是否为空,为空则返回空
            if (content == null) {
                return null;
            }
            // 获取当前配置内容大小 byte 数
            int currentSize = getCurrentSize(content);
            // 获取租户或者组的限制类型
            LimitType limitType = getGroupOrTenantLimitType(counterMode, group, tenant, currentSize, hasTenant);
            // 如果限制类型不为空,说明可能会超出组配额或者租户配额
            if (limitType != null) {
                // 回滚集群的总使用大小
                rollbackClusterUsage(counterMode);
                // 返回限制类型
                return limitType;
            }
        } catch (Exception e) {
            LOGGER.error("[capacityManagement] isLimited ", e);
        }
        return null;
    }
    
    /**
     * Get and return the byte size of encoding.
     */
    private int getCurrentSize(String content) {
        try {
            return content.getBytes(StandardCharsets.UTF_8).length;
        } catch (Exception e) {
            LOGGER.error("[capacityManagement] getCurrentSize ", e);
        }
        return 0;
    }
    
    private LimitType getGroupOrTenantLimitType(CounterMode counterMode, String group, String tenant, int currentSize,
            boolean hasTenant) {
        // 如果 group 为空则返回为空
        // 因为后面获取容量要根据 group 去查
        if (group == null) {
            return null;
        }
        // 从数据库中获取容量
        Capacity capacity = getCapacity(group, tenant, hasTenant);
        // 判断内容是否超过容量大小
        if (isSizeLimited(group, tenant, currentSize, hasTenant, false, capacity)) {
            // 如果超过这返回查过最大容量
            return LimitType.OVER_MAX_SIZE;
        }
        // 如果 capacity 为空则新增容量数据
        if (capacity == null) {
            // 如果 capacity 为空则为 租户 或者 组提供一个默认数据
            // 插入数据库
            insertCapacity(group, tenant, hasTenant);
        }
        // 判断是否更新成功
        boolean updateSuccess = isUpdateSuccess(counterMode, group, tenant, hasTenant);
        if (updateSuccess) {
            return null;
        }
        if (hasTenant) {
            // 超过租户配额限制
            return LimitType.OVER_TENANT_QUOTA;
        }
        // 超过组配额限制
        return LimitType.OVER_GROUP_QUOTA;
    }
    
    private boolean isUpdateSuccess(CounterMode counterMode, String group, String tenant, boolean hasTenant) {
        boolean updateSuccess;
        if (hasTenant) {
            // 更新租户使用容量
            updateSuccess = capacityService.updateTenantUsage(counterMode, tenant);
            if (!updateSuccess) {
                LOGGER.warn("[capacityManagement] tenant capacity reaches quota, tenant: {}", tenant);
            }
        } else {
            // 更新组使用容量
            updateSuccess = capacityService.updateGroupUsage(counterMode, group);
            if (!updateSuccess) {
                LOGGER.warn("[capacityManagement] group capacity reaches quota, group: {}", group);
            }
        }
        return updateSuccess;
    }
    
    private void insertCapacity(String group, String tenant, boolean hasTenant) {
        if (hasTenant) {
            capacityService.initTenantCapacity(tenant);
        } else {
            capacityService.initGroupCapacity(group);
        }
    }
    
    private Capacity getCapacity(String group, String tenant, boolean hasTenant) {
        Capacity capacity;
        // 判断是否是有效的租户
        if (hasTenant) {
            // 如果是有效的租户,则查询指定租户的容量
            // 查询 tenant_capacity 表
            capacity = capacityService.getTenantCapacity(tenant);
        } else {
            // 如果租户无效,则查询指定 group 的容量
            // 查询 group_capacity 表
            capacity = capacityService.getGroupCapacity(group);
        }
        // 返回容量
        return capacity;
    }
    
    private boolean isSizeLimited(String group, String tenant, int currentSize, boolean hasTenant, boolean isAggr,
            Capacity capacity) {
        // 这里判断的逻辑是 判断内容大小是否超过数据库设置的最大大小
        // 如果数据库中的默认最大大小为0 或者 没有相关数据则使用默认值来进行比较
        // 获取默认最大容量
        int defaultMaxSize = getDefaultMaxSize(isAggr);
        // 判断容量数据是否为空,也就是说数据库中 group_capacity 或者 tenant_capacity 无数据
        if (capacity != null) {
            // 如果容量不为空,获取数据库中的最大值 MaxAggrSize 或者 MaxSize
            Integer maxSize = getMaxSize(isAggr, capacity);
            // 数据库中的记录的最大值为0 则使用默认容量
            if (maxSize == 0) {
                // If there exists capacity info and maxSize = 0, then it uses maxSize limitation default value to compare.
                return isOverSize(group, tenant, currentSize, defaultMaxSize, hasTenant);
            }
            // 判断是否超过最大值
            // If there exists capacity info, then maxSize!=0.
            return isOverSize(group, tenant, currentSize, maxSize, hasTenant);
        }
        // 判断是否超过默认值
        // If there no exists capacity info, then it uses maxSize limitation default value to compare.
        return isOverSize(group, tenant, currentSize, defaultMaxSize, hasTenant);
    }
    
    private Integer getMaxSize(boolean isAggr, Capacity capacity) {
        if (isAggr) {
            return capacity.getMaxAggrSize();
        }
        return capacity.getMaxSize();
    }
    
    private int getDefaultMaxSize(boolean isAggr) {
        // 判断是否是聚合
        if (isAggr) {
            // 获取默认的最大聚合容量
            return PropertyUtil.getDefaultMaxAggrSize();
        }
        // 获取默认最大容量
        return PropertyUtil.getDefaultMaxSize();
    }
    
    private boolean isOverSize(String group, String tenant, int currentSize, int maxSize, boolean hasTenant) {
        if (currentSize > maxSize) {
            if (hasTenant) {
                LOGGER.warn(
                        "[capacityManagement] tenant content is over maxSize, tenant: {}, maxSize: {}, currentSize: {}",
                        tenant, maxSize, currentSize);
            } else {
                LOGGER.warn(
                        "[capacityManagement] group content is over maxSize, group: {}, maxSize: {}, currentSize: {}",
                        group, maxSize, currentSize);
            }
            return true;
        }
        return false;
    }
    
    private void doResult(CounterMode counterMode, HttpServletResponse response, String group, String tenant,
            Object result, boolean hasTenant) {
        try {
            // 判断是否更新失败,如果更新失败
            // 关于容量的大小会进行回滚
            if (!isSuccess(response, result)) {
                LOGGER.warn(
                        "[capacityManagement] inner operation is fail, rollback, counterMode: {}, group: {}, tenant: {}",
                        counterMode, group, tenant);
                // 回滚容量
                // 回滚集群使用容量
                // 回滚租户使用容量
                // 回滚组使用容量
                rollback(counterMode, group, tenant, hasTenant);
            }
        } catch (Exception e) {
            LOGGER.error("[capacityManagement] doResult ", e);
        }
    }
    
    private boolean isSuccess(HttpServletResponse response, Object result) {
        int status = response.getStatus();
        if (status == HttpServletResponse.SC_OK) {
            return true;
        }
        LOGGER.warn("[capacityManagement] response status is not 200, status: {}, result: {}", status, result);
        return false;
    }
    
    private void rollback(CounterMode counterMode, String group, String tenant, boolean hasTenant) {
        try {
            rollbackClusterUsage(counterMode);
            if (hasTenant) {
                capacityService.updateTenantUsage(counterMode.reverse(), tenant);
            } else {
                capacityService.updateGroupUsage(counterMode.reverse(), group);
            }
        } catch (Exception e) {
            LOGGER.error("[capacityManagement] rollback ", e);
        }
    }
    
    private void rollbackClusterUsage(CounterMode counterMode) {
        try {
            if (!capacityService.updateClusterUsage(counterMode.reverse())) {
                LOGGER.error("[capacityManagement] cluster usage rollback fail counterMode: {}", counterMode);
            }
        } catch (Exception e) {
            LOGGER.error("[capacityManagement] rollback ", e);
        }
    }
    
    /**
     * limit type.
     *
     * @author Nacos.
     */
    public enum LimitType {
        /**
         * over limit.
         */
        OVER_CLUSTER_QUOTA("超过集群配置个数上限", LIMIT_ERROR_CODE),
        OVER_GROUP_QUOTA("超过该Group配置个数上限", LIMIT_ERROR_CODE),
        OVER_TENANT_QUOTA("超过该租户配置个数上限", LIMIT_ERROR_CODE),
        OVER_MAX_SIZE("超过配置的内容大小上限", LIMIT_ERROR_CODE);
        
        public final String description;
        
        public final int status;
        
        LimitType(String description, int status) {
            this.description = description;
            this.status = status;
        }
    }
}
