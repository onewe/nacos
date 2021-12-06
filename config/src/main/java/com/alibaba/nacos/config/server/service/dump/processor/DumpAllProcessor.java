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

package com.alibaba.nacos.config.server.service.dump.processor;

import com.alibaba.nacos.common.task.NacosTask;
import com.alibaba.nacos.common.utils.MD5Utils;
import com.alibaba.nacos.config.server.constant.Constants;
import com.alibaba.nacos.common.task.NacosTaskProcessor;
import com.alibaba.nacos.config.server.model.ConfigInfoWrapper;
import com.alibaba.nacos.config.server.model.Page;
import com.alibaba.nacos.config.server.service.AggrWhitelist;
import com.alibaba.nacos.config.server.service.ClientIpWhiteList;
import com.alibaba.nacos.config.server.service.ConfigCacheService;
import com.alibaba.nacos.config.server.service.SwitchService;
import com.alibaba.nacos.config.server.service.dump.DumpService;
import com.alibaba.nacos.config.server.service.repository.PersistService;
import com.alibaba.nacos.config.server.utils.GroupKey2;
import com.alibaba.nacos.config.server.utils.LogUtil;

import static com.alibaba.nacos.config.server.utils.LogUtil.DEFAULT_LOG;

/**
 * Dump all processor.
 *
 * @author Nacos
 * @date 2020/7/5 12:19 PM
 */
public class DumpAllProcessor implements NacosTaskProcessor {
    
    public DumpAllProcessor(DumpService dumpService) {
        this.dumpService = dumpService;
        this.persistService = dumpService.getPersistService();
    }
    
    @Override
    public boolean process(NacosTask task) {
        // 从数据库中获取最大序列值
        long currentMaxId = persistService.findConfigMaxId();
        long lastMaxId = 0;
        while (lastMaxId < currentMaxId) {
            // 从数据库中查询大于 lastMaxId 的数据, 相当于就是查询所有了
            Page<ConfigInfoWrapper> page = persistService.findAllConfigInfoFragment(lastMaxId, PAGE_SIZE);
            // 判断查询的数据是否为空
            if (page != null && page.getPageItems() != null && !page.getPageItems().isEmpty()) {
                // 遍历查询出来的数据
                for (ConfigInfoWrapper cf : page.getPageItems()) {
                    // 获取 id
                    long id = cf.getId();
                    // 判断 lastMaxId 是否大于 当前数据的 id
                    // 谁的 id 大就设置谁
                    lastMaxId = Math.max(id, lastMaxId);
                    // 通过 dataId 判断是否是 com.alibaba.nacos.metadata.aggrIDs
                    if (cf.getDataId().equals(AggrWhitelist.AGGRIDS_METADATA)) {
                        // 加载 AggrWhitelist 中的正则表达式
                        AggrWhitelist.load(cf.getContent());
                    }
                    
                    // 通过 dataId 判断是否是 com.alibaba.nacos.metadata.clientIpWhitelist
                    if (cf.getDataId().equals(ClientIpWhiteList.CLIENT_IP_WHITELIST_METADATA)) {
                        // 加载 aclIp 列表
                        ClientIpWhiteList.load(cf.getContent());
                    }
                    
                    // 通过 dataId 判断是否是 com.alibaba.nacos.meta.switch
                    if (cf.getDataId().equals(SwitchService.SWITCH_META_DATAID)) {
                        // 加载 switches
                        SwitchService.load(cf.getContent());
                    }
    
                    ConfigCacheService.dump(cf.getDataId(), cf.getGroup(), cf.getTenant(), cf.getContent(),
                            cf.getLastModified(), cf.getType(), cf.getEncryptedDataKey());
                    
                    final String content = cf.getContent();
                    // TODO 这里需要优化, 没必要在进行一次 MD5
                    final String md5 = MD5Utils.md5Hex(content, Constants.ENCODE);
                    LogUtil.DUMP_LOG.info("[dump-all-ok] {}, {}, length={}, md5={}",
                            GroupKey2.getKey(cf.getDataId(), cf.getGroup()), cf.getLastModified(), content.length(),
                            md5);
                }
                DEFAULT_LOG.info("[all-dump] {} / {}", lastMaxId, currentMaxId);
            } else {
                lastMaxId += PAGE_SIZE;
            }
        }
        return true;
    }
    
    static final int PAGE_SIZE = 1000;
    
    final DumpService dumpService;
    
    final PersistService persistService;
}
