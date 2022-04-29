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

package com.alibaba.nacos.core.distributed.distro.task.verify;

import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.distributed.distro.component.DistroComponentHolder;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataStorage;
import com.alibaba.nacos.core.distributed.distro.component.DistroTransportAgent;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.task.execute.DistroExecuteTaskExecuteEngine;
import com.alibaba.nacos.core.utils.Loggers;

import java.util.List;

/**
 * Timed to start distro verify task.
 *
 * @author xiweng.yy
 */
public class DistroVerifyTimedTask implements Runnable {
    
    private final ServerMemberManager serverMemberManager;
    
    private final DistroComponentHolder distroComponentHolder;
    
    private final DistroExecuteTaskExecuteEngine executeTaskExecuteEngine;
    
    public DistroVerifyTimedTask(ServerMemberManager serverMemberManager, DistroComponentHolder distroComponentHolder,
            DistroExecuteTaskExecuteEngine executeTaskExecuteEngine) {
        this.serverMemberManager = serverMemberManager;
        this.distroComponentHolder = distroComponentHolder;
        this.executeTaskExecuteEngine = executeTaskExecuteEngine;
    }
    
    @Override
    public void run() {
        try {
            // 获取除了自身之外的节点列表
            List<Member> targetServer = serverMemberManager.allMembersWithoutSelf();
            if (Loggers.DISTRO.isDebugEnabled()) {
                Loggers.DISTRO.debug("server list is: {}", targetServer);
            }
            // 遍历注册的数据存储类型
            // com.alibaba.nacos.naming.iplist 和 Nacos:Naming:v2:ClientData
            for (String each : distroComponentHolder.getDataStorageTypes()) {
                // 数据存储验证
                verifyForDataStorage(each, targetServer);
            }
        } catch (Exception e) {
            Loggers.DISTRO.error("[DISTRO-FAILED] verify task failed.", e);
        }
    }
    
    private void verifyForDataStorage(String type, List<Member> targetServer) {
        // 通过指定类型获取 distro 协议的数据存储
        DistroDataStorage dataStorage = distroComponentHolder.findDataStorage(type);
        // 判断存储是否完成初始化
        if (!dataStorage.isFinishInitial()) {
            Loggers.DISTRO.warn("data storage {} has not finished initial step, do not send verify data",
                    dataStorage.getClass().getSimpleName());
            return;
        }
        // 获取验证数据
        List<DistroData> verifyData = dataStorage.getVerifyData();
        if (null == verifyData || verifyData.isEmpty()) {
            return;
        }
        for (Member member : targetServer) {
            DistroTransportAgent agent = distroComponentHolder.findTransportAgent(type);
            if (null == agent) {
                continue;
            }
            executeTaskExecuteEngine.addTask(member.getAddress() + type,
                    new DistroVerifyExecuteTask(agent, verifyData, member.getAddress(), type));
        }
    }
}
