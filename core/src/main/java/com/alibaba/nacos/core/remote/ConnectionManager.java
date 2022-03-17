/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.core.remote;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.remote.RemoteConstants;
import com.alibaba.nacos.api.remote.RequestCallBack;
import com.alibaba.nacos.api.remote.RpcScheduledExecutor;
import com.alibaba.nacos.api.remote.request.ClientDetectionRequest;
import com.alibaba.nacos.api.remote.request.ConnectResetRequest;
import com.alibaba.nacos.api.remote.request.RequestMeta;
import com.alibaba.nacos.api.remote.response.Response;
import com.alibaba.nacos.api.utils.NetUtils;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.remote.exception.ConnectionAlreadyClosedException;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.VersionUtils;
import com.alibaba.nacos.core.monitor.MetricsMonitor;
import com.alibaba.nacos.core.remote.event.ConnectionLimitRuleChangeEvent;
import com.alibaba.nacos.core.utils.Loggers;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.file.FileChangeEvent;
import com.alibaba.nacos.sys.file.FileWatcher;
import com.alibaba.nacos.sys.file.WatchFileCenter;
import com.alibaba.nacos.sys.utils.DiskUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * connect manager.
 *
 * @author liuzunfei
 * @version $Id: ConnectionManager.java, v 0.1 2020年07月13日 7:07 PM liuzunfei Exp $
 */
@Service
public class ConnectionManager extends Subscriber<ConnectionLimitRuleChangeEvent> {
    
    public static final String RULE_FILE_NAME = "limitRule";
    
    /**
     * 4 times of client keep alive.
     */
    private static final long KEEP_ALIVE_TIME = 20000L;
    
    /**
     * connection limit rule.
     * TODO 这里也能作为一个贡献点,因为这在多线程环境下可能会出问题
     */
    private ConnectionLimitRule connectionLimitRule = new ConnectionLimitRule();
    
    /**
     * current loader adjust count,only effective once,use to re balance.
     */
    private int loadClient = -1;
    
    String redirectAddress = null;
    
    private Map<String, AtomicInteger> connectionForClientIp = new ConcurrentHashMap<>(16);
    
    Map<String, Connection> connections = new ConcurrentHashMap<>();
    
    @Autowired
    private ClientConnectionEventListenerRegistry clientConnectionEventListenerRegistry;
    
    public ConnectionManager() {
        // 把事件注册到专属的发布器中
        NotifyCenter.registerToPublisher(ConnectionLimitRuleChangeEvent.class, NotifyCenter.ringBufferSize);
        // 注册事件到共享发布器中 事件是 ConnectionLimitRuleChangeEvent
        NotifyCenter.registerSubscriber(this);
    }
    
    /**
     * if monitor detail.
     *
     * @param clientIp clientIp.
     * @return
     */
    public boolean traced(String clientIp) {
        // 判断 connectionLimitRule 是否有 MonitorIpList 集合
        // 并且集合包含指定的 clientIp
        return connectionLimitRule != null && connectionLimitRule.getMonitorIpList() != null && connectionLimitRule
                .getMonitorIpList().contains(clientIp);
    }
    
    @PostConstruct
    protected void initLimitRue() {
        try {
            
            // 从本地加载 data/loader/limitRule 文件
            // 创建 ConnectionLimitRule 对象
            loadRuleFromLocal();
            // 注册文件监听器 监听 limitRule 文件
            // 发生变化刷新 ConnectionLimitRule 对象
            registerFileWatch();
        } catch (Exception e) {
            Loggers.REMOTE.warn("Fail to init limit rue from local ,error= ", e);
        }
    }
    
    /**
     * check connection id is valid.
     *
     * @param connectionId connectionId to be check.
     * @return is valid or not.
     */
    public boolean checkValid(String connectionId) {
        // 判断连接 id 是否有效
        return connections.containsKey(connectionId);
    }
    
    /**
     * register a new connect.
     *
     * @param connectionId connectionId
     * @param connection   connection
     */
    public synchronized boolean register(String connectionId, Connection connection) {
        
        // 判断连接是否已经连接
        if (connection.isConnected()) {
            // 如果连接集合中包含指定的 id
            // 则返回 true
            if (connections.containsKey(connectionId)) {
                return true;
            }
            // 判断连接是否超过规则中的限制
            if (!checkLimit(connection)) {
                // 注册失败
                return false;
            }
            // 判断是否要跟踪指定连接
            if (traced(connection.getMetaInfo().clientIp)) {
                connection.setTraced(true);
            }
            // 放入连接集合
            connections.put(connectionId, connection);
            // 客户端 ip 对应的计数器 +1
            connectionForClientIp.get(connection.getMetaInfo().clientIp).getAndIncrement();
            
            //发送事件,通知 subscriber
            // config 模块就通知 ConfigConnectionEventListener
            // ConfigConnectionEventListener 不响应 clientConnected
            // 只响应 clientDisConnected
            // 通知回调
            clientConnectionEventListenerRegistry.notifyClientConnected(connection);
            Loggers.REMOTE_DIGEST
                    .info("new connection registered successfully, connectionId = {},connection={} ", connectionId,
                            connection);
            return true;
            
        }
        return false;
        
    }
    
    /**
     * 判断指定连接的 ip 计数器有没有超过限制规则中的 ip 规则限制.
     * 或者 应用名限制.
     * 如果指定的规则中没有 ip 规则限制或者 应用名称限制 则使用默认规则进行检测.
     * @param connection 连接
     * @return true it's not over limit but false it's over limit
     */
    private boolean checkLimit(Connection connection) {
        // 获取客户端连接 ip
        String clientIp = connection.getMetaInfo().clientIp;
        
        // 判断来源是否是来自 cluster
        if (connection.getMetaInfo().isClusterSource()) {
            // 如果不包含此 IP 则放入集合中
            if (!connectionForClientIp.containsKey(clientIp)) {
                connectionForClientIp.putIfAbsent(clientIp, new AtomicInteger(0));
            }
            return true;
        }
        // 判断是否超过限制
        if (isOverLimit()) {
            return false;
        }
        
        // 放入计数器集合
        if (!connectionForClientIp.containsKey(clientIp)) {
            connectionForClientIp.putIfAbsent(clientIp, new AtomicInteger(0));
        }
        
        AtomicInteger currentCount = connectionForClientIp.get(clientIp);
        
        if (connectionLimitRule != null) {
            // 1.check rule of specific client ip limit.
            // 判断计数器的值是否超过规则中指定 ip 的值
            if (connectionLimitRule.getCountLimitPerClientIp().containsKey(clientIp)) {
                Integer integer = connectionLimitRule.getCountLimitPerClientIp().get(clientIp);
                if (integer != null && integer >= 0) {
                    return currentCount.get() < integer;
                }
            }
            // 2.check rule of specific client app limit.
            // 获取连接名称
            String appName = connection.getMetaInfo().getAppName();
            // 判断应用名称是否在限制客户端名单中
            if (StringUtils.isNotBlank(appName) && connectionLimitRule.getCountLimitPerClientApp()
                    .containsKey(appName)) {
                // 判断计数器的值是否超过规则中指定应用名称的值
                Integer integerApp = connectionLimitRule.getCountLimitPerClientApp().get(appName);
                if (integerApp != null && integerApp >= 0) {
                    return currentCount.get() < integerApp;
                }
            }
            
            // 3.check rule of default client ip.
            // 使用默认规则
            int countLimitPerClientIpDefault = connectionLimitRule.getCountLimitPerClientIpDefault();
            return countLimitPerClientIpDefault <= 0 || currentCount.get() < countLimitPerClientIpDefault;
        }
        
        return true;
        
    }
    
    /**
     * unregister a connection .
     *
     * @param connectionId connectionId.
     */
    public synchronized void unregister(String connectionId) {
        // 移除指定的连接
        Connection remove = this.connections.remove(connectionId);
        if (remove != null) {
            // 获取指定的 连接ip
            String clientIp = remove.getMetaInfo().clientIp;
            // 获取指定 ip 的计数器
            AtomicInteger atomicInteger = connectionForClientIp.get(clientIp);
            if (atomicInteger != null) {
                // -1
                int count = atomicInteger.decrementAndGet();
                // 负数,说明对应 IP 机器上没有客户端连接了
                if (count <= 0) {
                    // 移除计数器
                    connectionForClientIp.remove(clientIp);
                }
            }
            // 关闭连接
            remove.close();
            Loggers.REMOTE_DIGEST.info("[{}]Connection unregistered successfully. ", connectionId);
            // 通知对应的监听器 config 模块会通知 ConfigConnectionEventListener
            clientConnectionEventListenerRegistry.notifyClientDisConnected(remove);
        }
    }
    
    /**
     * get by connection id.
     *
     * @param connectionId connection id.
     * @return connection of the id.
     */
    public Connection getConnection(String connectionId) {
        return connections.get(connectionId);
    }
    
    /**
     * get by client ip.
     *
     * @param clientIp client ip.
     * @return connections of the client ip.
     */
    public List<Connection> getConnectionByIp(String clientIp) {
        Set<Map.Entry<String, Connection>> entries = connections.entrySet();
        List<Connection> connections = new ArrayList<>();
        for (Map.Entry<String, Connection> entry : entries) {
            Connection value = entry.getValue();
            if (clientIp.equals(value.getMetaInfo().clientIp)) {
                connections.add(value);
            }
        }
        return connections;
    }
    
    /**
     * get current connections count.
     *
     * @return get all connection count
     */
    public int getCurrentConnectionCount() {
        return this.connections.size();
    }
    
    /**
     * regresh connection active time.
     *
     * @param connectionId connectionId.
     */
    public void refreshActiveTime(String connectionId) {
        Connection connection = connections.get(connectionId);
        if (connection != null) {
            connection.freshActiveTime();
        }
    }
    
    /**
     * Start Task：Expel the connection which active Time expire.
     */
    @PostConstruct
    public void start() {
        
        // Start UnHealthy Connection Expel Task.
        RpcScheduledExecutor.COMMON_SERVER_EXECUTOR.scheduleWithFixedDelay(() -> {
            try {

                int totalCount = connections.size();
                Loggers.REMOTE_DIGEST.info("Connection check task start");
                MetricsMonitor.getLongConnectionMonitor().set(totalCount);
                Set<Map.Entry<String, Connection>> entries = connections.entrySet();
                int currentSdkClientCount = currentSdkClientCount();
                boolean isLoaderClient = loadClient >= 0;
                int currentMaxClient = isLoaderClient ? loadClient : connectionLimitRule.countLimit;
                int expelCount = currentMaxClient < 0 ? 0 : Math.max(currentSdkClientCount - currentMaxClient, 0);

                Loggers.REMOTE_DIGEST
                        .info("Total count ={}, sdkCount={},clusterCount={}, currentLimit={}, toExpelCount={}",
                                totalCount, currentSdkClientCount, (totalCount - currentSdkClientCount),
                                currentMaxClient + (isLoaderClient ? "(loaderCount)" : ""), expelCount);

                List<String> expelClient = new LinkedList<>();

                Map<String, AtomicInteger> expelForIp = new HashMap<>(16);

                //1. calculate expel count  of ip.
                for (Map.Entry<String, Connection> entry : entries) {

                    Connection client = entry.getValue();
                    String appName = client.getMetaInfo().getAppName();
                    String clientIp = client.getMetaInfo().getClientIp();
                    if (client.getMetaInfo().isSdkSource() && !expelForIp.containsKey(clientIp)) {
                        //get limit for current ip.
                        int countLimitOfIp = connectionLimitRule.getCountLimitOfIp(clientIp);
                        if (countLimitOfIp < 0) {
                            int countLimitOfApp = connectionLimitRule.getCountLimitOfApp(appName);
                            countLimitOfIp = countLimitOfApp < 0 ? countLimitOfIp : countLimitOfApp;
                        }
                        if (countLimitOfIp < 0) {
                            countLimitOfIp = connectionLimitRule.getCountLimitPerClientIpDefault();
                        }

                        if (countLimitOfIp >= 0 && connectionForClientIp.containsKey(clientIp)) {
                            AtomicInteger currentCountIp = connectionForClientIp.get(clientIp);
                            if (currentCountIp != null && currentCountIp.get() > countLimitOfIp) {
                                expelForIp.put(clientIp, new AtomicInteger(currentCountIp.get() - countLimitOfIp));
                            }
                        }
                    }
                }

                Loggers.REMOTE_DIGEST
                        .info("Check over limit for ip limit rule, over limit ip count={}", expelForIp.size());

                if (expelForIp.size() > 0) {
                    Loggers.REMOTE_DIGEST.info("Over limit ip expel info, {}", expelForIp);
                }

                Set<String> outDatedConnections = new HashSet<>();
                long now = System.currentTimeMillis();
                //2.get expel connection for ip limit.
                for (Map.Entry<String, Connection> entry : entries) {
                    Connection client = entry.getValue();
                    String clientIp = client.getMetaInfo().getClientIp();
                    AtomicInteger integer = expelForIp.get(clientIp);
                    if (integer != null && integer.intValue() > 0) {
                        integer.decrementAndGet();
                        expelClient.add(client.getMetaInfo().getConnectionId());
                        expelCount--;
                    } else if (now - client.getMetaInfo().getLastActiveTime() >= KEEP_ALIVE_TIME) {
                        outDatedConnections.add(client.getMetaInfo().getConnectionId());
                    }

                }

                //3. if total count is still over limit.
                if (expelCount > 0) {
                    for (Map.Entry<String, Connection> entry : entries) {
                        Connection client = entry.getValue();
                        if (!expelForIp.containsKey(client.getMetaInfo().clientIp) && client.getMetaInfo()
                                .isSdkSource() && expelCount > 0) {
                            expelClient.add(client.getMetaInfo().getConnectionId());
                            expelCount--;
                            outDatedConnections.remove(client.getMetaInfo().getConnectionId());
                        }
                    }
                }

                String serverIp = null;
                String serverPort = null;
                if (StringUtils.isNotBlank(redirectAddress) && redirectAddress.contains(Constants.COLON)) {
                    String[] split = redirectAddress.split(Constants.COLON);
                    serverIp = split[0];
                    serverPort = split[1];
                }

                for (String expelledClientId : expelClient) {
                    try {
                        Connection connection = getConnection(expelledClientId);
                        if (connection != null) {
                            ConnectResetRequest connectResetRequest = new ConnectResetRequest();
                            connectResetRequest.setServerIp(serverIp);
                            connectResetRequest.setServerPort(serverPort);
                            connection.asyncRequest(connectResetRequest, null);
                            Loggers.REMOTE_DIGEST
                                    .info("Send connection reset request , connection id = {},recommendServerIp={}, recommendServerPort={}",
                                            expelledClientId, connectResetRequest.getServerIp(),
                                            connectResetRequest.getServerPort());
                        }

                    } catch (ConnectionAlreadyClosedException e) {
                        unregister(expelledClientId);
                    } catch (Exception e) {
                        Loggers.REMOTE_DIGEST.error("Error occurs when expel connection, expelledClientId:{}", expelledClientId, e);
                    }
                }

                //4.client active detection.
                Loggers.REMOTE_DIGEST.info("Out dated connection ,size={}", outDatedConnections.size());
                if (CollectionUtils.isNotEmpty(outDatedConnections)) {
                    Set<String> successConnections = new HashSet<>();
                    final CountDownLatch latch = new CountDownLatch(outDatedConnections.size());
                    for (String outDateConnectionId : outDatedConnections) {
                        try {
                            Connection connection = getConnection(outDateConnectionId);
                            if (connection != null) {
                                ClientDetectionRequest clientDetectionRequest = new ClientDetectionRequest();
                                connection.asyncRequest(clientDetectionRequest, new RequestCallBack() {
                                    @Override
                                    public Executor getExecutor() {
                                        return null;
                                    }

                                    @Override
                                    public long getTimeout() {
                                        return 1000L;
                                    }

                                    @Override
                                    public void onResponse(Response response) {
                                        latch.countDown();
                                        if (response != null && response.isSuccess()) {
                                            connection.freshActiveTime();
                                            successConnections.add(outDateConnectionId);
                                        }
                                    }

                                    @Override
                                    public void onException(Throwable e) {
                                        latch.countDown();
                                    }
                                });

                                Loggers.REMOTE_DIGEST
                                        .info("[{}]send connection active request ", outDateConnectionId);
                            } else {
                                latch.countDown();
                            }

                        } catch (ConnectionAlreadyClosedException e) {
                            latch.countDown();
                        } catch (Exception e) {
                            Loggers.REMOTE_DIGEST
                                    .error("[{}]Error occurs when check client active detection ,error={}",
                                            outDateConnectionId, e);
                            latch.countDown();
                        }
                    }

                    latch.await(3000L, TimeUnit.MILLISECONDS);
                    Loggers.REMOTE_DIGEST
                            .info("Out dated connection check successCount={}", successConnections.size());

                    for (String outDateConnectionId : outDatedConnections) {
                        if (!successConnections.contains(outDateConnectionId)) {
                            Loggers.REMOTE_DIGEST
                                    .info("[{}]Unregister Out dated connection....", outDateConnectionId);
                            unregister(outDateConnectionId);
                        }
                    }
                }

                //reset loader client

                if (isLoaderClient) {
                    loadClient = -1;
                    redirectAddress = null;
                }

                Loggers.REMOTE_DIGEST.info("Connection check task end");

            } catch (Throwable e) {
                Loggers.REMOTE.error("Error occurs during connection check... ", e);
            }
        }, 1000L, 3000L, TimeUnit.MILLISECONDS);
        
    }
    
    private RequestMeta buildMeta() {
        RequestMeta meta = new RequestMeta();
        meta.setClientVersion(VersionUtils.getFullClientVersion());
        meta.setClientIp(NetUtils.localIP());
        return meta;
    }
    
    public void loadCount(int loadClient, String redirectAddress) {
        this.loadClient = loadClient;
        this.redirectAddress = redirectAddress;
    }
    
    /**
     * 让指定的连接进行重连,连到指定的服务端
     * send load request to spefic connetionId.
     *
     * @param connectionId    connection id of client.
     * @param redirectAddress server address to redirect.
     */
    public void loadSingle(String connectionId, String redirectAddress) {
        Connection connection = getConnection(connectionId);
        
        if (connection != null) {
            // 判断连接是否是从 sdk 端过来了的
            if (connection.getMetaInfo().isSdkSource()) {
                ConnectResetRequest connectResetRequest = new ConnectResetRequest();
                if (StringUtils.isNotBlank(redirectAddress) && redirectAddress.contains(Constants.COLON)) {
                    String[] split = redirectAddress.split(Constants.COLON);
                    connectResetRequest.setServerIp(split[0]);
                    connectResetRequest.setServerPort(split[1]);
                }
                try {
                    // 发送 ConnectResetRequest 请求
                    // 让客户端连接连接到其他服务器去
                    connection.request(connectResetRequest, 3000L);
                } catch (ConnectionAlreadyClosedException e) {
                    // 发送失败取消注册
                    unregister(connectionId);
                } catch (Exception e) {
                    Loggers.REMOTE.error("error occurs when expel connection, connectionId: {} ", connectionId, e);
                }
            }
        }
        
    }
    
    /**
     * get all client count.
     *
     * @return client count.
     */
    public int currentClientsCount() {
        return connections.size();
    }
    
    /**
     * get client count with labels filter.
     *
     * @param filterLabels label to filter client count.
     * @return count with the specific filter labels.
     */
    public int currentClientsCount(Map<String, String> filterLabels) {
        int count = 0;
        // 遍历所有连接
        for (Connection connection : connections.values()) {
            // 获取每个连接的 metaInfo 中的 标签
            Map<String, String> labels = connection.getMetaInfo().labels;
            boolean disMatchFound = false;
            // filterLabels 遍历 map 中的 所有 kv
            for (Map.Entry<String, String> entry : filterLabels.entrySet()) {
                // 判断指定的 label 是否存在对应的连接
                if (!entry.getValue().equals(labels.get(entry.getKey()))) {
                    disMatchFound = true;
                    break;
                }
            }
            // 如果匹配计数器 + 1
            if (!disMatchFound) {
                count++;
            }
        }
        return count;
    }
    
    /**
     * get client count from sdk.
     *
     * @return sdk client count.
     */
    public int currentSdkClientCount() {
        Map<String, String> filter = new HashMap<>(2);
        filter.put(RemoteConstants.LABEL_SOURCE, RemoteConstants.LABEL_SOURCE_SDK);
        return currentClientsCount(filter);
    }
    
    public Map<String, Connection> currentClients() {
        return connections;
    }
    
    /**
     * check if over limit.
     *
     * @return over limit or not.
     */
    private boolean isOverLimit() {
        // 判断客户端带有 sdk 标签的连接是否超过 指定的连接数
        return connectionLimitRule.countLimit > 0 && currentSdkClientCount() >= connectionLimitRule.getCountLimit();
    }
    
    /**
     * 用于响应 ConnectionLimitRuleChangeEvent 事件.
     * @param event {@link Event}
     */
    @Override
    public void onEvent(ConnectionLimitRuleChangeEvent event) {
        String limitRule = event.getLimitRule();
        Loggers.REMOTE.info("connection limit rule change event receive :{}", limitRule);
        
        try {
            ConnectionLimitRule connectionLimitRule = JacksonUtils.toObj(limitRule, ConnectionLimitRule.class);
            if (connectionLimitRule != null) {
                this.connectionLimitRule = connectionLimitRule;
                
                try {
                    saveRuleToLocal(this.connectionLimitRule);
                } catch (Exception e) {
                    Loggers.REMOTE.warn("Fail to save rule to local error is ", e);
                }
            } else {
                Loggers.REMOTE.info("Parse rule is null,Ignore illegal rule  :{}", limitRule);
            }
            
        } catch (Exception e) {
            Loggers.REMOTE.error("Fail to parse connection limit rule :{}", limitRule, e);
        }
    }
    
    @Override
    public Class<? extends Event> subscribeType() {
        return ConnectionLimitRuleChangeEvent.class;
    }
    
    static class ConnectionLimitRule {
        
        private Set<String> monitorIpList = new HashSet<>();
        
        private int countLimit = -1;
        
        private int countLimitPerClientIpDefault = -1;
        
        private Map<String, Integer> countLimitPerClientIp = new HashMap<>();
        
        private Map<String, Integer> countLimitPerClientApp = new HashMap<>();
        
        public int getCountLimit() {
            return countLimit;
        }
        
        public void setCountLimit(int countLimit) {
            this.countLimit = countLimit;
        }
        
        public int getCountLimitPerClientIpDefault() {
            return countLimitPerClientIpDefault;
        }
        
        public void setCountLimitPerClientIpDefault(int countLimitPerClientIpDefault) {
            this.countLimitPerClientIpDefault = countLimitPerClientIpDefault;
        }
        
        public int getCountLimitOfIp(String clientIp) {
            if (countLimitPerClientIp.containsKey(clientIp)) {
                Integer integer = countLimitPerClientIp.get(clientIp);
                if (integer != null && integer >= 0) {
                    return integer;
                }
            }
            return -1;
        }
        
        public int getCountLimitOfApp(String appName) {
            if (countLimitPerClientApp.containsKey(appName)) {
                Integer integer = countLimitPerClientApp.get(appName);
                if (integer != null && integer >= 0) {
                    return integer;
                }
            }
            return -1;
        }
        
        public Map<String, Integer> getCountLimitPerClientIp() {
            return countLimitPerClientIp;
        }
        
        public void setCountLimitPerClientIp(Map<String, Integer> countLimitPerClientIp) {
            this.countLimitPerClientIp = countLimitPerClientIp;
        }
        
        public Map<String, Integer> getCountLimitPerClientApp() {
            return countLimitPerClientApp;
        }
        
        public void setCountLimitPerClientApp(Map<String, Integer> countLimitPerClientApp) {
            this.countLimitPerClientApp = countLimitPerClientApp;
        }
        
        public Set<String> getMonitorIpList() {
            return monitorIpList;
        }
        
        public void setMonitorIpList(Set<String> monitorIpList) {
            this.monitorIpList = monitorIpList;
        }
    }
    
    public ConnectionLimitRule getConnectionLimitRule() {
        return connectionLimitRule;
    }
    
    private synchronized void loadRuleFromLocal() throws Exception {
        // 获取文件 data/loader/limitRule
        File limitFile = getRuleFile();
        // 如果文件不存在则创建
        if (!limitFile.exists()) {
            limitFile.createNewFile();
        }
        // 读取文件
        String ruleContent = DiskUtils.readFile(limitFile);
        // 判断文件内容是否为空
        // 如果为空则创建新的 ConnectionLimitRule 对象
        // 否则使用 Jackson 反序列为 ConnectionLimitRule 对象
        ConnectionLimitRule connectionLimitRule = StringUtils.isBlank(ruleContent) ? new ConnectionLimitRule()
                : JacksonUtils.toObj(ruleContent, ConnectionLimitRule.class);
        // apply rule.
        // 应用规则
        if (connectionLimitRule != null) {
            this.connectionLimitRule = connectionLimitRule;
            // 获取规则中的 ip 地址
            Set<String> monitorIpList = connectionLimitRule.monitorIpList;
            for (Connection connection : this.connections.values()) {
                String clientIp = connection.getMetaInfo().getClientIp();
                // 如果客户端连接中的 ip 地址在规则集合内
                // 则设置对映 ip 的连接 traced 为 true
                if (!CollectionUtils.isEmpty(monitorIpList) && monitorIpList.contains(clientIp)) {
                    connection.setTraced(true);
                } else {
                    connection.setTraced(false);
                }
            }
            
        }
        Loggers.REMOTE.info("Init loader limit rule from local,rule={}", ruleContent);
        
    }
    
    private synchronized void saveRuleToLocal(ConnectionLimitRule limitRule) throws IOException {
        
        File limitFile = getRuleFile();
        if (!limitFile.exists()) {
            limitFile.createNewFile();
        }
        DiskUtils.writeFile(limitFile, JacksonUtils.toJson(limitRule).getBytes(Constants.ENCODE), false);
    }
    
    /**
     * 获取 data/loader/limitRule 文件.
     * @return 返回文件对象
     */
    private File getRuleFile() {
        File baseDir = new File(EnvUtil.getNacosHome(), "data" + File.separator + "loader" + File.separator);
        if (!baseDir.exists()) {
            baseDir.mkdir();
        }
        return new File(baseDir, RULE_FILE_NAME);
    }
    
    private void registerFileWatch() {
        try {
            // 注册文件监控器
            // 监控到 data/loader/limitRule 文件的变动就重新加载 limitRule 文件
            String tpsPath = Paths.get(EnvUtil.getNacosHome(), "data", "loader").toString();
            WatchFileCenter.registerWatcher(tpsPath, new FileWatcher() {
                @Override
                public void onChange(FileChangeEvent event) {
                    try {
                        String fileName = event.getContext().toString();
                        if (RULE_FILE_NAME.equals(fileName)) {
                            loadRuleFromLocal();
                        }
                    } catch (Throwable throwable) {
                        Loggers.REMOTE.warn("Fail to load rule from local", throwable);
                    }
                }
                
                @Override
                public boolean interest(String context) {
                    return RULE_FILE_NAME.equals(context);
                }
            });
        } catch (NacosException e) {
            Loggers.REMOTE.warn("Register  connection rule fail ", e);
        }
    }
}
