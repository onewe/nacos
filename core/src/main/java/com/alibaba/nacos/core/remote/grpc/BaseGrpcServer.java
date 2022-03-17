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

package com.alibaba.nacos.core.remote.grpc;

import com.alibaba.nacos.api.grpc.auto.Payload;
import com.alibaba.nacos.common.remote.ConnectionType;
import com.alibaba.nacos.common.utils.ReflectUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.core.remote.BaseRpcServer;
import com.alibaba.nacos.core.remote.ConnectionManager;
import com.alibaba.nacos.core.utils.Loggers;
import io.grpc.Attributes;
import io.grpc.CompressorRegistry;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.DecompressorRegistry;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServerTransportFilter;
import io.grpc.internal.ServerStream;
import io.grpc.netty.shaded.io.netty.channel.Channel;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.util.MutableHandlerRegistry;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Grpc implementation as a rpc server.
 *
 * @author liuzunfei
 * @version $Id: BaseGrpcServer.java, v 0.1 2020年07月13日 3:42 PM liuzunfei Exp $
 */
public abstract class BaseGrpcServer extends BaseRpcServer {
    
    private Server server;
    
    private static final String REQUEST_BI_STREAM_SERVICE_NAME = "BiRequestStream";
    
    private static final String REQUEST_BI_STREAM_METHOD_NAME = "requestBiStream";
    
    private static final String REQUEST_SERVICE_NAME = "Request";
    
    private static final String REQUEST_METHOD_NAME = "request";
    
    private static final String GRPC_MAX_INBOUND_MSG_SIZE_PROPERTY = "nacos.remote.server.grpc.maxinbound.message.size";
    
    private static final long DEFAULT_GRPC_MAX_INBOUND_MSG_SIZE = 10 * 1024 * 1024;
    
    @Autowired
    private GrpcRequestAcceptor grpcCommonRequestAcceptor;
    
    @Autowired
    private GrpcBiStreamRequestAcceptor grpcBiStreamRequestAcceptor;
    
    @Autowired
    private ConnectionManager connectionManager;
    
    @Override
    public ConnectionType getConnectionType() {
        return ConnectionType.GRPC;
    }
    
    @Override
    public void startServer() throws Exception {
        // 创建一个 hashMap 的默认实现 HandlerRegistry
        final MutableHandlerRegistry handlerRegistry = new MutableHandlerRegistry();
        
        // server interceptor to set connection id.
        // 创建一个 server 拦截器 在处理请求之前进行一些预处理
        // 比如设置 设置 connectionId 到 本次请求上下文中
        ServerInterceptor serverInterceptor = new ServerInterceptor() {
            @Override
            public <T, S> ServerCall.Listener<T> interceptCall(ServerCall<T, S> call, Metadata headers,
                    ServerCallHandler<T, S> next) {
                Context ctx = Context.current()
                        // 设置 connection_id 属性到上下文
                        .withValue(CONTEXT_KEY_CONN_ID, call.getAttributes().get(TRANS_KEY_CONN_ID))
                        // 设置 remote_ip 属性到上下文
                        .withValue(CONTEXT_KEY_CONN_REMOTE_IP, call.getAttributes().get(TRANS_KEY_REMOTE_IP))
                        // 设置 remote_port 属性到上下文
                        .withValue(CONTEXT_KEY_CONN_REMOTE_PORT, call.getAttributes().get(TRANS_KEY_REMOTE_PORT))
                        // 设置 local_port 属性到上下文
                        .withValue(CONTEXT_KEY_CONN_LOCAL_PORT, call.getAttributes().get(TRANS_KEY_LOCAL_PORT));
                // 判断是否是调用的 BiRequestStream
                if (REQUEST_BI_STREAM_SERVICE_NAME.equals(call.getMethodDescriptor().getServiceName())) {
                    // 获取 call 中的内部 channel
                    Channel internalChannel = getInternalChannel(call);
                    // 把 channel 设置到上下文中
                    ctx = ctx.withValue(CONTEXT_KEY_CHANNEL, internalChannel);
                }
                return Contexts.interceptCall(ctx, call, headers, next);
            }
        };
        // 注册服务
        addServices(handlerRegistry, serverInterceptor);
        
        // 构建 server,设置 server 端口 线程池 最大进站消息内容大小 等
        server = ServerBuilder.forPort(getServicePort()).executor(getRpcExecutor())
                .maxInboundMessageSize(getInboundMessageSize()).fallbackHandlerRegistry(handlerRegistry)
                .compressorRegistry(CompressorRegistry.getDefaultInstance())
                .decompressorRegistry(DecompressorRegistry.getDefaultInstance())
                // 设置数据传输 filter 这个优先级比 interceptor 都高
                // 这个 filter 在 interceptor 前执行
                .addTransportFilter(new ServerTransportFilter() {
                    // 此方法是在请求进来时转发到处理器之前执行
                    @Override
                    public Attributes transportReady(Attributes transportAttrs) {
                        // 获取远程连接地址
                        InetSocketAddress remoteAddress = (InetSocketAddress) transportAttrs
                                .get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
                        // 获取本地连接地址
                        InetSocketAddress localAddress = (InetSocketAddress) transportAttrs
                                .get(Grpc.TRANSPORT_ATTR_LOCAL_ADDR);
                        // 获取远程端口
                        int remotePort = remoteAddress.getPort();
                        // 获取本地端口
                        int localPort = localAddress.getPort();
                        // 获取远程 ip
                        String remoteIp = remoteAddress.getAddress().getHostAddress();
                        // 构建 Attributes 对象 把上面获取到到值都给设置进去
                        // 特别重要都是要把 connection id 也给设置进去
                        Attributes attrWrapper = transportAttrs.toBuilder()
                                .set(TRANS_KEY_CONN_ID, System.currentTimeMillis() + "_" + remoteIp + "_" + remotePort)
                                .set(TRANS_KEY_REMOTE_IP, remoteIp).set(TRANS_KEY_REMOTE_PORT, remotePort)
                                .set(TRANS_KEY_LOCAL_PORT, localPort).build();
                        String connectionId = attrWrapper.get(TRANS_KEY_CONN_ID);
                        Loggers.REMOTE_DIGEST.info("Connection transportReady,connectionId = {} ", connectionId);
                        
                        // attribute 对象会放到请求到 call 中
                        return attrWrapper;
                        
                    }
                    
                    // 此方法是在请求结束时执行
                    @Override
                    public void transportTerminated(Attributes transportAttrs) {
                        String connectionId = null;
                        try {
                            // 获取 connection id 如果有到话
                            connectionId = transportAttrs.get(TRANS_KEY_CONN_ID);
                        } catch (Exception e) {
                            // Ignore
                        }
                        
                        // 取消注册
                        if (StringUtils.isNotBlank(connectionId)) {
                            Loggers.REMOTE_DIGEST
                                    .info("Connection transportTerminated,connectionId = {} ", connectionId);
                            connectionManager.unregister(connectionId);
                        }
                    }
                }).build();
        
        server.start();
    }
    
    private int getInboundMessageSize() {
        String messageSize = System
                .getProperty(GRPC_MAX_INBOUND_MSG_SIZE_PROPERTY, String.valueOf(DEFAULT_GRPC_MAX_INBOUND_MSG_SIZE));
        return Integer.parseInt(messageSize);
    }
    
    private Channel getInternalChannel(ServerCall serverCall) {
        ServerStream serverStream = (ServerStream) ReflectUtils.getFieldValue(serverCall, "stream");
        return (Channel) ReflectUtils.getFieldValue(serverStream, "channel");
    }
    
    private void addServices(MutableHandlerRegistry handlerRegistry, ServerInterceptor... serverInterceptor) {
        
        // unary common call register.
        // 创建一个常规的阻塞式调用服务
        final MethodDescriptor<Payload, Payload> unaryPayloadMethod = MethodDescriptor.<Payload, Payload>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                // 服务名称全称为 Request 方法为 调用的方法为 request
                .setFullMethodName(MethodDescriptor.generateFullMethodName(REQUEST_SERVICE_NAME, REQUEST_METHOD_NAME))
                // 设置请求序列化方式 默认为 proto
                .setRequestMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance()))
                // 设置响应序列化方式 默认为 proto
                .setResponseMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance())).build();
        
        // 创建一个常规请求处理器, 默认常规请求使用 GrpcRequestAcceptor 进行统一处理
        final ServerCallHandler<Payload, Payload> payloadHandler = ServerCalls
                .asyncUnaryCall((request, responseObserver) -> grpcCommonRequestAcceptor.request(request, responseObserver));
        
        // 构建服务定义
        final ServerServiceDefinition serviceDefOfUnaryPayload = ServerServiceDefinition.builder(REQUEST_SERVICE_NAME)
                .addMethod(unaryPayloadMethod, payloadHandler).build();
        // 服务注册
        handlerRegistry.addService(ServerInterceptors.intercept(serviceDefOfUnaryPayload, serverInterceptor));
        
        // bi stream register.
        // 创建一个双边流式服务,默认使用 GrpcBiStreamRequestAcceptor 进行处理
        final ServerCallHandler<Payload, Payload> biStreamHandler = ServerCalls.asyncBidiStreamingCall(
                (responseObserver) -> grpcBiStreamRequestAcceptor.requestBiStream(responseObserver));
        // 创建服务定义
        final MethodDescriptor<Payload, Payload> biStreamMethod = MethodDescriptor.<Payload, Payload>newBuilder()
                .setType(MethodDescriptor.MethodType.BIDI_STREAMING).setFullMethodName(MethodDescriptor
                        .generateFullMethodName(REQUEST_BI_STREAM_SERVICE_NAME, REQUEST_BI_STREAM_METHOD_NAME))
                .setRequestMarshaller(ProtoUtils.marshaller(Payload.newBuilder().build()))
                .setResponseMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance())).build();
        // 构建服务
        final ServerServiceDefinition serviceDefOfBiStream = ServerServiceDefinition
                .builder(REQUEST_BI_STREAM_SERVICE_NAME).addMethod(biStreamMethod, biStreamHandler).build();
        // 服务注册
        handlerRegistry.addService(ServerInterceptors.intercept(serviceDefOfBiStream, serverInterceptor));
        
    }
    
    @Override
    public void shutdownServer() {
        if (server != null) {
            server.shutdownNow();
        }
    }
    
    /**
     * get rpc executor.
     *
     * @return executor.
     */
    public abstract ThreadPoolExecutor getRpcExecutor();
    
    static final Attributes.Key<String> TRANS_KEY_CONN_ID = Attributes.Key.create("conn_id");
    
    static final Attributes.Key<String> TRANS_KEY_REMOTE_IP = Attributes.Key.create("remote_ip");
    
    static final Attributes.Key<Integer> TRANS_KEY_REMOTE_PORT = Attributes.Key.create("remote_port");
    
    static final Attributes.Key<Integer> TRANS_KEY_LOCAL_PORT = Attributes.Key.create("local_port");
    
    static final Context.Key<String> CONTEXT_KEY_CONN_ID = Context.key("conn_id");
    
    static final Context.Key<String> CONTEXT_KEY_CONN_REMOTE_IP = Context.key("remote_ip");
    
    static final Context.Key<Integer> CONTEXT_KEY_CONN_REMOTE_PORT = Context.key("remote_port");
    
    static final Context.Key<Integer> CONTEXT_KEY_CONN_LOCAL_PORT = Context.key("local_port");
    
    static final Context.Key<Channel> CONTEXT_KEY_CHANNEL = Context.key("ctx_channel");
    
}
