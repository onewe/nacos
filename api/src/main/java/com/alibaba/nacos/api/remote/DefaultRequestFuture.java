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

package com.alibaba.nacos.api.remote;

import com.alibaba.nacos.api.remote.response.Response;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * default request future.
 *
 * @author liuzunfei
 * @version $Id: DefaultRequestFuture.java, v 0.1 2020年09月01日 6:42 PM liuzunfei Exp $
 */
public class DefaultRequestFuture implements RequestFuture {
    
    private long timeStamp;
    
    private volatile boolean isDone = false;
    
    private boolean isSuccess;
    
    private RequestCallBack requestCallBack;
    
    private Exception exception;
    
    private String requestId;
    
    private String connectionId;
    
    private Response response;
    
    private ScheduledFuture timeoutFuture;
    
    TimeoutInnerTrigger timeoutInnerTrigger;
    
    /**
     * Getter method for property <tt>requestCallBack</tt>.
     *
     * @return property value of requestCallBack
     */
    public RequestCallBack getRequestCallBack() {
        return requestCallBack;
    }
    
    /**
     * Getter method for property <tt>timeStamp</tt>.
     *
     * @return property value of timeStamp
     */
    public long getTimeStamp() {
        return timeStamp;
    }
    
    public DefaultRequestFuture() {
    }
    
    public DefaultRequestFuture(String connectionId, String requestId) {
        this(connectionId, requestId, null, null);
    }
    
    public DefaultRequestFuture(String connectionId, String requestId, RequestCallBack requestCallBack,
            TimeoutInnerTrigger timeoutInnerTrigger) {
        this.timeStamp = System.currentTimeMillis();
        this.requestCallBack = requestCallBack;
        this.requestId = requestId;
        this.connectionId = connectionId;
        if (requestCallBack != null) {
            this.timeoutFuture = RpcScheduledExecutor.TIMEOUT_SCHEDULER
                    .schedule(new TimeoutHandler(), requestCallBack.getTimeout(), TimeUnit.MILLISECONDS);
        }
        this.timeoutInnerTrigger = timeoutInnerTrigger;
    }
    
    public void setResponse(final Response response) {
        // 设置标识为 true 表示改任务已完成
        isDone = true;
        // 设置响应值
        this.response = response;
        // 设置 success 为 true
        this.isSuccess = response.isSuccess();
        // 判断 timeoutFuture 是否不为空
        // 如果不为空则取消定时任务
        if (this.timeoutFuture != null) {
            timeoutFuture.cancel(true);
        }
        
        // 唤醒同一把锁等待的线程
        synchronized (this) {
            notifyAll();
        }
        
        // 执行回调
        callBacInvoke();
    }
    
    public void setFailResult(Exception e) {
        // 设置任务 已经完成
        isDone = true;
        // 设置任务 失败
        isSuccess = false;
        // 异常信息赋值
        this.exception = e;
        // 唤醒等待的线程
        synchronized (this) {
            notifyAll();
        }
        
        // 执行回调
        callBacInvoke();
    }
    
    private void callBacInvoke() {
        // 回调不为空
        if (requestCallBack != null) {
            // 获取回调任务的线程执行器
            if (requestCallBack.getExecutor() != null) {
                // 使用回调任务的线程执行器执行回调
                requestCallBack.getExecutor().execute(new CallBackHandler());
            } else {
                // 同步执行
                new CallBackHandler().run();
            }
        }
    }
    
    public String getRequestId() {
        return this.requestId;
    }
    
    @Override
    public boolean isDone() {
        return isDone;
    }
    
    @Override
    public Response get() throws InterruptedException {
        // 未设置超时时间,则一致等待
        synchronized (this) {
            // 防止被过早唤醒
            while (!isDone) {
                wait();
            }
        }
        return response;
    }
    
    @Override
    public Response get(long timeout) throws TimeoutException, InterruptedException {
        // 如果超时时间小于 0 则不进行超时等待
        if (timeout < 0) {
            synchronized (this) {
                while (!isDone) {
                    wait();
                }
            }
        } else if (timeout > 0) {
            // 进行超时等待
            long end = System.currentTimeMillis() + timeout;
            long waitTime = timeout;
            synchronized (this) {
                // 任务未完成 并且超时时间大于0
                // 防止线程过早被唤醒 循环
                while (!isDone && waitTime > 0) {
                    wait(waitTime);
                    // 结束时间 - 当前时间 = 等待时间
                    waitTime = end - System.currentTimeMillis();
                }
            }
        }
        
        // 如果任务已经完成
        if (isDone) {
            // 返回响应
            return response;
        } else {
            // 如果超时触发器不为空 则触发超时触发器
            if (timeoutInnerTrigger != null) {
                timeoutInnerTrigger.triggerOnTimeout();
            }
            // 抛出超时异常
            throw new TimeoutException("request timeout after " + timeout + " milliseconds, requestId=" + requestId);
        }
    }
    
    class CallBackHandler implements Runnable {
        
        @Override
        public void run() {
            if (exception != null) {
                requestCallBack.onException(exception);
            } else {
                requestCallBack.onResponse(response);
            }
        }
    }
    
    class TimeoutHandler implements Runnable {
        
        public TimeoutHandler() {
        }
        
        @Override
        public void run() {
            setFailResult(new TimeoutException(
                    "Timeout After " + requestCallBack.getTimeout() + " milliseconds,requestId =" + requestId));
            if (timeoutInnerTrigger != null) {
                timeoutInnerTrigger.triggerOnTimeout();
            }
        }
    }
    
    public interface TimeoutInnerTrigger {
        
        /**
         * triggered on timeout .
         */
        public void triggerOnTimeout();
        
    }
    
    /**
     * Getter method for property <tt>connectionId</tt>.
     *
     * @return property value of connectionId
     */
    public String getConnectionId() {
        return connectionId;
    }
    
    /**
     * Setter method for property <tt>timeoutFuture</tt>.
     *
     * @param timeoutFuture value to be assigned to property timeoutFuture
     */
    public void setTimeoutFuture(ScheduledFuture timeoutFuture) {
        this.timeoutFuture = timeoutFuture;
    }
    
}
