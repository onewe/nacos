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

package com.alibaba.nacos.common.task.engine;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.executor.ExecutorFactory;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.task.AbstractDelayTask;
import com.alibaba.nacos.common.task.NacosTaskProcessor;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Nacos delay task execute engine.
 *
 * @author xiweng.yy
 */
public class NacosDelayTaskExecuteEngine extends AbstractNacosTaskExecuteEngine<AbstractDelayTask> {
    
    private final ScheduledExecutorService processingExecutor;
    
    protected final ConcurrentHashMap<Object, AbstractDelayTask> tasks;
    
    protected final ReentrantLock lock = new ReentrantLock();
    
    public NacosDelayTaskExecuteEngine(String name) {
        this(name, null);
    }
    
    public NacosDelayTaskExecuteEngine(String name, Logger logger) {
        this(name, 32, logger, 100L);
    }
    
    public NacosDelayTaskExecuteEngine(String name, Logger logger, long processInterval) {
        this(name, 32, logger, processInterval);
    }
    
    public NacosDelayTaskExecuteEngine(String name, int initCapacity, Logger logger) {
        this(name, initCapacity, logger, 100L);
    }
    
    public NacosDelayTaskExecuteEngine(String name, int initCapacity, Logger logger, long processInterval) {
        super(logger);
        tasks = new ConcurrentHashMap<>(initCapacity);
        processingExecutor = ExecutorFactory.newSingleScheduledExecutorService(new NameThreadFactory(name));
        processingExecutor
                .scheduleWithFixedDelay(new ProcessRunnable(), processInterval, processInterval, TimeUnit.MILLISECONDS);
    }
    
    @Override
    public int size() {
        lock.lock();
        try {
            return tasks.size();
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public boolean isEmpty() {
        lock.lock();
        try {
            return tasks.isEmpty();
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public AbstractDelayTask removeTask(Object key) {
        lock.lock();
        try {
            AbstractDelayTask task = tasks.get(key);
            if (null != task && task.shouldProcess()) {
                return tasks.remove(key);
            } else {
                return null;
            }
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public Collection<Object> getAllTaskKeys() {
        Collection<Object> keys = new HashSet<>();
        lock.lock();
        try {
            keys.addAll(tasks.keySet());
        } finally {
            lock.unlock();
        }
        return keys;
    }
    
    @Override
    public void shutdown() throws NacosException {
        tasks.clear();
        processingExecutor.shutdown();
    }
    
    @Override
    public void addTask(Object key, AbstractDelayTask newTask) {
        lock.lock();
        try {
            // 判断任务是否存在
            AbstractDelayTask existTask = tasks.get(key);
            // 如果任务已存在则进行合并
            if (null != existTask) {
                newTask.merge(existTask);
            }
            // 假如任务队列
            tasks.put(key, newTask);
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * process tasks in execute engine.
     */
    protected void processTasks() {
        // 获取所有任务 key
        Collection<Object> keys = getAllTaskKeys();
        // 遍历所有 key
        for (Object taskKey : keys) {
            // 从任务列表中获取指定的任务 并从任务列表中移除
            AbstractDelayTask task = removeTask(taskKey);
            // 如果任务列表中没有任务则继续
            if (null == task) {
                continue;
            }
            // 获取该任务的处理器
            // 处理器需要提前注册
            NacosTaskProcessor processor = getProcessor(taskKey);
            if (null == processor) {
                getEngineLog().error("processor not found for task, so discarded. " + task);
                continue;
            }
            try {
                // ReAdd task if process failed
                // 判断是否处理失败, 处理失败则重试
                if (!processor.process(task)) {
                    retryFailedTask(taskKey, task);
                }
            } catch (Throwable e) {
                getEngineLog().error("Nacos task execute error ", e);
                retryFailedTask(taskKey, task);
            }
        }
    }
    
    private void retryFailedTask(Object key, AbstractDelayTask task) {
        task.setLastProcessTime(System.currentTimeMillis());
        addTask(key, task);
    }
    
    private class ProcessRunnable implements Runnable {
        
        @Override
        public void run() {
            try {
                processTasks();
            } catch (Throwable e) {
                getEngineLog().error(e.toString(), e);
            }
        }
    }
}
