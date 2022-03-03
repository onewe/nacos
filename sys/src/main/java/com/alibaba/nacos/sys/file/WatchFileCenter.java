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

package com.alibaba.nacos.sys.file;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.executor.ExecutorFactory;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.common.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unified file change monitoring management center, which uses {@link WatchService} internally. One file directory
 * corresponds to one {@link WatchService}. It can only monitor up to 32 file directories. When a file change occurs, a
 * {@link FileChangeEvent} will be issued
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
public class WatchFileCenter {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(WatchFileCenter.class);
    
    /**
     * Maximum number of monitored file directories.
     * 最大任务数量
     */
    private static final int MAX_WATCH_FILE_JOB = Integer.getInteger("nacos.watch-file.max-dirs", 16);
    
    private static final Map<String, WatchDirJob> MANAGER = new HashMap<String, WatchDirJob>(MAX_WATCH_FILE_JOB);
    
    private static final FileSystem FILE_SYSTEM = FileSystems.getDefault();
    
    private static final AtomicBoolean CLOSED = new AtomicBoolean(false);
    
    static {
        ThreadUtils.addShutdownHook(WatchFileCenter::shutdown);
    }
    
    /**
     * The number of directories that are currently monitored.
     * 当前监听任务数量
     */
    @SuppressWarnings("checkstyle:StaticVariableName")
    private static int NOW_WATCH_JOB_CNT = 0;
    
    /**
     * Register {@link FileWatcher} in this directory.
     *
     * @param paths   directory
     * @param watcher {@link FileWatcher}
     * @return register is success
     * @throws NacosException NacosException
     */
    public static synchronized boolean registerWatcher(final String paths, FileWatcher watcher) throws NacosException {
        checkState();
        // 判断当前的JOB数量是否等于最大job数量
        if (NOW_WATCH_JOB_CNT == MAX_WATCH_FILE_JOB) {
            return false;
        }
        // 获取指定的监听job
        WatchDirJob job = MANAGER.get(paths);
        // 如果为能获取到则新建一个job
        if (job == null) {
            job = new WatchDirJob(paths);
            job.start();
            // 放入 map 中
            MANAGER.put(paths, job);
            // 当前 job 数量+1
            NOW_WATCH_JOB_CNT++;
        }
        // 增加订阅
        job.addSubscribe(watcher);
        return true;
    }
    
    /**
     * Deregister all {@link FileWatcher} in this directory.
     *
     * @param path directory
     * @return deregister is success
     */
    public static synchronized boolean deregisterAllWatcher(final String path) {
        // 从 map 中获取 job
        WatchDirJob job = MANAGER.get(path);
        // 判断 map 中是否有指定的job
        if (job != null) {
            // 关闭 job
            job.shutdown();
            // 移除 job 映射
            MANAGER.remove(path);
            // 当前 job 数量-1
            NOW_WATCH_JOB_CNT--;
            return true;
        }
        return false;
    }
    
    /**
     * close {@link WatchFileCenter}.
     */
    public static void shutdown() {
        if (!CLOSED.compareAndSet(false, true)) {
            return;
        }
        LOGGER.warn("[WatchFileCenter] start close");
        // 判断 map 集合中的所有 job
        for (Map.Entry<String, WatchDirJob> entry : MANAGER.entrySet()) {
            LOGGER.warn("[WatchFileCenter] start to shutdown this watcher which is watch : " + entry.getKey());
            try {
                // 依次关闭 job
                entry.getValue().shutdown();
            } catch (Throwable e) {
                LOGGER.error("[WatchFileCenter] shutdown has error : ", e);
            }
        }
        // 清空 map 集合
        MANAGER.clear();
        // 设置当前数量为 0
        NOW_WATCH_JOB_CNT = 0;
        LOGGER.warn("[WatchFileCenter] already closed");
    }
    
    /**
     * Deregister {@link FileWatcher} in this directory.
     *
     * @param path    directory
     * @param watcher {@link FileWatcher}
     * @return deregister is success
     */
    public static synchronized boolean deregisterWatcher(final String path, final FileWatcher watcher) {
        // 获取映射关系
        WatchDirJob job = MANAGER.get(path);
        // 判断 job 是否不为空
        if (job != null) {
            // job 中移除指定的 watcher
            job.watchers.remove(watcher);
            return true;
        }
        return false;
    }
    
    private static class WatchDirJob extends Thread {
        
        private ExecutorService callBackExecutor;
        
        private final String paths;
        
        private WatchService watchService;
        
        private volatile boolean watch = true;
        
        private Set<FileWatcher> watchers = new ConcurrentHashSet<>();
        
        public WatchDirJob(String paths) throws NacosException {
            setName(paths);
            this.paths = paths;
            final Path p = Paths.get(paths);
            // 只监听目录 如果路径不是目录则跑出移除
            if (!p.toFile().isDirectory()) {
                throw new IllegalArgumentException("Must be a file directory : " + paths);
            }
            // 创建一个用于回掉的线程池
            this.callBackExecutor = ExecutorFactory
                    .newSingleExecutorService(new NameThreadFactory("com.alibaba.nacos.sys.file.watch-" + paths));
            
            try {
                // 获取文件系统的文件监听服务
                WatchService service = FILE_SYSTEM.newWatchService();
                // 注册监听 监听事件为 OVERFLOW ENTRY_MODIFY ENTRY_CREATE ENTRY_DELETE
                p.register(service, StandardWatchEventKinds.OVERFLOW, StandardWatchEventKinds.ENTRY_MODIFY,
                        StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE);
                // 设置监听服务
                this.watchService = service;
            } catch (Throwable ex) {
                throw new NacosException(NacosException.SERVER_ERROR, ex);
            }
        }
        
        void addSubscribe(final FileWatcher watcher) {
            // 添加到 watchers 集合
            watchers.add(watcher);
        }
        
        void shutdown() {
            // 设置 watch 变量为 false
            watch = false;
            // 关闭线程池
            ThreadUtils.shutdownThreadPool(this.callBackExecutor);
        }
        
        @Override
        public void run() {
            while (watch) {
                try {
                    // 通过系统监听服务获取 watchKey
                    final WatchKey watchKey = watchService.take();
                    // 获取系统监听服务获取到到事件集合
                    final List<WatchEvent<?>> events = watchKey.pollEvents();
                    // 重置
                    watchKey.reset();
                    // 判断回调线程池是否被关闭
                    if (callBackExecutor.isShutdown()) {
                        return;
                    }
                    // 判断事件集合是否为空
                    if (events.isEmpty()) {
                        continue;
                    }
                    callBackExecutor.execute(() -> {
                        for (WatchEvent<?> event : events) {
                            WatchEvent.Kind<?> kind = event.kind();

                            // Since the OS's event cache may be overflow, a backstop is needed
                            if (StandardWatchEventKinds.OVERFLOW.equals(kind)) {
                                eventOverflow();
                            } else {
                                eventProcess(event.context());
                            }
                        }
                    });
                } catch (InterruptedException ignore) {
                    Thread.interrupted();
                } catch (Throwable ex) {
                    LOGGER.error("An exception occurred during file listening : ", ex);
                }
            }
        }
        
        private void eventProcess(Object context) {
            final FileChangeEvent fileChangeEvent = FileChangeEvent.builder().paths(paths).context(context).build();
            final String str = String.valueOf(context);
            for (final FileWatcher watcher : watchers) {
                if (watcher.interest(str)) {
                    Runnable job = () -> watcher.onChange(fileChangeEvent);
                    Executor executor = watcher.executor();
                    if (executor == null) {
                        try {
                            job.run();
                        } catch (Throwable ex) {
                            LOGGER.error("File change event callback error : ", ex);
                        }
                    } else {
                        executor.execute(job);
                    }
                }
            }
        }
        
        private void eventOverflow() {
            // 获取该目录下的所有的文件,每个文件进行单独处理
            File dir = Paths.get(paths).toFile();
            for (File file : Objects.requireNonNull(dir.listFiles())) {
                // Subdirectories do not participate in listening
                if (file.isDirectory()) {
                    continue;
                }
                eventProcess(file.getName());
            }
        }
        
    }
    
    private static void checkState() {
        if (CLOSED.get()) {
            throw new IllegalStateException("WatchFileCenter already shutdown");
        }
    }
}
