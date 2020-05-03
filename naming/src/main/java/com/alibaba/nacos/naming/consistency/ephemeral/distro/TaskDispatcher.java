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
package com.alibaba.nacos.naming.consistency.ephemeral.distro;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.misc.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Data sync task dispatcher
 *
 * @author nkorange
 * @since 1.0.0
 * 任务分发器 对象
 * 对应的抽象层是当插入某个服务时 做一个任务分发的逻辑 一般情况下就是集群间的数据同步 比如基于AP模式肯定就要将本次的bian't
 */
@Component
public class TaskDispatcher {

    @Autowired
    private GlobalConfig partitionConfig;

    /**
     * 对应基于AP的注册中心  每当往某节点注册提供者时 信息需要同步到集群其他节点
     */
    @Autowired
    private DataSyncer dataSyncer;

    /**
     * 代表n 个任务处理器
     */
    private List<TaskScheduler> taskSchedulerList = new ArrayList<>();

    private final int cpuCoreCount = Runtime.getRuntime().availableProcessors();


    @PostConstruct
    public void init() {
        for (int i = 0; i < cpuCoreCount; i++) {
            TaskScheduler taskScheduler = new TaskScheduler(i);
            taskSchedulerList.add(taskScheduler);
            GlobalExecutor.submitTaskDispatch(taskScheduler);
        }
    }

    /**
     * 采用hash负载的方式 插入到某个 定时任务下
     * 当 通过 DistroConsistencyServiceImpl 插入某个 service 下所有服务实例时就会触发该方法
     * @param key
     */
    public void addTask(String key) {
        taskSchedulerList.get(UtilsAndCommons.shakeUp(key, cpuCoreCount)).addTask(key);
    }

    /**
     * 任务定时器
     */
    public class TaskScheduler implements Runnable {

        private int index;

        private int dataSize = 0;

        private long lastDispatchTime = 0L;

        private BlockingQueue<String> queue = new LinkedBlockingQueue<>(128 * 1024);

        public TaskScheduler(int index) {
            this.index = index;
        }

        public void addTask(String key) {
            queue.offer(key);
        }

        public int getIndex() {
            return index;
        }

        @Override
        public void run() {

            List<String> keys = new ArrayList<>();
            while (true) {

                try {

                    // 从队列中拉取 key  对应某个 service的信息
                    String key = queue.poll(partitionConfig.getTaskDispatchPeriod(),
                        TimeUnit.MILLISECONDS);

                    if (Loggers.DISTRO.isDebugEnabled() && StringUtils.isNotBlank(key)) {
                        Loggers.DISTRO.debug("got key: {}", key);
                    }

                    // 如果集群中没有节点可用 则忽略
                    if (dataSyncer.getServers() == null || dataSyncer.getServers().isEmpty()) {
                        continue;
                    }

                    if (StringUtils.isBlank(key)) {
                        continue;
                    }

                    // 每当处理完一批数据后 dataSize 要重置成0  所以keys要清空
                    if (dataSize == 0) {
                        keys = new ArrayList<>();
                    }

                    keys.add(key);
                    dataSize++;

                    // 这里是尽可能采用批量处理  等数量达到1000 或者 距离上一次分发 经过了多久
                    if (dataSize == partitionConfig.getBatchSyncKeyCount() ||
                        (System.currentTimeMillis() - lastDispatchTime) > partitionConfig.getTaskDispatchPeriod()) {

                        for (Server member : dataSyncer.getServers()) {
                            if (NetUtils.localServer().equals(member.getKey())) {
                                continue;
                            }
                            // 生成同步任务
                            SyncTask syncTask = new SyncTask();
                            syncTask.setKeys(keys);
                            syncTask.setTargetServer(member.getKey());

                            if (Loggers.DISTRO.isDebugEnabled() && StringUtils.isNotBlank(key)) {
                                Loggers.DISTRO.debug("add sync task: {}", JSON.toJSONString(syncTask));
                            }

                            // 该对象会负责与其他server 进行通信
                            dataSyncer.submit(syncTask, 0);
                        }
                        lastDispatchTime = System.currentTimeMillis();
                        dataSize = 0;
                    }

                } catch (Exception e) {
                    Loggers.DISTRO.error("dispatch sync task failed.", e);
                }
            }
        }
    }
}
