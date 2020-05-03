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

import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.misc.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Data replicator
 *
 * @author nkorange
 * @since 1.0.0
 */
@Component
// 该注解的意思应该是 必须先注入 serverListManager 才能注入该类
@DependsOn("serverListManager")
public class DataSyncer {

    /**
     * 数据仓库 内部存放键值对  同时通过一个key 来引用
     */
    @Autowired
    private DataStore dataStore;

    /**
     * 引入了全局配置对象
     */
    @Autowired
    private GlobalConfig partitionConfig;

    /**
     * 序列化工具
     */
    @Autowired
    private Serializer serializer;

    @Autowired
    private DistroMapper distroMapper;

    /**
     * 该对象负责维护集群内所有服务实例
     */
    @Autowired
    private ServerListManager serverListManager;

    private Map<String, String> taskMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        // 开启定时任务 定期将本节点维护的所有服务实例信息的校验和发往其他节点
        startTimedSync();
    }

    /**
     * 提交与某个server 进行数据同步的任务
     * @param task   task.keys  代表本次数据同步相关的所有service
     * @param delay
     */
    public void submit(SyncTask task, long delay) {

        // If it's a new task:  代表提交了一个新的任务
        if (task.getRetryCount() == 0) {
            // 将 task 内部的keys 都转移到 taskMap中
            Iterator<String> iterator = task.getKeys().iterator();
            while (iterator.hasNext()) {
                String key = iterator.next();
                // 代表本次任务已经存在了 为了避免重复执行任务  从迭代器中移除 key
                if (StringUtils.isNotBlank(taskMap.putIfAbsent(buildKey(key, task.getTargetServer()), key))) {
                    // associated key already exist:
                    if (Loggers.DISTRO.isDebugEnabled()) {
                        Loggers.DISTRO.debug("sync already in process, key: {}", key);
                    }
                    iterator.remove();
                }
            }
        }

        if (task.getKeys().isEmpty()) {
            // all keys are removed:
            return;
        }

        // 使用专门处理数据同步的线程池
        GlobalExecutor.submitDataSync(() -> {
            // 1. check the server
            if (getServers() == null || getServers().isEmpty()) {
                Loggers.SRV_LOG.warn("try to sync data but server list is empty.");
                return;
            }

            List<String> keys = task.getKeys();

            if (Loggers.SRV_LOG.isDebugEnabled()) {
                Loggers.SRV_LOG.debug("try to sync data for this keys {}.", keys);
            }
            // 2. get the datums by keys and check the datum is empty or not
            // 对应的key如果找不到数据了 就从 taskMap 中移除
            // key 对应service 相关的信息 然后每个service下有哪些instance 则是保存在 dataStore 中
            Map<String, Datum> datumMap = dataStore.batchGet(keys);
            if (datumMap == null || datumMap.isEmpty()) {
                // clear all flags of this task:
                // 如果没有实例信息 那么久不需要发送同步任务了
                for (String key : keys) {
                    taskMap.remove(buildKey(key, task.getTargetServer()));
                }
                return;
            }

            // 将数据序列化
            byte[] data = serializer.serialize(datumMap);

            long timestamp = System.currentTimeMillis();
            // 代表数据同步是否成功
            boolean success = NamingProxy.syncData(data, task.getTargetServer());
            if (!success) {
                // 重试
                SyncTask syncTask = new SyncTask();
                syncTask.setKeys(task.getKeys());
                syncTask.setRetryCount(task.getRetryCount() + 1);
                syncTask.setLastExecuteTime(timestamp);
                syncTask.setTargetServer(task.getTargetServer());
                retrySync(syncTask);
            } else {
                // clear all flags of this task:
                // 成功的情况下移除相关任务
                for (String key : task.getKeys()) {
                    taskMap.remove(buildKey(key, task.getTargetServer()));
                }
            }
        }, delay);
    }

    /**
     * 当某个同步任务执行失败时 尝试重发
     * @param syncTask
     */
    public void retrySync(SyncTask syncTask) {

        Server server = new Server();
        server.setIp(syncTask.getTargetServer().split(":")[0]);
        server.setServePort(Integer.parseInt(syncTask.getTargetServer().split(":")[1]));
        // 代表本节点失去响应
        if (!getServers().contains(server)) {
            // if server is no longer in healthy server list, ignore this task:
            //fix #1665 remove existing tasks
            // 移除同步任务 并返回
            if (syncTask.getKeys() != null) {
                for (String key : syncTask.getKeys()) {
                    taskMap.remove(buildKey(key, syncTask.getTargetServer()));
                }
            }
            return;
        }

        // TODO may choose other retry policy.
        // 在一定延时后重新执行任务
        submit(syncTask, partitionConfig.getSyncRetryDelay());
    }

    public void startTimedSync() {
        GlobalExecutor.schedulePartitionDataTimedSync(new TimedSync());
    }

    /**
     * 会定时执行该任务 负责数据同步  基于AP 实现的注册中心 需要定期同步数据   这里只是发送数据的校验和 校验和匹配不上才有同步数据的必要
     */
    public class TimedSync implements Runnable {

        @Override
        public void run() {

            try {

                if (Loggers.DISTRO.isDebugEnabled()) {
                    Loggers.DISTRO.debug("server list is: {}", getServers());
                }

                // send local timestamps to other servers:
                Map<String, String> keyChecksums = new HashMap<>(64);
                // dataStore下 包含了 nacos server 下所有的service
                for (String key : dataStore.keys()) {
                    // 确保该服务下有实例被注册在本注册中心上
                    if (!distroMapper.responsible(KeyBuilder.getServiceName(key))) {
                        continue;
                    }

                    // 找到对应的服务实例信息
                    Datum datum = dataStore.get(key);
                    if (datum == null) {
                        continue;
                    }
                    // 取出数据中的校验和
                    keyChecksums.put(key, datum.value.getChecksum());
                }

                if (keyChecksums.isEmpty()) {
                    return;
                }

                if (Loggers.DISTRO.isDebugEnabled()) {
                    Loggers.DISTRO.debug("sync checksums: {}", keyChecksums);
                }

                // 遍历集群内其他节点 并发送校验请求
                for (Server member : getServers()) {
                    if (NetUtils.localServer().equals(member.getKey())) {
                        continue;
                    }
                    // 将校验和信息发往其他节点  如果其他节点发现校验失败 应该就会触发一个同步动作啥的
                    NamingProxy.syncCheckSums(keyChecksums, member.getKey());
                }
            } catch (Exception e) {
                Loggers.DISTRO.error("timed sync task failed.", e);
            }
        }

    }

    /**
     * 通过该对象获取集群内其他可用的服务器
     * @return
     */
    public List<Server> getServers() {
        return serverListManager.getHealthyServers();
    }

    public String buildKey(String key, String targetServer) {
        return key + UtilsAndCommons.CACHE_KEY_SPLITER + targetServer;
    }
}
