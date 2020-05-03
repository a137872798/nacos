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

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.core.utils.SystemUtils;
import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.ServerStatus;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.naming.consistency.ApplyAction;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.ephemeral.EphemeralConsistencyService;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.*;
import com.alibaba.nacos.naming.pojo.Record;
import org.apache.commons.lang3.StringUtils;
import org.javatuples.Pair;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A consistency protocol algorithm called <b>Distro</b>
 * <p>
 * Use a distro algorithm to divide data into many blocks. Each Nacos server node takes
 * responsibility for exactly one block of data. Each block of data is generated, removed
 * and synchronized by its responsible server. So every Nacos server only handles writings
 * for a subset of the total service data.
 * <p>
 * At mean time every Nacos server receives data sync of other Nacos server, so every Nacos
 * server will eventually have a complete set of data.
 *
 * @author nkorange
 * @since 1.0.0
 * 代表本注册中心不需要做数据持久化
 */
@org.springframework.stereotype.Service("distroConsistencyService")
public class DistroConsistencyServiceImpl implements EphemeralConsistencyService {

    @Autowired
    private DistroMapper distroMapper;

    /**
     * 该对象负责存储数据   每当发生 onChange事件时 需要通过 key 去该对象下拉取所有服务实例信息  进而触发监听器
     */
    @Autowired
    private DataStore dataStore;

    /**
     * 负责将更新动作同步到其他节点
     */
    @Autowired
    private TaskDispatcher taskDispatcher;

    /**
     * 序列化对象
     */
    @Autowired
    private Serializer serializer;

    /**
     * 记录当前集群所有可用节点
     */
    @Autowired
    private ServerListManager serverListManager;

    @Autowired
    private SwitchDomain switchDomain;

    @Autowired
    private GlobalConfig globalConfig;

    private boolean initialized = false;

    /**
     * 该对象轮询扫描一个阻塞队列 并根据情况触发监听器的 onDelete / onChange 方法
     */
    private volatile Notifier notifier = new Notifier();

    private LoadDataTask loadDataTask = new LoadDataTask();

    /**
     * 每个key 会被一组监听器 订阅  key 应该就是service（加工过的）
     */
    private Map<String, CopyOnWriteArrayList<RecordListener>> listeners = new ConcurrentHashMap<>();

    private Map<String, String> syncChecksumTasks = new ConcurrentHashMap<>(16);

    /**
     * 初始化方法  注册中心中某个实例启动时 不是尝试从本机历史数据文件中加载服务实例信息 而是先尝试与其他服务实例同步数据  如果其他节点也处于刚启动的状态呢
     */
    @PostConstruct
    public void init() {
        GlobalExecutor.submit(loadDataTask);
        GlobalExecutor.submitDistroNotifyTask(notifier);
    }

    /**
     * 当某节点启动时 尝试与集群中其他节点同步数据  不需要从持久化文件中读取
     */
    private class LoadDataTask implements Runnable {

        @Override
        public void run() {
            try {
                load();
                // 代表数据同步失败  一定延时后继续执行
                if (!initialized) {
                    GlobalExecutor.submit(this, globalConfig.getLoadDataRetryDelayMillis());
                }
            } catch (Exception e) {
                Loggers.DISTRO.error("load data failed.", e);
            }
        }
    }

    /**
     * 开始加载数据
     * @throws Exception
     */
    public void load() throws Exception {
        // 单节点模式下不需要加载数据
        if (SystemUtils.STANDALONE_MODE) {
            initialized = true;
            return;
        }
        // size = 1 means only myself in the list, we need at least one another server alive:
        // 单节点情况下 等待其他节点上线
        while (serverListManager.getHealthyServers().size() <= 1) {
            Thread.sleep(1000L);
            Loggers.DISTRO.info("waiting server list init...");
        }

        for (Server server : serverListManager.getHealthyServers()) {
            if (NetUtils.localServer().equals(server.getKey())) {
                continue;
            }
            if (Loggers.DISTRO.isDebugEnabled()) {
                Loggers.DISTRO.debug("sync from " + server);
            }
            // try sync data from remote server:
            if (syncAllDataFromRemote(server)) {
                initialized = true;
                return;
            }
        }
    }

    /**
     * 存储一组键值对
     * @param key   key of data, this key should be globally unique
     * @param value value of data
     * @throws NacosException
     */
    @Override
    public void put(String key, Record value) throws NacosException {
        onPut(key, value);
        // 将本次请求同步到集群中其他节点
        taskDispatcher.addTask(key);
    }

    /**
     * 从一致性服务中 移除某个key
     * @param key key of data
     * @throws NacosException
     */
    @Override
    public void remove(String key) throws NacosException {
        onRemove(key);
        // 同时移除相关的监听器   监听器都被移除了 这个 onRemove 怎么触发？？？  这不是异步的吗   不会是认为那个不断自旋的拉取任务一定比这里移除动作要快把
        listeners.remove(key);
    }

    /**
     * 从一致性存储中获取某个数据
     * @param key key of data
     * @return
     * @throws NacosException
     */
    @Override
    public Datum get(String key) throws NacosException {
        return dataStore.get(key);
    }

    /**
     * put() 会转发到这里
     * @param key
     * @param value  某服务下的所有实例  这里是全量数据
     */
    public void onPut(String key, Record value) {

        if (KeyBuilder.matchEphemeralInstanceListKey(key)) {
            Datum<Instances> datum = new Datum<>();
            datum.value = (Instances) value;
            datum.key = key;
            // 相当于版本号
            datum.timestamp.incrementAndGet();
            // 将数据存储到数据仓库
            dataStore.put(key, datum);
        }

        // 如果没有相关监听器 直接返回
        if (!listeners.containsKey(key)) {
            return;
        }

        notifier.addTask(key, ApplyAction.CHANGE);
    }

    public void onRemove(String key) {

        dataStore.remove(key);

        if (!listeners.containsKey(key)) {
            return;
        }

        notifier.addTask(key, ApplyAction.DELETE);
    }

    /**
     * 收到了其他节点的数据同步请求
     * @param checksumMap
     * @param server
     */
    public void onReceiveChecksums(Map<String, String> checksumMap, String server) {

        // 避免重复处理
        if (syncChecksumTasks.containsKey(server)) {
            // Already in process of this server:
            Loggers.DISTRO.warn("sync checksum task already in process with {}", server);
            return;
        }

        // 随便设置一个占位符
        syncChecksumTasks.put(server, "1");

        try {

            List<String> toUpdateKeys = new ArrayList<>();
            List<String> toRemoveKeys = new ArrayList<>();
            for (Map.Entry<String, String> entry : checksumMap.entrySet()) {
                if (distroMapper.responsible(KeyBuilder.getServiceName(entry.getKey()))) {
                    // this key should not be sent from remote server:
                    Loggers.DISTRO.error("receive responsible key timestamp of " + entry.getKey() + " from " + server);
                    // abort the procedure:
                    return;
                }

                // 代表需要进行更新
                if (!dataStore.contains(entry.getKey()) ||
                    dataStore.get(entry.getKey()).value == null ||
                    !dataStore.get(entry.getKey()).value.getChecksum().equals(entry.getValue())) {
                    toUpdateKeys.add(entry.getKey());
                }
            }

            for (String key : dataStore.keys()) {

                if (!server.equals(distroMapper.mapSrv(KeyBuilder.getServiceName(key)))) {
                    continue;
                }

                // 代表该服务已经不再提供了  需要被移除
                if (!checksumMap.containsKey(key)) {
                    toRemoveKeys.add(key);
                }
            }

            if (Loggers.DISTRO.isDebugEnabled()) {
                Loggers.DISTRO.info("to remove keys: {}, to update keys: {}, source: {}", toRemoveKeys, toUpdateKeys, server);
            }

            for (String key : toRemoveKeys) {
                onRemove(key);
            }

            if (toUpdateKeys.isEmpty()) {
                return;
            }

            try {
                // 通过对比校验和 找到发生变化的服务 然后去对应节点获取真正的数据
                byte[] result = NamingProxy.getData(toUpdateKeys, server);
                processData(result);
            } catch (Exception e) {
                Loggers.DISTRO.error("get data from " + server + " failed!", e);
            }
        } finally {
            // Remove this 'in process' flag:
            syncChecksumTasks.remove(server);
        }

    }

    /**
     * 尝试与远端同步数据
     * @param server
     * @return
     */
    public boolean syncAllDataFromRemote(Server server) {

        try {
            // key 内包含了 该节点的 ip port
            byte[] data = NamingProxy.getAllData(server.getKey());
            // 处理拉取到的数据 这里可以考虑使用压缩算法 提高传输效率  eureka 是有使用压缩算法的
            processData(data);
            return true;
        } catch (Exception e) {
            Loggers.DISTRO.error("sync full data from " + server + " failed!", e);
            return false;
        }
    }

    /**
     * 同步从其他server拉取到的数据
     * @param data
     * @throws Exception
     */
    public void processData(byte[] data) throws Exception {
        if (data.length > 0) {
            // 反序列化生成实例信息
            Map<String, Datum<Instances>> datumMap =
                serializer.deserializeMap(data, Instances.class);

            // 更新服务提供者数据
            for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) {
                dataStore.put(entry.getKey(), entry.getValue());

                // 如果针对某个 key 没有设置监听器
                if (!listeners.containsKey(entry.getKey())) {
                    // pretty sure the service not exist:
                    // 同时创建服务实例
                    if (switchDomain.isDefaultInstanceEphemeral()) {
                        // create empty service
                        Loggers.DISTRO.info("creating service {}", entry.getKey());
                        // 在本节点上构建关于新服务实例的信息
                        Service service = new Service();
                        String serviceName = KeyBuilder.getServiceName(entry.getKey());
                        String namespaceId = KeyBuilder.getNamespace(entry.getKey());
                        service.setName(serviceName);
                        service.setNamespaceId(namespaceId);
                        service.setGroupName(Constants.DEFAULT_GROUP);
                        // now validate the service. if failed, exception will be thrown
                        service.setLastModifiedMillis(System.currentTimeMillis());
                        // 生成校验和信息
                        service.recalculateChecksum();
                        // 触发nacos 内置的某个监听器  实际上就是触发 ServiceManager 这样就会使用该service 信息去更新 nacos本地的服务信息 又或者 进行初始化(代表该服务是新添加的)
                        listeners.get(KeyBuilder.SERVICE_META_KEY_PREFIX).get(0)
                            .onChange(KeyBuilder.buildServiceMetaKey(namespaceId, serviceName), service);
                    }
                }
            }

            // 开始遍历实例信息 并填充到 service 内
            for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) {

                if (!listeners.containsKey(entry.getKey())) {
                    // Should not happen:
                    Loggers.DISTRO.warn("listener of {} not found.", entry.getKey());
                    continue;
                }

                try {
                    // 通过实例信息触发监听器   此时的监听器 就是 service (service 会负责监听下面所有的instance)
                    for (RecordListener listener : listeners.get(entry.getKey())) {
                        listener.onChange(entry.getKey(), entry.getValue().value);
                    }
                } catch (Exception e) {
                    Loggers.DISTRO.error("[NACOS-DISTRO] error while execute listener of key: {}", entry.getKey(), e);
                    continue;
                }

                // Update data store if listener executed successfully:
                // 将数据转移到存储对象中  基于接口开发的话  该对象之后还可以拓展出持久化实现
                dataStore.put(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * 为某个key 注册监听器 该监听器 监听的对象可以是 instance 也可以是 service
     * @param key      key of data
     * @param listener callback of data change
     * @throws NacosException
     */
    @Override
    public void listen(String key, RecordListener listener) throws NacosException {
        if (!listeners.containsKey(key)) {
            listeners.put(key, new CopyOnWriteArrayList<>());
        }

        if (listeners.get(key).contains(listener)) {
            return;
        }

        listeners.get(key).add(listener);
    }

    @Override
    public void unlisten(String key, RecordListener listener) throws NacosException {
        if (!listeners.containsKey(key)) {
            return;
        }
        for (RecordListener recordListener : listeners.get(key)) {
            if (recordListener.equals(listener)) {
                listeners.get(key).remove(listener);
                break;
            }
        }
    }

    /**
     * 判断当前一致性服务是否可用
     * @return
     */
    @Override
    public boolean isAvailable() {
        return isInitialized() || ServerStatus.UP.name().equals(switchDomain.getOverriddenServerStatus());
    }

    public boolean isInitialized() {
        return initialized || !globalConfig.isDataWarmup();
    }

    /**
     * 负责触发监听器
     */
    public class Notifier implements Runnable {

        /**
         * 该容器相当于是去重 避免某个任务在很短的时间内被重复添加到 task中
         */
        private ConcurrentHashMap<String, String> services = new ConcurrentHashMap<>(10 * 1024);

        // Pair 是一组键值对
        private BlockingQueue<Pair> tasks = new LinkedBlockingQueue<Pair>(1024 * 1024);

        public void addTask(String datumKey, ApplyAction action) {

            if (services.containsKey(datumKey) && action == ApplyAction.CHANGE) {
                return;
            }
            // 这里只是设置一个标识 不需要实际数据
            if (action == ApplyAction.CHANGE) {
                services.put(datumKey, StringUtils.EMPTY);
            }
            tasks.add(Pair.with(datumKey, action));
        }

        public int getTaskSize() {
            return tasks.size();
        }

        @Override
        public void run() {
            Loggers.DISTRO.info("distro notifier started");

            while (true) {
                try {

                    Pair pair = tasks.take();

                    if (pair == null) {
                        continue;
                    }

                    String datumKey = (String) pair.getValue0();
                    ApplyAction action = (ApplyAction) pair.getValue1();

                    services.remove(datumKey);

                    int count = 0;

                    if (!listeners.containsKey(datumKey)) {
                        continue;
                    }

                    // 一般注册中心都会针对某个key 提供监听机制  当感知到配置发生变化时 就触发监听器  而在分布式系统中监听器就可以是通过 rpc调用来实现
                    for (RecordListener listener : listeners.get(datumKey)) {

                        count++;

                        try {
                            if (action == ApplyAction.CHANGE) {
                                // 实例数据从 dataStore 中获取   那么dataStore 应该是要做持久化的  不然注册中心宕机了 恢复时没有任何实例信息 还是说其他节点会负责同步工作
                                listener.onChange(datumKey, dataStore.get(datumKey).value);
                                continue;
                            }

                            if (action == ApplyAction.DELETE) {
                                listener.onDelete(datumKey);
                                continue;
                            }
                        } catch (Throwable e) {
                            Loggers.DISTRO.error("[NACOS-DISTRO] error while notifying listener of key: {}", datumKey, e);
                        }
                    }

                    if (Loggers.DISTRO.isDebugEnabled()) {
                        Loggers.DISTRO.debug("[NACOS-DISTRO] datum change notified, key: {}, listener count: {}, action: {}",
                            datumKey, count, action.name());
                    }
                } catch (Throwable e) {
                    Loggers.DISTRO.error("[NACOS-DISTRO] Error while handling notifying task", e);
                }
            }
        }
    }
}
