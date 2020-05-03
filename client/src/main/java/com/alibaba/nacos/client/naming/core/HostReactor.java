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
package com.alibaba.nacos.client.naming.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.backups.FailoverReactor;
import com.alibaba.nacos.client.naming.cache.DiskCache;
import com.alibaba.nacos.client.naming.net.NamingProxy;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.*;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * @author xuanyin
 */
public class HostReactor {

    private static final long DEFAULT_DELAY = 1000L;

    private static final long UPDATE_HOLD_INTERVAL = 5000L;

    /**
     * 代表已经为某个服务准备了下次更新
     */
    private final Map<String, ScheduledFuture<?>> futureMap = new HashMap<String, ScheduledFuture<?>>();

    /**
     * 存放服务信息的map
     */
    private Map<String, ServiceInfo> serviceInfoMap;

    private Map<String, Object> updatingMap;

    /**
     * 该对象专门用于接收数据 并将相关方法转发到本对象处理
     */
    private PushReceiver pushReceiver;

    /**
     * 该对象将 服务info 信息的变更通知到关联的监听器
     */
    private EventDispatcher eventDispatcher;

    private NamingProxy serverProxy;

    /**
     * 定期保存服务数据 并定期更新内部的缓存
     */
    private FailoverReactor failoverReactor;

    private String cacheDir;

    private ScheduledExecutorService executor;

    public HostReactor(EventDispatcher eventDispatcher, NamingProxy serverProxy, String cacheDir) {
        this(eventDispatcher, serverProxy, cacheDir, false, UtilAndComs.DEFAULT_POLLING_THREAD_COUNT);
    }

    public HostReactor(EventDispatcher eventDispatcher, NamingProxy serverProxy, String cacheDir,
                       boolean loadCacheAtStart, int pollingThreadCount) {

        executor = new ScheduledThreadPoolExecutor(pollingThreadCount, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName("com.alibaba.nacos.client.naming.updater");
                return thread;
            }
        });

        this.eventDispatcher = eventDispatcher;
        this.serverProxy = serverProxy;
        this.cacheDir = cacheDir;
        // 如果指定了缓存文件 且在初始化时 就开始加载
        if (loadCacheAtStart) {
            this.serviceInfoMap = new ConcurrentHashMap<String, ServiceInfo>(DiskCache.read(this.cacheDir));
        } else {
            this.serviceInfoMap = new ConcurrentHashMap<String, ServiceInfo>(16);
        }

        this.updatingMap = new ConcurrentHashMap<String, Object>();
        // 分别启动2个组件
        // 容灾组件 负责定期将缓存中的服务信息写入到容灾文件 并且在开启容灾的情况下 使用容灾文件的数据来初始化缓存
        // 避免注册中心不可用的情况
        this.failoverReactor = new FailoverReactor(this, cacheDir);
        // 类似与配置中心的长轮询 注册中心客户端也有一个定期接收 server新数据的任务对象
        this.pushReceiver = new PushReceiver(this);
    }

    public Map<String, ServiceInfo> getServiceInfoMap() {
        return serviceInfoMap;
    }

    /**
     * 定期拉取某个服务的最新信息
     * @param task
     * @return
     */
    public synchronized ScheduledFuture<?> addTask(UpdateTask task) {
        return executor.schedule(task, DEFAULT_DELAY, TimeUnit.MILLISECONDS);
    }

    /**
     * 将json 字符串转换成 服务信息
     * @param json
     * @return
     */
    public ServiceInfo processServiceJSON(String json) {
        ServiceInfo serviceInfo = JSON.parseObject(json, ServiceInfo.class);
        ServiceInfo oldService = serviceInfoMap.get(serviceInfo.getKey());
        // 代表从udp 收到的数据不完整 那么忽略
        if (serviceInfo.getHosts() == null || !serviceInfo.validate()) {
            //empty or error push, just ignore
            return oldService;
        }

        boolean changed = false;

        if (oldService != null) {

            if (oldService.getLastRefTime() > serviceInfo.getLastRefTime()) {
                NAMING_LOGGER.warn("out of date data received, old-t: " + oldService.getLastRefTime()
                    + ", new-t: " + serviceInfo.getLastRefTime());
                // TODO 这里应该加个 return 吧
            }

            // 注意 即使是旧数据 还是会进行覆盖
            serviceInfoMap.put(serviceInfo.getKey(), serviceInfo);

            // 找到能提供该服务的一组服务提供者
            Map<String, Instance> oldHostMap = new HashMap<String, Instance>(oldService.getHosts().size());
            for (Instance host : oldService.getHosts()) {
                oldHostMap.put(host.toInetAddr(), host);
            }

            Map<String, Instance> newHostMap = new HashMap<String, Instance>(serviceInfo.getHosts().size());
            for (Instance host : serviceInfo.getHosts()) {
                newHostMap.put(host.toInetAddr(), host);
            }

            Set<Instance> modHosts = new HashSet<Instance>();
            Set<Instance> newHosts = new HashSet<Instance>();
            Set<Instance> remvHosts = new HashSet<Instance>();

            List<Map.Entry<String, Instance>> newServiceHosts = new ArrayList<Map.Entry<String, Instance>>(
                newHostMap.entrySet());
            for (Map.Entry<String, Instance> entry : newServiceHosts) {
                Instance host = entry.getValue();
                String key = entry.getKey();
                // 找到发生变化的 host
                if (oldHostMap.containsKey(key) && !StringUtils.equals(host.toString(),
                    oldHostMap.get(key).toString())) {
                    modHosts.add(host);
                    continue;
                }

                // 代表是新加入的 host
                if (!oldHostMap.containsKey(key)) {
                    newHosts.add(host);
                }
            }

            for (Map.Entry<String, Instance> entry : oldHostMap.entrySet()) {
                Instance host = entry.getValue();
                String key = entry.getKey();
                if (newHostMap.containsKey(key)) {
                    continue;
                }

                if (!newHostMap.containsKey(key)) {
                    remvHosts.add(host);
                }

            }

            // 打印发生变化的端点信息
            if (newHosts.size() > 0) {
                changed = true;
                NAMING_LOGGER.info("new ips(" + newHosts.size() + ") service: "
                    + serviceInfo.getKey() + " -> " + JSON.toJSONString(newHosts));
            }

            if (remvHosts.size() > 0) {
                changed = true;
                NAMING_LOGGER.info("removed ips(" + remvHosts.size() + ") service: "
                    + serviceInfo.getKey() + " -> " + JSON.toJSONString(remvHosts));
            }

            if (modHosts.size() > 0) {
                changed = true;
                NAMING_LOGGER.info("modified ips(" + modHosts.size() + ") service: "
                    + serviceInfo.getKey() + " -> " + JSON.toJSONString(modHosts));
            }

            //
            serviceInfo.setJsonFromServer(json);

            if (newHosts.size() > 0 || remvHosts.size() > 0 || modHosts.size() > 0) {
                // 因为服务实例发生了变化 所以要通知监听器
                eventDispatcher.serviceChanged(serviceInfo);
                // 将信息写入到 文件
                DiskCache.write(serviceInfo, cacheDir);
            }

        } else {
            // 首次存储数据
            changed = true;
            NAMING_LOGGER.info("init new ips(" + serviceInfo.ipCount() + ") service: " + serviceInfo.getKey() + " -> " + JSON
                .toJSONString(serviceInfo.getHosts()));
            serviceInfoMap.put(serviceInfo.getKey(), serviceInfo);
            eventDispatcher.serviceChanged(serviceInfo);
            serviceInfo.setJsonFromServer(json);
            DiskCache.write(serviceInfo, cacheDir);
        }

        MetricsMonitor.getServiceInfoMapSizeMonitor().set(serviceInfoMap.size());

        if (changed) {
            NAMING_LOGGER.info("current ips:(" + serviceInfo.ipCount() + ") service: " + serviceInfo.getKey() +
                " -> " + JSON.toJSONString(serviceInfo.getHosts()));
        }

        return serviceInfo;
    }

    private ServiceInfo getServiceInfo0(String serviceName, String clusters) {

        String key = ServiceInfo.getKey(serviceName, clusters);

        // 内部的数据是通过接收udp 发送的数据包生成的
        return serviceInfoMap.get(key);
    }

    /**
     * 从 nacos server 上拉取某个服务的最新信息
     * @param serviceName
     * @param clusters
     * @return
     * @throws NacosException
     */
    public ServiceInfo getServiceInfoDirectlyFromServer(final String serviceName, final String clusters) throws NacosException {
        String result = serverProxy.queryList(serviceName, clusters, 0, false);
        if (StringUtils.isNotEmpty(result)) {
            return JSON.parseObject(result, ServiceInfo.class);
        }
        return null;
    }

    public ServiceInfo getServiceInfo(final String serviceName, final String clusters) {

        NAMING_LOGGER.debug("failover-mode: " + failoverReactor.isFailoverSwitch());
        String key = ServiceInfo.getKey(serviceName, clusters);
        // 如果当前处于容灾状态   使用之前订阅时收集到的 udp 数据
        if (failoverReactor.isFailoverSwitch()) {
            return failoverReactor.getService(key);
        }

        // 获取一级缓存数据
        ServiceInfo serviceObj = getServiceInfo0(serviceName, clusters);

        // 代表本次还没有作为订阅者注册到 nacos-naming server 或者 还没有收到首批数据 那么立即拉取一次最新数据
        if (null == serviceObj) {
            serviceObj = new ServiceInfo(serviceName, clusters);

            serviceInfoMap.put(serviceObj.getKey(), serviceObj);

            updatingMap.put(serviceName, new Object());
            // 就是从 nacos server 拉取最新的服务信息
            updateServiceNow(serviceName, clusters);
            updatingMap.remove(serviceName);

            // 发现正在更新了 就等待
        } else if (updatingMap.containsKey(serviceName)) {

            if (UPDATE_HOLD_INTERVAL > 0) {
                // hold a moment waiting for update finish
                synchronized (serviceObj) {
                    try {
                        serviceObj.wait(UPDATE_HOLD_INTERVAL);
                    } catch (InterruptedException e) {
                        NAMING_LOGGER.error("[getServiceInfo] serviceName:" + serviceName + ", clusters:" + clusters, e);
                    }
                }
            }
        }

        // 尝试提交一个更新任务   因为如果长时间没有收到推送数据 那么无法确定是对端服务器宕机了 无法发送更新数据 还是真的没有数据更新 (对端只会在确认数据更新时 发送udp包)
        scheduleUpdateIfAbsent(serviceName, clusters);

        // 上面2个分支会将数据存入到map 中
        return serviceInfoMap.get(serviceObj.getKey());
    }



    public void scheduleUpdateIfAbsent(String serviceName, String clusters) {
        // 同样的思路 先存放在一个文件中 如果已经设置了 就不需要添加更新任务了
        if (futureMap.get(ServiceInfo.getKey(serviceName, clusters)) != null) {
            return;
        }

        synchronized (futureMap) {
            if (futureMap.get(ServiceInfo.getKey(serviceName, clusters)) != null) {
                return;
            }

            ScheduledFuture<?> future = addTask(new UpdateTask(serviceName, clusters));
            futureMap.put(ServiceInfo.getKey(serviceName, clusters), future);
        }
    }

    public void updateServiceNow(String serviceName, String clusters) {
        ServiceInfo oldService = getServiceInfo0(serviceName, clusters);
        try {
            // 将自身的udp端口暴露到 nacos-naming server 方便对端发数据包
            String result = serverProxy.queryList(serviceName, clusters, pushReceiver.getUDPPort(), false);

            if (StringUtils.isNotEmpty(result)) {
                processServiceJSON(result);
            }
        } catch (Exception e) {
            NAMING_LOGGER.error("[NA] failed to update serviceName: " + serviceName, e);
        } finally {
            if (oldService != null) {
                synchronized (oldService) {
                    oldService.notifyAll();
                }
            }
        }
    }

    public void refreshOnly(String serviceName, String clusters) {
        try {
            serverProxy.queryList(serviceName, clusters, pushReceiver.getUDPPort(), false);
        } catch (Exception e) {
            NAMING_LOGGER.error("[NA] failed to update serviceName: " + serviceName, e);
        }
    }

    /**
     * 当本节点作为订阅者订阅到某个nacos-naming server 后 集群内只有那个节点 会发送数据到该client 而不是向所有server 发送订阅请求
     * 那么这里server就有宕机的可能
     * 所以这里还需要另一个机制 确保能主动的拉取最新的数据
     */
    public class UpdateTask implements Runnable {
        long lastRefTime = Long.MAX_VALUE;
        private String clusters;
        private String serviceName;

        public UpdateTask(String serviceName, String clusters) {
            this.serviceName = serviceName;
            this.clusters = clusters;
        }

        @Override
        public void run() {
            try {
                // 获取某服务当前信息
                ServiceInfo serviceObj = serviceInfoMap.get(ServiceInfo.getKey(serviceName, clusters));

                // 每次调用 updateServiceNow 都可能会往新的server上注册订阅消息

                if (serviceObj == null) {
                    updateServiceNow(serviceName, clusters);
                    executor.schedule(this, DEFAULT_DELAY, TimeUnit.MILLISECONDS);
                    return;
                }

                // 代表长时间没有收到推送的数据  那么很可能集群中对应节点已经宕机了  这里触发 updateServiceNow 的同时 还重新与集群中某个节点建立了 订阅关系
                if (serviceObj.getLastRefTime() <= lastRefTime) {
                    updateServiceNow(serviceName, clusters);
                    serviceObj = serviceInfoMap.get(ServiceInfo.getKey(serviceName, clusters));
                } else {
                    // if serviceName already updated by push, we should not override it
                    // since the push data may be different from pull through force push
                    // 已经经过 udp的数据包推送进行更新了
                    refreshOnly(serviceName, clusters);
                }

                lastRefTime = serviceObj.getLastRefTime();

                // 只要订阅任务还存在 就会在一定延时后继续执行
                if (!eventDispatcher.isSubscribed(serviceName, clusters) &&
                    !futureMap.containsKey(ServiceInfo.getKey(serviceName, clusters))) {
                    // abort the update task:
                    NAMING_LOGGER.info("update task is stopped, service:" + serviceName + ", clusters:" + clusters);
                    return;
                }

                // 在一定时间后判断是否要继续更新
                executor.schedule(this, serviceObj.getCacheMillis(), TimeUnit.MILLISECONDS);


            } catch (Throwable e) {
                NAMING_LOGGER.warn("[NA] failed to update serviceName: " + serviceName, e);
            }

        }
    }
}
