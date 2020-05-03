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

import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * @author xuanyin
 * 事件分发器
 */
public class EventDispatcher {

    private ExecutorService executor = null;

    /**
     * 发生变化的服务信息会存放在该队列中
     */
    private BlockingQueue<ServiceInfo> changedServices = new LinkedBlockingQueue<ServiceInfo>();

    /**
     * 服务名  与 对应的一组监听器
     */
    private ConcurrentMap<String, List<EventListener>> observerMap
        = new ConcurrentHashMap<String, List<EventListener>>();

    public EventDispatcher() {

        executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "com.alibaba.nacos.naming.client.listener");
                thread.setDaemon(true);

                return thread;
            }
        });

        // 该对象从阻塞队列中获取已更新的服务实例 并触发对应的监听器
        executor.execute(new Notifier());
    }

    /**
     * 针对某个服务信息 创建监听器
     * @param serviceInfo
     * @param clusters
     * @param listener
     */
    public void addListener(ServiceInfo serviceInfo, String clusters, EventListener listener) {

        NAMING_LOGGER.info("[LISTENER] adding " + serviceInfo.getName() + " with " + clusters + " to listener map");
        List<EventListener> observers = Collections.synchronizedList(new ArrayList<EventListener>());
        observers.add(listener);

        observers = observerMap.putIfAbsent(ServiceInfo.getKey(serviceInfo.getName(), clusters), observers);
        if (observers != null) {
            observers.add(listener);
        }

        serviceChanged(serviceInfo);
    }

    public void removeListener(String serviceName, String clusters, EventListener listener) {

        NAMING_LOGGER.info("[LISTENER] removing " + serviceName + " with " + clusters + " from listener map");

        // 服务和集群可以生成某个key 之后可以在map中找到对应的一组监听器
        List<EventListener> observers = observerMap.get(ServiceInfo.getKey(serviceName, clusters));
        if (observers != null) {
            Iterator<EventListener> iter = observers.iterator();
            while (iter.hasNext()) {
                EventListener oldListener = iter.next();
                if (oldListener.equals(listener)) {
                    iter.remove();
                }
            }
            if (observers.isEmpty()) {
                observerMap.remove(ServiceInfo.getKey(serviceName, clusters));
            }
        }
    }

    /**
     * 通过 service 和 cluster 生成key  并判断是否包含相关监听器
     * @param serviceName
     * @param clusters
     * @return
     */
    public boolean isSubscribed(String serviceName, String clusters) {
        return observerMap.containsKey(ServiceInfo.getKey(serviceName, clusters));
    }

    /**
     * 当前订阅的所有服务
     * @return
     */
    public List<ServiceInfo> getSubscribeServices() {
        List<ServiceInfo> serviceInfos = new ArrayList<ServiceInfo>();
        for (String key : observerMap.keySet()) {
            serviceInfos.add(ServiceInfo.fromKey(key));
        }
        return serviceInfos;
    }

    public void serviceChanged(ServiceInfo serviceInfo) {
        if (serviceInfo == null) {
            return;
        }

        changedServices.add(serviceInfo);
    }

    /**
     * 当注册中心启动时 会开启该任务
     */
    private class Notifier implements Runnable {
        @Override
        public void run() {
            while (true) {
                ServiceInfo serviceInfo = null;
                try {
                    // 拉取队列中存储的 发生变化的服务信息
                    serviceInfo = changedServices.poll(5, TimeUnit.MINUTES);
                } catch (Exception ignore) {
                }

                if (serviceInfo == null) {
                    continue;
                }

                try {
                    // 获取对应的一组监听器
                    List<EventListener> listeners = observerMap.get(serviceInfo.getKey());

                    if (!CollectionUtils.isEmpty(listeners)) {
                        for (EventListener listener : listeners) {
                            List<Instance> hosts = Collections.unmodifiableList(serviceInfo.getHosts());
                            // 生成服务信息更新事件 并触发监听器
                            listener.onEvent(new NamingEvent(serviceInfo.getName(), serviceInfo.getGroupName(), serviceInfo.getClusters(), hosts));
                        }
                    }

                } catch (Exception e) {
                    NAMING_LOGGER.error("[NA] notify error for service: "
                        + serviceInfo.getName() + ", clusters: " + serviceInfo.getClusters(), e);
                }
            }
        }
    }
}
