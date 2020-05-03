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
package com.alibaba.nacos.naming.core;

import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.nacos.naming.healthcheck.HealthCheckReactor;
import com.alibaba.nacos.naming.healthcheck.HealthCheckStatus;
import com.alibaba.nacos.naming.healthcheck.HealthCheckTask;
import com.alibaba.nacos.naming.misc.Loggers;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.Assert;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author nkorange
 * @author jifengnan 2019-04-26
 * 集群对象   该对象创建时 还会负责维护集群下所有节点的心跳检测
 */
public class Cluster extends com.alibaba.nacos.api.naming.pojo.Cluster implements Cloneable {

    private static final String CLUSTER_NAME_SYNTAX = "[0-9a-zA-Z-]+";
    /**
     * a addition for same site routing, can group multiple sites into a region, like Hangzhou, Shanghai, etc.
     */
    private String sitegroup = StringUtils.EMPTY;

    private int defCkport = 80;

    private int defIPPort = -1;

    /**
     * 心跳检测任务对象   描述该集群内部的节点按照什么方式发送心跳
     */
    @JSONField(serialize = false)
    private HealthCheckTask checkTask;

    // 该集群内部的持久化实例和短暂实例

    // 并发手段是什么 这里没有使用volatile修饰
    @JSONField(serialize = false)
    private Set<Instance> persistentInstances = new HashSet<>();

    @JSONField(serialize = false)
    private Set<Instance> ephemeralInstances = new HashSet<>();

    /**
     * 该组集群提供的服务
     */
    @JSONField(serialize = false)
    private Service service;

    @JSONField(serialize = false)
    private volatile boolean inited = false;

    private Map<String, String> metadata = new ConcurrentHashMap<>();

    public Cluster() {
    }

    /**
     * Create a cluster.
     * <p>the cluster name cannot be null, and only the arabic numerals, letters and endashes are allowed.
     *
     * @param clusterName the cluster name
     * @param service     the service to which the current cluster belongs
     * @throws IllegalArgumentException the service is null, or the cluster name is null, or the cluster name is illegal
     * @author jifengnan 2019-04-26
     * @since 1.0.1
     */
    public Cluster(String clusterName, Service service) {
        this.setName(clusterName);
        this.service = service;
        validate();
    }

    public int getDefIPPort() {
        // for compatibility with old entries
        return defIPPort == -1 ? defCkport : defIPPort;
    }

    public void setDefIPPort(int defIPPort) {
        if (defIPPort == 0) {
            throw new IllegalArgumentException("defIPPort can not be 0");
        }
        this.defIPPort = defIPPort;
    }

    public List<Instance> allIPs() {
        List<Instance> allInstances = new ArrayList<>();
        allInstances.addAll(persistentInstances);
        allInstances.addAll(ephemeralInstances);
        return allInstances;
    }

    public List<Instance> allIPs(boolean ephemeral) {
        return ephemeral ? new ArrayList<>(ephemeralInstances) : new ArrayList<>(persistentInstances);
    }

    // 初始化集群对象
    public void init() {
        if (inited) {
            return;
        }
        // 这里负责所有实例的心跳检测任务   在service 层则是负责间实例的变化通知到client  它们的职能分工不同 (心跳检测是更细粒度的 每个集群可以有不同的通信方式
        // 而service 则进行统一的通知)
        checkTask = new HealthCheckTask(this);

        // 生成心跳任务 应该是检测该集群下instance 的  在一定延时后执行
        HealthCheckReactor.scheduleCheck(checkTask);
        inited = true;
    }

    public void destroy() {
        if (checkTask != null) {
            checkTask.setCancelled(true);
        }
    }

    public HealthCheckTask getHealthCheckTask() {
        return checkTask;
    }

    public Service getService() {
        return service;
    }

    /**
     * Replace the service for the current cluster.
     * <p>  the service shouldn't be replaced. so if the service is not empty will nothing to do.
     * (the service fields can be changed, but the service A shouldn't be replaced to service B).
     * If the service of a cluster is required to replace, actually, a new cluster is required.
     *
     * @param service the new service
     */
    public void setService(Service service) {
        if (this.service != null) {
            return;
        }
        this.service = service;
    }

    /**
     * this method has been deprecated, the service name shouldn't be changed.
     *
     * @param serviceName the service name
     * @author jifengnan  2019-04-26
     * @since 1.0.1
     */
    @Deprecated
    @Override
    public void setServiceName(String serviceName) {
        super.setServiceName(serviceName);
    }

    /**
     * Get the service name of the current cluster.
     * <p>Note that the returned service name is not the name which set by {@link #setServiceName(String)},
     * but the name of the service to which the current cluster belongs.
     *
     * @return the service name of the current cluster.
     */
    @Override
    public String getServiceName() {
        if (service != null) {
            return service.getName();
        } else {
            return super.getServiceName();
        }
    }

    @Override
    public Cluster clone() throws CloneNotSupportedException {
        super.clone();
        Cluster cluster = new Cluster(this.getName(), service);
        cluster.setHealthChecker(getHealthChecker().clone());
        cluster.persistentInstances = new HashSet<>();
        cluster.checkTask = null;
        cluster.metadata = new HashMap<>(metadata);
        return cluster;
    }

    public boolean isEmpty() {
        return ephemeralInstances.isEmpty() && persistentInstances.isEmpty();
    }

    /**
     * 更新实例信息
     * @param ips
     * @param ephemeral  本次更新的实例是 瞬时实例还是持久实例
     */
    public void updateIPs(List<Instance> ips, boolean ephemeral) {

        // 代表本次需要更新的是 持久化实例 还是瞬时实例
        Set<Instance> toUpdateInstances = ephemeral ? ephemeralInstances : persistentInstances;

        // 每个实例信息还有一个标识用的key   (ip+port+clusterName)
        HashMap<String, Instance> oldIPMap = new HashMap<>(toUpdateInstances.size());

        for (Instance ip : toUpdateInstances) {
            oldIPMap.put(ip.getDatumKey(), ip);
        }

        // 照理说返回的应该是一个空list 先忽略下面的部分
        List<Instance> updatedIPs = updatedIPs(ips, oldIPMap.values());
        if (updatedIPs.size() > 0) {
            for (Instance ip : updatedIPs) {
                Instance oldIP = oldIPMap.get(ip.getDatumKey());

                // do not update the ip validation status of updated ips
                // because the checker has the most precise result
                // Only when ip is not marked, don't we update the health status of IP:
                // 使用旧快照的健康信息
                if (!ip.isMarked()) {
                    ip.setHealthy(oldIP.isHealthy());
                }

                if (ip.isHealthy() != oldIP.isHealthy()) {
                    // ip validation status updated
                    Loggers.EVT_LOG.info("{} {SYNC} IP-{} {}:{}@{}",
                        getService().getName(), (ip.isHealthy() ? "ENABLED" : "DISABLED"), ip.getIp(), ip.getPort(), getName());
                }

                if (ip.getWeight() != oldIP.getWeight()) {
                    // ip validation status updated
                    Loggers.EVT_LOG.info("{} {SYNC} {IP-UPDATED} {}->{}", getService().getName(), oldIP.toString(), ip.toString());
                }
            }
        }

        // 找到本次新增的节点
        List<Instance> newIPs = subtract(ips, oldIPMap.values());
        if (newIPs.size() > 0) {
            Loggers.EVT_LOG.info("{} {SYNC} {IP-NEW} cluster: {}, new ips size: {}, content: {}",
                getService().getName(), getName(), newIPs.size(), newIPs.toString());

            for (Instance ip : newIPs) {
                // 为该实例对象维护状态
                HealthCheckStatus.reset(ip);
            }
        }

        List<Instance> deadIPs = subtract(oldIPMap.values(), ips);

        if (deadIPs.size() > 0) {
            Loggers.EVT_LOG.info("{} {SYNC} {IP-DEAD} cluster: {}, dead ips size: {}, content: {}",
                getService().getName(), getName(), deadIPs.size(), deadIPs.toString());

            // 代表不再需要为该实例维护状态
            for (Instance ip : deadIPs) {
                HealthCheckStatus.remv(ip);
            }
        }

        toUpdateInstances = new HashSet<>(ips);

        // 将当前实例节点 重置成 ips
        if (ephemeral) {
            ephemeralInstances = toUpdateInstances;
        } else {
            persistentInstances = toUpdateInstances;
        }
    }

    /**
     * 找到 交集 也就是 更新的实例 而不包含新增加的实例
     * @param a 本次检测到新的服务实例
     * @param b 之前维护的旧服务实例
     * @return  返回的应该是个空list
     */
    public List<Instance> updatedIPs(Collection<Instance> a, Collection<Instance> b) {

        // 取交集
        List<Instance> intersects = (List<Instance>) CollectionUtils.intersection(a, b);
        // 存放交集 地址 和 实例的映射
        Map<String, Instance> stringIPAddressMap = new ConcurrentHashMap<>(intersects.size());

        // 交集的部分相当于不需要处理 所以先存入 map
        for (Instance instance : intersects) {
            stringIPAddressMap.put(instance.getIp() + ":" + instance.getPort(), instance);
        }

        // 记录某个实例出现的次数
        Map<String, Integer> intersectMap = new ConcurrentHashMap<>(a.size() + b.size());
        Map<String, Instance> instanceMap = new ConcurrentHashMap<>(a.size());
        // 存放a实例的容器
        Map<String, Instance> instanceMap1 = new ConcurrentHashMap<>(a.size());

        // 先遍历旧的容器 找到属于交集的部分 并存放到 intersectMap  中
        for (Instance instance : b) {
            if (stringIPAddressMap.containsKey(instance.getIp() + ":" + instance.getPort())) {
                intersectMap.put(instance.toString(), 1);
            }
        }


        // 这里遍历a 查看某个实例出现的次数
        for (Instance instance : a) {
            // 这里也过滤掉新增的部分 实际上留下来的只有 与b 相交的部分
            if (stringIPAddressMap.containsKey(instance.getIp() + ":" + instance.getPort())) {

                // 代表出现了2次
                if (intersectMap.containsKey(instance.toString())) {
                    intersectMap.put(instance.toString(), 2);
                } else {
                    intersectMap.put(instance.toString(), 1);
                }
            }

            instanceMap1.put(instance.toString(), instance);

        }

        // 照理说所有entry value 都应该是2
        for (Map.Entry<String, Integer> entry : intersectMap.entrySet()) {
            String key = entry.getKey();
            Integer value = entry.getValue();

            if (value == 1) {
                if (instanceMap1.containsKey(key)) {
                    instanceMap.put(key, instanceMap1.get(key));
                }
            }
        }

        return new ArrayList<>(instanceMap.values());
    }

    public List<Instance> subtract(Collection<Instance> a, Collection<Instance> b) {
        Map<String, Instance> mapa = new HashMap<>(b.size());
        for (Instance o : b) {
            mapa.put(o.getIp() + ":" + o.getPort(), o);
        }

        List<Instance> result = new ArrayList<>();

        for (Instance o : a) {
            if (!mapa.containsKey(o.getIp() + ":" + o.getPort())) {
                result.add(o);
            }
        }

        return result;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Cluster)) {
            return false;
        }

        return getName().equals(((Cluster) obj).getName());
    }

    public int getDefCkport() {
        return defCkport;
    }

    public void setDefCkport(int defCkport) {
        this.defCkport = defCkport;
    }

    public void update(Cluster cluster) {

        if (!getHealthChecker().equals(cluster.getHealthChecker())) {
            Loggers.SRV_LOG.info("[CLUSTER-UPDATE] {}:{}:, healthChecker: {} -> {}",
                getService().getName(), getName(), getHealthChecker().toString(), cluster.getHealthChecker().toString());
            setHealthChecker(cluster.getHealthChecker());
        }

        if (defCkport != cluster.getDefCkport()) {
            Loggers.SRV_LOG.info("[CLUSTER-UPDATE] {}:{}, defCkport: {} -> {}",
                getService().getName(), getName(), defCkport, cluster.getDefCkport());
            defCkport = cluster.getDefCkport();
        }

        if (defIPPort != cluster.getDefIPPort()) {
            Loggers.SRV_LOG.info("[CLUSTER-UPDATE] {}:{}, defIPPort: {} -> {}",
                getService().getName(), getName(), defIPPort, cluster.getDefIPPort());
            defIPPort = cluster.getDefIPPort();
        }

        if (!StringUtils.equals(sitegroup, cluster.getSitegroup())) {
            Loggers.SRV_LOG.info("[CLUSTER-UPDATE] {}:{}, sitegroup: {} -> {}",
                getService().getName(), getName(), sitegroup, cluster.getSitegroup());
            sitegroup = cluster.getSitegroup();
        }

        if (isUseIPPort4Check() != cluster.isUseIPPort4Check()) {
            Loggers.SRV_LOG.info("[CLUSTER-UPDATE] {}:{}, useIPPort4Check: {} -> {}",
                getService().getName(), getName(), isUseIPPort4Check(), cluster.isUseIPPort4Check());
            setUseIPPort4Check(cluster.isUseIPPort4Check());
        }

        metadata = cluster.getMetadata();
    }

    public String getSitegroup() {
        return sitegroup;
    }

    public void setSitegroup(String sitegroup) {
        this.sitegroup = sitegroup;
    }

    public boolean contains(Instance ip) {
        return persistentInstances.contains(ip) || ephemeralInstances.contains(ip);
    }

    /**
     * validate the current cluster.
     * <p>the cluster name cannot be null, and only the arabic numerals, letters and endashes are allowed.
     *
     * @throws IllegalArgumentException the service is null, or the cluster name is null, or the cluster name is illegal
     */
    public void validate() {
        Assert.notNull(getName(), "cluster name cannot be null");
        Assert.notNull(service, "service cannot be null");
        if (!getName().matches(CLUSTER_NAME_SYNTAX)) {
            throw new IllegalArgumentException("cluster name can only have these characters: 0-9a-zA-Z-, current: " + getName());
        }
    }
}
