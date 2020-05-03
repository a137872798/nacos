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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.nacos.naming.boot.SpringContext;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.healthcheck.ClientBeatCheckTask;
import com.alibaba.nacos.naming.healthcheck.ClientBeatProcessor;
import com.alibaba.nacos.naming.healthcheck.HealthCheckReactor;
import com.alibaba.nacos.naming.healthcheck.RsInfo;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.pojo.Record;
import com.alibaba.nacos.naming.push.PushService;
import com.alibaba.nacos.naming.selector.NoneSelector;
import com.alibaba.nacos.naming.selector.Selector;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.util.*;

/**
 * Service of Nacos server side
 * <p>
 * We introduce a 'service --> cluster --> instance' model, in which service stores a list of clusters,
 * which contain a list of instances.
 * <p>
 * This class inherits from Service in API module and stores some fields that do not have to expose to client.
 *
 * @author nkorange
 * 一个service 可以对应多个cluster  一个cluster 可以对应多个instance
 * 每个 service 又是有关instances的监听器 应该是通过心跳机制检测到某个服务实例下线 进而触发监听器 更新service内部的数据 ServiceManager 作为 Service层的监听器 当某个service
 * 无效时 将service 从nacos Server中移除
 */
public class Service extends com.alibaba.nacos.api.naming.pojo.Service implements Record, RecordListener<Instances> {

    private static final String SERVICE_NAME_SYNTAX = "[0-9a-zA-Z@\\.:_-]+";

    /**
     * 推测是负责与 service 下所有 cluster 下所有 instance 维持心跳
     */
    @JSONField(serialize = false)
    private ClientBeatCheckTask clientBeatCheckTask = new ClientBeatCheckTask(this);

    /**
     * Identify the information used to determine how many isEmpty judgments the service has experienced
     * serviceManager 下有一个定时清理无效service的任务 每次检测到对应service下没有提供者 会为该值+1 直到超过一个限定值时 才真正做清理工作
     */
    private int finalizeCount = 0;

    private String token;
    private List<String> owners = new ArrayList<>();
    private Boolean resetWeight = false;
    private Boolean enabled = true;
    private Selector selector = new NoneSelector();
    private String namespaceId;

    /**
     * IP will be deleted if it has not send beat for some time, default timeout is 30 seconds.
     */
    private long ipDeleteTimeout = 30 * 1000;

    private volatile long lastModifiedMillis = 0L;

    private volatile String checksum;

    /**
     * TODO set customized push expire time:
     */
    private long pushCacheMillis = 0L;

    /**
     * 一个服务本身可以由多个 集群来提供
     */
    private Map<String, Cluster> clusterMap = new HashMap<>();

    public Service() {
    }

    public Service(String name) {
        super(name);
    }

    @JSONField(serialize = false)
    public PushService getPushService() {
        return SpringContext.getAppContext().getBean(PushService.class);
    }

    public long getIpDeleteTimeout() {
        return ipDeleteTimeout;
    }

    public void setIpDeleteTimeout(long ipDeleteTimeout) {
        this.ipDeleteTimeout = ipDeleteTimeout;
    }

    /**
     * 将集群下 rsInfo内包含的实例信息设置成 isHealth = true
     * @param rsInfo
     */
    public void processClientBeat(final RsInfo rsInfo) {
        ClientBeatProcessor clientBeatProcessor = new ClientBeatProcessor();
        clientBeatProcessor.setService(this);
        clientBeatProcessor.setRsInfo(rsInfo);
        HealthCheckReactor.scheduleNow(clientBeatProcessor);
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    public long getLastModifiedMillis() {
        return lastModifiedMillis;
    }

    public void setLastModifiedMillis(long lastModifiedMillis) {
        this.lastModifiedMillis = lastModifiedMillis;
    }

    public Boolean getResetWeight() {
        return resetWeight;
    }

    public void setResetWeight(Boolean resetWeight) {
        this.resetWeight = resetWeight;
    }

    public Selector getSelector() {
        return selector;
    }

    public void setSelector(Selector selector) {
        this.selector = selector;
    }

    @Override
    public boolean interests(String key) {
        return KeyBuilder.matchInstanceListKey(key, namespaceId, getName());
    }

    @Override
    public boolean matchUnlistenKey(String key) {
        return KeyBuilder.matchInstanceListKey(key, namespaceId, getName());
    }

    /**
     * service 作为监听器负责监听下面实例的变化
     * @param key   target key
     * @param value data of the key   能够提供该服务的实例信息 注意 service 下先是一个 cluster 在此之下 才是instance
     * @throws Exception
     */
    @Override
    public void onChange(String key, Instances value) throws Exception {

        Loggers.SRV_LOG.info("[NACOS-RAFT] datum is changed, key: {}, value: {}", key, value);

        // 修正异常的instance 信息
        for (Instance instance : value.getInstanceList()) {

            if (instance == null) {
                // Reject this abnormal instance list:
                throw new RuntimeException("got null instance " + key);
            }

            if (instance.getWeight() > 10000.0D) {
                instance.setWeight(10000.0D);
            }

            if (instance.getWeight() < 0.01D && instance.getWeight() > 0.0D) {
                instance.setWeight(0.01D);
            }
        }

        // 更新所有实例信息  内部还会触发 pushService.serviceChanged
        updateIPs(value.getInstanceList(), KeyBuilder.matchEphemeralInstanceListKey(key));

        recalculateChecksum();
    }

    @Override
    public void onDelete(String key) throws Exception {
        // ignore
    }

    /**
     * 找到所有 健康实例
     * @return
     */
    public int healthyInstanceCount() {

        int healthyCount = 0;
        for (Instance instance : allIPs()) {
            if (instance.isHealthy()) {
                healthyCount++;
            }
        }
        return healthyCount;
    }

    public boolean triggerFlag() {
        return (healthyInstanceCount() * 1.0 / allIPs().size()) <= getProtectThreshold();
    }

    /**
     *
     * @param instances  这里是所有实例 还要按照集群来划分
     * @param ephemeral  代表本次更新的实例是  瞬时实例还是持久实例
     */
    public void updateIPs(Collection<Instance> instances, boolean ephemeral) {
        // 清空之前按集群划分的 实例组   这里使用写时拷贝 所以没有加锁操作
        Map<String, List<Instance>> ipMap = new HashMap<>(clusterMap.size());
        for (String clusterName : clusterMap.keySet()) {
            ipMap.put(clusterName, new ArrayList<>());
        }

        for (Instance instance : instances) {
            try {
                if (instance == null) {
                    Loggers.SRV_LOG.error("[NACOS-DOM] received malformed ip: null");
                    continue;
                }

                // 当某个服务实例没有标记所在集群时 使用默认集群
                if (StringUtils.isEmpty(instance.getClusterName())) {
                    instance.setClusterName(UtilsAndCommons.DEFAULT_CLUSTER_NAME);
                }

                // 发现新集群的实例时 初始化并添加到 集群容器中
                if (!clusterMap.containsKey(instance.getClusterName())) {
                    Loggers.SRV_LOG.warn("cluster: {} not found, ip: {}, will create new cluster with default configuration.",
                        instance.getClusterName(), instance.toJSON());
                    Cluster cluster = new Cluster(instance.getClusterName(), this);
                    // 按照集群为单位进行初始化  就是启动心跳对象并维护下面所有实例的心跳状态
                    cluster.init();
                    getClusterMap().put(instance.getClusterName(), cluster);
                }

                List<Instance> clusterIPs = ipMap.get(instance.getClusterName());
                if (clusterIPs == null) {
                    clusterIPs = new LinkedList<>();
                    ipMap.put(instance.getClusterName(), clusterIPs);
                }

                // 添加实例
                clusterIPs.add(instance);
            } catch (Exception e) {
                Loggers.SRV_LOG.error("[NACOS-DOM] failed to process ip: " + instance, e);
            }
        }

        // ipMap 最后会将 本次所有实例分组   传入的instances 代表全量更新
        for (Map.Entry<String, List<Instance>> entry : ipMap.entrySet()) {
            //make every ip mine
            List<Instance> entryIPs = entry.getValue();
            clusterMap.get(entry.getKey()).updateIPs(entryIPs, ephemeral);
        }

        setLastModifiedMillis(System.currentTimeMillis());
        // 代表某个服务下实例发生了变化 需要通知到client
        getPushService().serviceChanged(this);
        StringBuilder stringBuilder = new StringBuilder();

        for (Instance instance : allIPs()) {
            stringBuilder.append(instance.toIPAddr()).append("_").append(instance.isHealthy()).append(",");
        }

        Loggers.EVT_LOG.info("[IP-UPDATED] namespace: {}, service: {}, ips: {}",
            getNamespaceId(), getName(), stringBuilder.toString());

    }

    /**
     * 当某个服务实例被创建时 需要进行初始化工作
     */
    public void init() {

        // 执行心跳检测任务   应该就是往服务下 所有集群的所有 instance 发送心跳包
        HealthCheckReactor.scheduleCheck(clientBeatCheckTask);

        // 挨个初始化cluster
        for (Map.Entry<String, Cluster> entry : clusterMap.entrySet()) {
            entry.getValue().setService(this);
            entry.getValue().init();
        }
    }

    public void destroy() throws Exception {
        for (Map.Entry<String, Cluster> entry : clusterMap.entrySet()) {
            entry.getValue().destroy();
        }
        HealthCheckReactor.cancelCheck(clientBeatCheckTask);
    }

    public boolean isEmpty() {
        for (Map.Entry<String, Cluster> entry : clusterMap.entrySet()) {
            final Cluster cluster = entry.getValue();
            if (!cluster.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    public List<Instance> allIPs() {
        List<Instance> allIPs = new ArrayList<>();
        for (Map.Entry<String, Cluster> entry : clusterMap.entrySet()) {
            allIPs.addAll(entry.getValue().allIPs());
        }

        return allIPs;
    }

    public List<Instance> allIPs(boolean ephemeral) {
        List<Instance> allIPs = new ArrayList<>();
        for (Map.Entry<String, Cluster> entry : clusterMap.entrySet()) {
            allIPs.addAll(entry.getValue().allIPs(ephemeral));
        }

        return allIPs;
    }

    public List<Instance> allIPs(List<String> clusters) {
        List<Instance> allIPs = new ArrayList<>();
        for (String cluster : clusters) {
            Cluster clusterObj = clusterMap.get(cluster);
            if (clusterObj == null) {
                continue;
            }

            allIPs.addAll(clusterObj.allIPs());
        }

        return allIPs;
    }

    public List<Instance> srvIPs(List<String> clusters) {
        if (CollectionUtils.isEmpty(clusters)) {
            clusters = new ArrayList<>();
            clusters.addAll(clusterMap.keySet());
        }
        return allIPs(clusters);
    }

    public String toJSON() {
        return JSON.toJSONString(this);
    }

    @JSONField(serialize = false)
    public String getServiceString() {
        Map<Object, Object> serviceObject = new HashMap<Object, Object>(10);
        Service service = this;

        serviceObject.put("name", service.getName());

        List<Instance> ips = service.allIPs();
        int invalidIPCount = 0;
        int ipCount = 0;
        for (Instance ip : ips) {
            if (!ip.isHealthy()) {
                invalidIPCount++;
            }

            ipCount++;
        }

        serviceObject.put("ipCount", ipCount);
        serviceObject.put("invalidIPCount", invalidIPCount);

        serviceObject.put("owners", service.getOwners());
        serviceObject.put("token", service.getToken());

        serviceObject.put("protectThreshold", service.getProtectThreshold());

        List<Object> clustersList = new ArrayList<Object>();

        for (Map.Entry<String, Cluster> entry : service.getClusterMap().entrySet()) {
            Cluster cluster = entry.getValue();

            Map<Object, Object> clusters = new HashMap<Object, Object>(10);
            clusters.put("name", cluster.getName());
            clusters.put("healthChecker", cluster.getHealthChecker());
            clusters.put("defCkport", cluster.getDefCkport());
            clusters.put("defIPPort", cluster.getDefIPPort());
            clusters.put("useIPPort4Check", cluster.isUseIPPort4Check());
            clusters.put("sitegroup", cluster.getSitegroup());

            clustersList.add(clusters);
        }

        serviceObject.put("clusters", clustersList);

        return JSON.toJSONString(serviceObject);
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public List<String> getOwners() {
        return owners;
    }

    public void setOwners(List<String> owners) {
        this.owners = owners;
    }

    public Map<String, Cluster> getClusterMap() {
        return clusterMap;
    }

    public void setClusterMap(Map<String, Cluster> clusterMap) {
        this.clusterMap = clusterMap;
    }

    public String getNamespaceId() {
        return namespaceId;
    }

    public void setNamespaceId(String namespaceId) {
        this.namespaceId = namespaceId;
    }

    /**
     * 更新当前服务实例信息
     * @param vDom
     */
    public void update(Service vDom) {

        if (!StringUtils.equals(token, vDom.getToken())) {
            Loggers.SRV_LOG.info("[SERVICE-UPDATE] service: {}, token: {} -> {}", getName(), token, vDom.getToken());
            token = vDom.getToken();
        }

        if (!ListUtils.isEqualList(owners, vDom.getOwners())) {
            Loggers.SRV_LOG.info("[SERVICE-UPDATE] service: {}, owners: {} -> {}", getName(), owners, vDom.getOwners());
            owners = vDom.getOwners();
        }

        if (getProtectThreshold() != vDom.getProtectThreshold()) {
            Loggers.SRV_LOG.info("[SERVICE-UPDATE] service: {}, protectThreshold: {} -> {}", getName(), getProtectThreshold(), vDom.getProtectThreshold());
            setProtectThreshold(vDom.getProtectThreshold());
        }

        if (resetWeight != vDom.getResetWeight().booleanValue()) {
            Loggers.SRV_LOG.info("[SERVICE-UPDATE] service: {}, resetWeight: {} -> {}", getName(), resetWeight, vDom.getResetWeight());
            resetWeight = vDom.getResetWeight();
        }

        if (enabled != vDom.getEnabled().booleanValue()) {
            Loggers.SRV_LOG.info("[SERVICE-UPDATE] service: {}, enabled: {} -> {}", getName(), enabled, vDom.getEnabled());
            enabled = vDom.getEnabled();
        }

        selector = vDom.getSelector();

        setMetadata(vDom.getMetadata());

        updateOrAddCluster(vDom.getClusterMap().values());
        remvDeadClusters(this, vDom);

        Loggers.SRV_LOG.info("cluster size, new: {}, old: {}", getClusterMap().size(), vDom.getClusterMap().size());

        recalculateChecksum();
    }

    @Override
    public String getChecksum() {
        if (StringUtils.isEmpty(checksum)) {
            recalculateChecksum();
        }

        return checksum;
    }

    /**
     * 计算校验和
     */
    public synchronized void recalculateChecksum() {
        List<Instance> ips = allIPs();

        StringBuilder ipsString = new StringBuilder();
        ipsString.append(getServiceString());

        if (Loggers.SRV_LOG.isDebugEnabled()) {
            Loggers.SRV_LOG.debug("service to json: " + getServiceString());
        }

        if (CollectionUtils.isNotEmpty(ips)) {
            Collections.sort(ips);
        }

        for (Instance ip : ips) {
            String string = ip.getIp() + ":" + ip.getPort() + "_" + ip.getWeight() + "_"
                + ip.isHealthy() + "_" + ip.getClusterName();
            ipsString.append(string);
            ipsString.append(",");
        }

        try {
            String result;
            try {
                MessageDigest md5 = MessageDigest.getInstance("MD5");
                result = new BigInteger(1, md5.digest((ipsString.toString()).getBytes(Charset.forName("UTF-8")))).toString(16);
            } catch (Exception e) {
                Loggers.SRV_LOG.error("[NACOS-DOM] error while calculating checksum(md5)", e);
                result = RandomStringUtils.randomAscii(32);
            }

            checksum = result;
        } catch (Exception e) {
            Loggers.SRV_LOG.error("[NACOS-DOM] error while calculating checksum(md5)", e);
            checksum = RandomStringUtils.randomAscii(32);
        }
    }

    private void updateOrAddCluster(Collection<Cluster> clusters) {
        for (Cluster cluster : clusters) {
            Cluster oldCluster = clusterMap.get(cluster.getName());
            if (oldCluster != null) {
                oldCluster.setService(this);
                oldCluster.update(cluster);
            } else {
                cluster.init();
                cluster.setService(this);
                clusterMap.put(cluster.getName(), cluster);
            }
        }
    }

    private void remvDeadClusters(Service oldDom, Service newDom) {
        Collection<Cluster> oldClusters = oldDom.getClusterMap().values();
        Collection<Cluster> newClusters = newDom.getClusterMap().values();
        List<Cluster> deadClusters = (List<Cluster>) CollectionUtils.subtract(oldClusters, newClusters);
        for (Cluster cluster : deadClusters) {
            oldDom.getClusterMap().remove(cluster.getName());

            cluster.destroy();
        }
    }

    public int getFinalizeCount() {
        return finalizeCount;
    }

    public void setFinalizeCount(int finalizeCount) {
        this.finalizeCount = finalizeCount;
    }

    public void addCluster(Cluster cluster) {
        clusterMap.put(cluster.getName(), cluster);
    }

    public void validate() {
        if (!getName().matches(SERVICE_NAME_SYNTAX)) {
            throw new IllegalArgumentException("dom name can only have these characters: 0-9a-zA-Z-._:, current: " + getName());
        }
        for (Cluster cluster : clusterMap.values()) {
            cluster.validate();
        }
    }
}
