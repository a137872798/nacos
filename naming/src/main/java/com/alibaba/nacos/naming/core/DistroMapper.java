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

import com.alibaba.nacos.core.utils.SystemUtils;
import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.cluster.servers.ServerChangeListener;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

/**
 * @author nkorange
 * 映射对象 内部包含通往其他服务的地址信息
 * 将获取地址服务的功能抽象到 ServerChangeListener  该对象本身只负责查找
 */
@Component("distroMapper")
public class DistroMapper implements ServerChangeListener {

    /**
     * 当前还活跃的 服务器节点   实际上跟 serverListManager 里面的healthyServers 一样
     */
    private List<String> healthyList = new ArrayList<>();

    public List<String> getHealthyList() {
        return healthyList;
    }

    @Autowired
    private SwitchDomain switchDomain;

    /**
     * 该对象内部包含了 这个集群下所有的server 并定期会将自身信息发送到其他所有节点 也就是 ap模式
     */
    @Autowired
    private ServerListManager serverListManager;

    /**
     * init server list
     * 将本对象作为监听器 注册到 serverListManager
     */
    @PostConstruct
    public void init() {
        serverListManager.listen(this);
    }

    /**
     * 判断该节点是否是可响应的
     * @param cluster
     * @param instance
     * @return
     */
    public boolean responsible(Cluster cluster, Instance instance) {
        return switchDomain.isHealthCheckEnabled(cluster.getServiceName())
            // 心跳检测任务未关闭
            && !cluster.getHealthCheckTask().isCancelled()
            && responsible(cluster.getServiceName())
            && cluster.contains(instance);
    }

    /**
     * 当前nacos-naming server 节点能否找到某个service的提供者
     * @param serviceName
     * @return
     */
    public boolean responsible(String serviceName) {
        // 如果不支持转发  或者当前使用单机模式  那么只能通过该节点进行响应 返回true 代表不能转发本次请求
        if (!switchDomain.isDistroEnabled() || SystemUtils.STANDALONE_MODE) {
            return true;
        }

        if (CollectionUtils.isEmpty(healthyList)) {
            // means distro config is not ready yet
            return false;
        }

        int index = healthyList.indexOf(NetUtils.localServer());
        int lastIndex = healthyList.lastIndexOf(NetUtils.localServer());
        if (lastIndex < 0 || index < 0) {
            return true;
        }

        int target = distroHash(serviceName) % healthyList.size();
        return target >= index && target <= lastIndex;
    }

    /**
     * 根据服务名找到某个健康的服务ip
     * @param serviceName
     * @return
     */
    public String mapSrv(String serviceName) {
        // 代表不允许进行重定向
        if (CollectionUtils.isEmpty(healthyList) || !switchDomain.isDistroEnabled()) {
            return NetUtils.localServer();
        }

        try {
            // 根据hash 定位到某个服务实例
            return healthyList.get(distroHash(serviceName) % healthyList.size());
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("distro mapper failed, return localhost: " + NetUtils.localServer(), e);

            return NetUtils.localServer();
        }
    }

    public int distroHash(String serviceName) {
        return Math.abs(serviceName.hashCode() % Integer.MAX_VALUE);
    }

    @Override
    public void onChangeServerList(List<Server> latestMembers) {

    }

    /**
     * 当该对象感知到集群中某些节点已经失活
     * @param latestReachableMembers   当前在线的所有节点
     */
    @Override
    public void onChangeHealthyServerList(List<Server> latestReachableMembers) {

        List<String> newHealthyList = new ArrayList<>();
        for (Server server : latestReachableMembers) {
            newHealthyList.add(server.getKey());
        }
        // 同步本对象的 healthyList
        healthyList = newHealthyList;
    }
}
