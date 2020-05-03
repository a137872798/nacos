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

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.client.naming.utils.Chooser;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.naming.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * @author xuanyin
 * 均衡负载对象
 */
public class Balancer {

    /**
     * report status to server
     */
    public final static List<String> UNCONSISTENT_SERVICE_WITH_ADDRESS_SERVER = new CopyOnWriteArrayList<String>();

    public static class RandomByWeight {

        public static List<Instance> selectAll(ServiceInfo serviceInfo) {
            List<Instance> hosts = serviceInfo.getHosts();

            if (CollectionUtils.isEmpty(hosts)) {
                throw new IllegalStateException("no host to srv for serviceInfo: " + serviceInfo.getName());
            }

            return hosts;
        }

        // 从中随机选择一个服务实例
        public static Instance selectHost(ServiceInfo dom) {

            List<Instance> hosts = selectAll(dom);

            if (CollectionUtils.isEmpty(hosts)) {
                throw new IllegalStateException("no host to srv for service: " + dom.getName());
            }

            return getHostByRandomWeight(hosts);
        }
    }

    /**
     * Return one host from the host list by random-weight.
     *
     * @param hosts The list of the host.
     * @return The random-weight result of the host
     * 从一组服务实例中选择一个
     */
    protected static Instance getHostByRandomWeight(List<Instance> hosts) {
        NAMING_LOGGER.debug("entry randomWithWeight");
        if (hosts == null || hosts.size() == 0) {
            NAMING_LOGGER.debug("hosts == null || hosts.size() == 0");
            return null;
        }

        Chooser<String, Instance> vipChooser = new Chooser<String, Instance>("www.taobao.com");

        NAMING_LOGGER.debug("new Chooser");

        List<Pair<Instance>> hostsWithWeight = new ArrayList<Pair<Instance>>();
        for (Instance host : hosts) {
            // 首先确保实例是健康的
            if (host.isHealthy()) {
                hostsWithWeight.add(new Pair<Instance>(host, host.getWeight()));
            }
        }
        NAMING_LOGGER.debug("for (Host host : hosts)");
        // 洗牌
        vipChooser.refresh(hostsWithWeight);
        NAMING_LOGGER.debug("vipChooser.refresh");
        // 随机选一个 (跟权重挂钩)
        return vipChooser.randomWithWeight();
    }
}
