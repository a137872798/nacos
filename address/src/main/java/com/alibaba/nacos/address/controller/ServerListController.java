/*
 * Copyright (C) 2019 the original author or authors.
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
package com.alibaba.nacos.address.controller;

import com.alibaba.nacos.address.component.AddressServerGeneratorManager;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.naming.core.Cluster;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.core.ServiceManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author pbting
 * @date 2019-06-18 5:04 PM
 * @since 1.1.0
 * 用于查找节点地址
 */
@RestController
public class ServerListController {

    @Autowired
    private ServiceManager serviceManager;

    @Autowired
    private AddressServerGeneratorManager addressServerBuilderManager;

    /**
     * @param product will get Ip list of that products to be associated
     * @param cluster will get Ip list of that product cluster to be associated
     * @return
     * 这里 product 实际上是指服务   某个服务可以由多个集群来提供 而一个集群下有多个节点
     * 这里是找到能提供某个服务且在某个集群下的所有节点ip
     */
    @RequestMapping(value = "/{product}/{cluster}", method = RequestMethod.GET)
    public ResponseEntity getCluster(@PathVariable String product,
                                     @PathVariable String cluster) {

        // 加工生成 serviceName
        String productName = addressServerBuilderManager.generateProductName(product);
        String serviceName = addressServerBuilderManager.generateNacosServiceName(productName);
        // 通过命名空间和 服务名 找到 对应的服务  service 可以由一组集群提供
        Service service = serviceManager.getService(Constants.DEFAULT_NAMESPACE_ID, serviceName);
        if (service == null) {

            // 代表使用指定的 serviceName 没有找到服务信息
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("product=" + product + " not found.");
        }

        // 如果 该 service 下不包含该集群
        if (!service.getClusterMap().containsKey(cluster)) {

            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("product=" + product + ",cluster=" + cluster + " not found.");
        }

        // 将该集群下所有的ip 返回
        Cluster clusterObj = service.getClusterMap().get(cluster);
        // 集群对象分为持久化对象 和 短暂对象 这里是返回 该集群下所有持久对象
        return ResponseEntity.status(HttpStatus.OK).body(addressServerBuilderManager.generateResponseIps(clusterObj.allIPs(false)));
    }
}
