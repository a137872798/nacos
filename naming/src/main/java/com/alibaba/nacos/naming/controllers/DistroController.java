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
package com.alibaba.nacos.naming.controllers;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.DataStore;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.DistroConsistencyServiceImpl;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.ServiceManager;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Restful methods for Partition protocol.
 *
 *
 * @author nkorange
 * @since 1.0.0
 */
@RestController
@RequestMapping(UtilsAndCommons.NACOS_NAMING_CONTEXT + "/distro")
public class DistroController {

    @Autowired
    private Serializer serializer;

    /**
     * 代表该controller 对应的注册中心实现方式不需要做数据持久化
     */
    @Autowired
    private DistroConsistencyServiceImpl consistencyService;

    /**
     * service 下实例信息存储在该对象中
     */
    @Autowired
    private DataStore dataStore;

    @Autowired
    private ServiceManager serviceManager;

    @Autowired
    private SwitchDomain switchDomain;

    /**
     * 当往集群中某个节点注册新实例时  需要将信息同步到其他节点 （基于AP的实现）
     * @param dataMap
     * @return
     * @throws Exception
     */
    @PutMapping("/datum")
    public ResponseEntity onSyncDatum(@RequestBody Map<String, Datum<Instances>> dataMap) throws Exception {

        if (dataMap.isEmpty()) {
            Loggers.DISTRO.error("[onSync] receive empty entity!");
            throw new NacosException(NacosException.INVALID_PARAM, "receive empty entity!");
        }

        for (Map.Entry<String, Datum<Instances>> entry : dataMap.entrySet()) {
            if (KeyBuilder.matchEphemeralInstanceListKey(entry.getKey())) {
                String namespaceId = KeyBuilder.getNamespace(entry.getKey());
                String serviceName = KeyBuilder.getServiceName(entry.getKey());
                // 如果服务实例还没创建 先创建服务实例
                if (!serviceManager.containService(namespaceId, serviceName)
                    && switchDomain.isDefaultInstanceEphemeral()) {
                    serviceManager.createEmptyService(namespaceId, serviceName, true);
                }
                // 更新实例
                consistencyService.onPut(entry.getKey(), entry.getValue().value);
            }
        }
        return ResponseEntity.ok("ok");
    }

    /**
     * 接收集群内 其他节点同步数据的请求
     * @param source  代表是哪个节点发起的请求
     * @param dataMap
     * @return
     */
    @PutMapping("/checksum")
    public ResponseEntity syncChecksum(@RequestParam String source, @RequestBody Map<String, String> dataMap) {

        consistencyService.onReceiveChecksums(dataMap, source);
        return ResponseEntity.ok("ok");
    }

    /**
     * 根据 key 从DataStore 中找到对应数据
     * @param body
     * @return
     * @throws Exception
     */
    @GetMapping("/datum")
    public ResponseEntity get(@RequestBody JSONObject body) throws Exception {

        String keys = body.getString("keys");
        String keySplitter = ",";
        Map<String, Datum> datumMap = new HashMap<>(64);
        for (String key : keys.split(keySplitter)) {
            Datum datum = consistencyService.get(key);
            if (datum == null) {
                continue;
            }
            datumMap.put(key, datum);
        }

        String content = new String(serializer.serialize(datumMap), StandardCharsets.UTF_8);
        return ResponseEntity.ok(content);
    }

    @GetMapping("/datums")
    public ResponseEntity getAllDatums() {
        String content = new String(serializer.serialize(dataStore.getDataMap()), StandardCharsets.UTF_8);
        return ResponseEntity.ok(content);
    }
}
