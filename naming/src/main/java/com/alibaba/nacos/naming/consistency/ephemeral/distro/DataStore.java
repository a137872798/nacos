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

import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.core.Instances;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Store of data
 *
 * @author nkorange
 * @since 1.0.0
 * 存储数据的仓库
 */
@Component
public class DataStore {

    /**
     * 仓库实体    Datum 对象就是一个键值对     Datum默认情况下是  instances  代表一组节点实例  
     */
    private Map<String, Datum> dataMap = new ConcurrentHashMap<>(1024);

    public void put(String key, Datum value) {
        dataMap.put(key, value);
    }

    public Datum remove(String key) {
        return dataMap.remove(key);
    }

    public Set<String> keys() {
        return dataMap.keySet();
    }

    public Datum get(String key) {
        return dataMap.get(key);
    }

    public boolean contains(String key) {
        return dataMap.containsKey(key);
    }

    /**
     * 根据一组key 批量拉取数据
     * @param keys
     * @return
     */
    public Map<String, Datum> batchGet(List<String> keys) {
        Map<String, Datum> map = new HashMap<>(128);
        for (String key : keys) {
            Datum datum = dataMap.get(key);
            if (datum == null) {
                continue;
            }
            map.put(key, datum);
        }
        return map;
    }

    /**
     * 遍历仓库中所有数据 找到 instances 并计算总和
     * @return
     */
    public int getInstanceCount() {
        int count = 0;
        for (Map.Entry<String, Datum> entry : dataMap.entrySet()) {
            try {
                Datum instancesDatum = entry.getValue();
                if (instancesDatum.value instanceof Instances) {
                    count += ((Instances) instancesDatum.value).getInstanceList().size();
                }
            } catch (Exception ignore) {
            }
        }
        return count;
    }

    public Map<String, Datum> getDataMap() {
        return dataMap;
    }
}
