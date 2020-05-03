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
package com.alibaba.nacos.client.config.impl;

import com.alibaba.nacos.api.config.ConfigChangeItem;
import com.alibaba.nacos.api.config.PropertyChangeType;
import com.alibaba.nacos.api.config.listener.ConfigChangeParser;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * AbstractConfigChangeParser
 *
 * @author rushsky518
 * 配置解析器骨架类
 */
public abstract class AbstractConfigChangeParser implements ConfigChangeParser {
    /**
     * 解析的文件类型
     */
    private String configType;

    public AbstractConfigChangeParser(String configType) {
        this.configType = configType;
    }

    @Override
    public boolean isResponsibleFor(String type) {
        return this.configType.equalsIgnoreCase(type);
    }

    /**
     *
     * @param oldMap  代表旧配置
     * @param newMap  代表新配置
     * @return
     */
    protected Map<String, ConfigChangeItem> filterChangeData(Map oldMap, Map newMap) {
        Map<String, ConfigChangeItem> result = new HashMap<String, ConfigChangeItem>(16);
        for (Iterator<Map.Entry<String, Object>> entryItr = oldMap.entrySet().iterator(); entryItr.hasNext();) {
            Map.Entry<String, Object> e = entryItr.next();
            ConfigChangeItem cci = null;
            // 遍历所有旧的配置一旦发现新的配置包含相同的项
            if (newMap.containsKey(e.getKey()))  {
                // 进行比较  相同则跳过
                if (e.getValue().equals(newMap.get(e.getKey()))) {
                    continue;
                }
                // 发生变化 记录 Delete
                cci = new ConfigChangeItem(e.getKey(), e.getValue().toString(), newMap.get(e.getKey()).toString());
                cci.setType(PropertyChangeType.MODIFIED);
            } else {
                // 在新配置中没有找到 代表该配置被删除
                cci = new ConfigChangeItem(e.getKey(), e.getValue().toString(), null);
                cci.setType(PropertyChangeType.DELETED);
            }

            result.put(e.getKey(), cci);
        }

        // 记录新增的配置
        for (Iterator<Map.Entry<String, Object>> entryItr = newMap.entrySet().iterator(); entryItr.hasNext();) {
            Map.Entry<String, Object> e = entryItr.next();
            if (!oldMap.containsKey(e.getKey())) {
                ConfigChangeItem cci = new ConfigChangeItem(e.getKey(), null, e.getValue().toString());
                cci.setType(PropertyChangeType.ADDED);
                result.put(e.getKey(), cci);
            }
        }

        return result;
    }

}
