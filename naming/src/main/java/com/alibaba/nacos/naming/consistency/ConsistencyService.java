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
package com.alibaba.nacos.naming.consistency;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.naming.pojo.Record;

/**
 * Consistence service for all implementations to derive.
 * <p>
 * We announce this consistency service to decouple the specific consistency implementation with business logic.
 * User should not be aware of what consistency protocol is being used.
 * <p>
 * In this way, we also provide space for user to extend the underlying consistency protocols, as long as they
 * obey our consistency baseline.
 *
 * @author nkorange
 * @since 1.0.0
 * 一致性服务  (比如raft)
 */
public interface ConsistencyService {

    /**
     * Put a data related to a key to Nacos cluster
     *
     * @param key   key of data, this key should be globally unique
     * @param value value of data
     * @throws NacosException
     * 将一组键值对保存到 nacos   Record 代表数据体 本身可以获取校验和属性
     */
    void put(String key, Record value) throws NacosException;

    /**
     * Remove a data from Nacos cluster
     *
     * @param key key of data
     * @throws NacosException
     */
    void remove(String key) throws NacosException;

    /**
     * Get a data from Nacos cluster
     *
     * @param key key of data
     * @return data related to the key
     * @throws NacosException
     * 从集群中获取某个属性
     */
    Datum get(String key) throws NacosException;

    /**
     * Listen for changes of a data
     *
     * @param key      key of data
     * @param listener callback of data change
     * @throws NacosException
     * 为某个 key 设置一个监听器 用于监听值的变化
     */
    void listen(String key, RecordListener listener) throws NacosException;

    /**
     * Cancel listening of a data
     *
     * @param key      key of data
     * @param listener callback of data change
     * @throws NacosException
     * 注销监听
     */
    void unlisten(String key, RecordListener listener) throws NacosException;

    /**
     * Tell the status of this consistency service
     *
     * @return true if available
     * 当前该服务是否可用 比如 raft 可能正处在重新选举
     */
    boolean isAvailable();
}
