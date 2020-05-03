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
package com.alibaba.nacos.api.config;

import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;

/**
 * Config Service Interface
 *
 * @author Nacos
 * 配置中心服务接口  用户通过引入 api 调用相关方法
 */
public interface ConfigService {

    /**
     * Get config
     *
     * @param dataId    dataId
     * @param group     group
     * @param timeoutMs read timeout
     * @return config value
     * @throws NacosException NacosException
     * 通过数据id  和数据组 获取对应配置
     */
    String getConfig(String dataId, String group, long timeoutMs) throws NacosException;

    /**
     * Get config and register Listener
     *
     * If you want to pull it yourself when the program starts to get the configuration for the first time,
     * and the registered Listener is used for future configuration updates, you can keep the original
     * code unchanged, just add the system parameter: enableRemoteSyncConfig = "true" ( But there is network overhead);
     * therefore we recommend that you use this interface directly
     *
     * @param dataId    dataId
     * @param group     group
     * @param timeoutMs read timeout
     * @param listener {@link Listener}
     * @return config value
     * @throws NacosException NacosException
     * 获取配置的同时 设置一个监听器 这样一旦配置发生更新 就会自动触发
     */
    String getConfigAndSignListener(String dataId, String group, long timeoutMs, Listener listener) throws NacosException;

    /**
     * Add a listener to the configuration, after the server modified the
     * configuration, the client will use the incoming listener callback.
     * Recommended asynchronous processing, the application can implement the
     * getExecutor method in the ManagerListener, provide a thread pool of
     * execution. If provided, use the main thread callback, May block other
     * configurations or be blocked by other configurations.
     *
     * @param dataId   dataId
     * @param group    group
     * @param listener listener
     * @throws NacosException NacosException
     * 为某个组的某个配置增加一个监听器
     */
    void addListener(String dataId, String group, Listener listener) throws NacosException;

    /**
     * Publish config.
     *
     * @param dataId  dataId
     * @param group   group
     * @param content content
     * @return Whether publish
     * @throws NacosException NacosException
     * 往配置中心发送某个配置
     */
    boolean publishConfig(String dataId, String group, String content) throws NacosException;

    /**
     * Remove config
     *
     * @param dataId dataId
     * @param group  group
     * @return whether remove
     * @throws NacosException NacosException
     * 从配置中心移除某个配置
     */
    boolean removeConfig(String dataId, String group) throws NacosException;

    /**
     * Remove listener
     *
     * @param dataId   dataId
     * @param group    group
     * @param listener listener
     */
    void removeListener(String dataId, String group, Listener listener);

    /**
     * Get server status
     *
     * @return whether health
     * 返回当前集群状态  false 代表当前配置中心集群不可用
     */
    String getServerStatus();

}
