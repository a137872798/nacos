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
package com.alibaba.nacos.api;

import java.util.Properties;

import com.alibaba.nacos.api.config.ConfigFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingMaintainFactory;
import com.alibaba.nacos.api.naming.NamingMaintainService;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;

/**
 * Nacos Factory
 *
 * @author Nacos
 * nacos 对外暴露的api  通过该对象可以创建一个配置中心和 一个 注册中心
 */
public class NacosFactory {

    // ConfigService 应该只是一个 入口 内部封装了 与 nacos服务器 通信的逻辑

    /**
     * Create config service
     *
     * @param properties init param
     * @return config
     * @throws NacosException Exception
     * 通过指定的prop初始化 配置中心
     */
    public static ConfigService createConfigService(Properties properties) throws NacosException {
        return ConfigFactory.createConfigService(properties);
    }

    /**
     * Create config service
     *
     * @param serverAddr server list
     * @return config
     * @throws NacosException Exception
     * 指定 配置中心的地址
     */
    public static ConfigService createConfigService(String serverAddr) throws NacosException {
        return ConfigFactory.createConfigService(serverAddr);
    }

    /**
     * Create naming service
     *
     * @param serverAddr server list
     * @return Naming
     * @throws NacosException Exception
     * 注册中心
     */
    public static NamingService createNamingService(String serverAddr) throws NacosException {
        return NamingFactory.createNamingService(serverAddr);
    }

    /**
     * Create naming service
     *
     * @param properties init param
     * @return Naming
     * @throws NacosException Exception
     */
    public static NamingService createNamingService(Properties properties) throws NacosException {
        return NamingFactory.createNamingService(properties);
    }

    /**
     * Create maintain service
     *
     * @param serverAddr
     * @return NamingMaintainService
     * @throws NacosException Exception
     */
    public static NamingMaintainService createMaintainService(String serverAddr) throws NacosException {
        return NamingMaintainFactory.createMaintainService(serverAddr);
    }

    /**
     * Create maintain service
     *
     * @param properties
     * @return NamingMaintainService
     * @throws NacosException Exception
     */
    public static NamingMaintainService createMaintainService(Properties properties) throws NacosException {
        return NamingMaintainFactory.createMaintainService(properties);
    }

}
