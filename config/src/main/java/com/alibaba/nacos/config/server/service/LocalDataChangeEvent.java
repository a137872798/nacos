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
package com.alibaba.nacos.config.server.service;

import com.alibaba.nacos.config.server.utils.event.EventDispatcher.Event;

import java.util.List;

/**
 * 代表发生了本地数据与数据库数据的同步
 *
 * @author Nacos
 */
public class LocalDataChangeEvent implements Event {
    /**
     * 能够定位到某个具体的配置
     */
    final public String groupKey;
    /**
     * 该配置是否是 beta配置
     */
    final public boolean isBeta;
    /**
     * 如果是beta配置又是对哪些ip开放
     */
    final public List<String> betaIps;
    final public String tag;

    public LocalDataChangeEvent(String groupKey) {
        this.groupKey = groupKey;
        this.isBeta = false;
        this.betaIps = null;
        this.tag = null;
    }

    public LocalDataChangeEvent(String groupKey, boolean isBeta, List<String> betaIps) {
        this.groupKey = groupKey;
        this.isBeta = isBeta;
        this.betaIps = betaIps;
        this.tag = null;
    }

    public LocalDataChangeEvent(String groupKey, boolean isBeta, List<String> betaIps, String tag) {
        this.groupKey = groupKey;
        this.isBeta = isBeta;
        this.betaIps = betaIps;
        this.tag = tag;
    }
}
