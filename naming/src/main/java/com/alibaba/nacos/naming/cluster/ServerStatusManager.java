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
package com.alibaba.nacos.naming.cluster;

import com.alibaba.nacos.naming.consistency.ConsistencyService;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

/**
 * Detect and control the working status of local server
 *
 * @author nkorange
 * @since 1.0.0
 * 服务器状态管理
 */
@Service
public class ServerStatusManager {

    /**
     * 一致性 服务
     */
    @Resource(name = "consistencyDelegate")
    private ConsistencyService consistencyService;

    @Autowired
    private SwitchDomain switchDomain;

    /**
     * 默认情况 服务器处于启动状态
     */
    private ServerStatus serverStatus = ServerStatus.STARTING;

    /**
     * 在全局定时器中注册状态更新对象
     */
    @PostConstruct
    public void init() {
        GlobalExecutor.registerServerStatusUpdater(new ServerStatusUpdater());
    }

    /**
     * 对应ServerStatusUpdater() 的定时任务
     */
    private void refreshServerStatus() {

        // 如果 switchDomain 设置了 overriddenServerStatus  代表此时服务器状态已经发生了改变 同时覆盖本对象的值
        if (StringUtils.isNotBlank(switchDomain.getOverriddenServerStatus())) {
            serverStatus = ServerStatus.valueOf(switchDomain.getOverriddenServerStatus());
            return;
        }

        // 如果 服务器状态没有被覆盖  查看一致性服务是否可用 如果本节点无法在集群内实现一致性了 可以认为它就是下线了 比如 基于raft时 发生脑裂 处于少数派
        if (consistencyService.isAvailable()) {
            serverStatus = ServerStatus.UP;
        } else {
            serverStatus = ServerStatus.DOWN;
        }
    }

    public ServerStatus getServerStatus() {
        return serverStatus;
    }

    public class ServerStatusUpdater implements Runnable {

        @Override
        public void run() {
            refreshServerStatus();
        }
    }
}
