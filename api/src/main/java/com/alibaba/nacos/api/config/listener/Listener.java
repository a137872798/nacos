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
package com.alibaba.nacos.api.config.listener;

import java.util.concurrent.Executor;

/**
 * Listener for watch config
 *
 * @author Nacos
 * 配置变化监听器
 */
public interface Listener {

    /**
     * Get executor for execute this receive
     *
     * @return Executor
     * 获取对应的执行器 用于处理请求
     */
    Executor getExecutor();

    /**
     * Receive config info
     *
     * @param configInfo config info
     *                   代表接收到配置的更新
     */
    void receiveConfigInfo(final String configInfo);
}
