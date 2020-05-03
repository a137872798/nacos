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

import java.util.concurrent.*;

/**
 * Time Service
 *
 * @author Nacos
 * 可以往该对象添加定时任务   为什么要使用这一条线程呢 那么任务会很杂乱 很多情况甚至创建多个线程池 就是为了解耦
 */
public class TimerService {

    static public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay,
                                                            long delay, TimeUnit unit) {
        return scheduledExecutor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    static ScheduledExecutorService scheduledExecutor = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("com.alibaba.nacos.client.Timer");
                t.setDaemon(true);
                return t;
            }
        });

}
