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
package com.alibaba.nacos.client.naming.utils;

import java.util.List;

/**
 * @author nkorange
 * 类似迭代器
 */
public interface Poller<T> {
    /**
     * Get next element selected by poller
     *
     * @return next element
     */
    T next();

    /**
     * Update items stored in poller
     *
     * @param items new item list
     * @return new poller instance
     * 更新内部存储的数据
     */
    Poller<T> refresh(List<T> items);
}
