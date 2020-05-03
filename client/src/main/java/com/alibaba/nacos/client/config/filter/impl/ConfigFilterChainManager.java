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
package com.alibaba.nacos.client.config.filter.impl;

import com.alibaba.nacos.api.config.filter.IConfigFilter;
import com.alibaba.nacos.api.config.filter.IConfigFilterChain;
import com.alibaba.nacos.api.config.filter.IConfigRequest;
import com.alibaba.nacos.api.config.filter.IConfigResponse;
import com.alibaba.nacos.api.exception.NacosException;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Config Filter Chain Management
 *
 * @author Nacos
 * 该对象管理所有的 filter
 */
public class ConfigFilterChainManager implements IConfigFilterChain {

    /**
     * 内部一组过滤器对象
     */
    private List<IConfigFilter> filters = Lists.newArrayList();

    /**
     * 添加过滤器
     * @param filter
     * @return
     */
    public synchronized ConfigFilterChainManager addFilter(IConfigFilter filter) {
        // 根据order大小顺序插入
        int i = 0;
        while (i < this.filters.size()) {
            IConfigFilter currentValue = this.filters.get(i);
            // 确保不重复添加
            if (currentValue.getFilterName().equals(filter.getFilterName())) {
                break;
            }
            // 未到达末尾前 递增偏移量
            if (filter.getOrder() >= currentValue.getOrder() && i < this.filters.size()) {
                i++;
            } else {
                // 在指定的位置插入新元素
                this.filters.add(i, filter);
                break;
            }
        }

        // 代表当前list 容量不够 选择添加一个新元素
        if (i == this.filters.size()) {
            this.filters.add(i, filter);
        }
        return this;
    }

    /**
     * 传播到下一环进行处理
     * @param request  request
     * @param response response
     * @throws NacosException
     */
    @Override
    public void doFilter(IConfigRequest request, IConfigResponse response) throws NacosException {
        new VirtualFilterChain(this.filters).doFilter(request, response);
    }

    /**
     * 实际上 当 manager 处理请求时 会转发到内部的对象
     */
    private static class VirtualFilterChain implements IConfigFilterChain {

        /**
         * 内部的一组 过滤器
         */
        private final List<? extends IConfigFilter> additionalFilters;

        private int currentPosition = 0;

        public VirtualFilterChain(List<? extends IConfigFilter> additionalFilters) {
            this.additionalFilters = additionalFilters;
        }

        @Override
        public void doFilter(final IConfigRequest request, final IConfigResponse response) throws NacosException {
            if (this.currentPosition != this.additionalFilters.size()) {
                this.currentPosition++;
                IConfigFilter nextFilter = this.additionalFilters.get(this.currentPosition - 1);
                // 传入 this 相当于 不让外部直接操作 manager 对象 类似于 门面模式
                nextFilter.doFilter(request, response, this);
            }
        }
    }

}
