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
package com.alibaba.nacos.api.config.filter;

import com.alibaba.nacos.api.exception.NacosException;

/**
 * Config Filter Interface
 *
 * @author Nacos
 * 过滤器对象
 */
public interface IConfigFilter {
    /**
     * Init
     *
     * @param filterConfig Filter Config
     *                     使用相关配置进行初始化
     */
    void init(IFilterConfig filterConfig);

    /**
     * do filter
     *
     * @param request     request
     * @param response    response
     * @param filterChain filter Chain
     * @throws NacosException exception
     * 跟 servlet.Filter 类似的api 在内部 通过调用filterChain.doFilter  传播到下一环
     */
    void doFilter(IConfigRequest request, IConfigResponse response, IConfigFilterChain filterChain)
            throws NacosException;

    /**
     * deploy
     * 部署 应该是初始化吧
     */
    void deploy();

    /**
     * Get order
     *
     * @return order number
     * 获取过滤器的顺序
     */
    int getOrder();

    /**
     * Get filterName
     *
     * @return filter name
     */
    String getFilterName();

}
