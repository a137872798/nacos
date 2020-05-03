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
package com.alibaba.nacos.naming.web;

import com.alibaba.nacos.common.utils.HttpMethod;
import com.alibaba.nacos.core.utils.Constants;
import com.alibaba.nacos.core.utils.WebUtils;
import com.alibaba.nacos.naming.cluster.ServerStatus;
import com.alibaba.nacos.naming.cluster.ServerStatusManager;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

/**
 * Filter incoming traffic to refuse or revise unexpected requests
 *
 * @author nkorange
 * @since 1.0.0
 * 限流过滤器
 */
public class TrafficReviseFilter implements Filter {

    /**
     * 本服务器状态的管理器   会定期检测当前服务器状态
     */
    @Autowired
    private ServerStatusManager serverStatusManager;

    @Autowired
    private SwitchDomain switchDomain;

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {

        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse resp = (HttpServletResponse) response;

        // request limit if exist:
        String urlString = req.getRequestURI() + "?" + req.getQueryString();
        // 找到被限流的url 容器
        Map<String, Integer> limitedUrlMap = switchDomain.getLimitedUrlMap();

        if (limitedUrlMap != null && limitedUrlMap.size() > 0) {
            for (Map.Entry<String, Integer> entry : limitedUrlMap.entrySet()) {
                String limitedUrl = entry.getKey();
                if (StringUtils.startsWith(urlString, limitedUrl)) {
                    // 直接设置状态码
                    resp.setStatus(entry.getValue());
                    return;
                }
            }
        }

        // if server is UP:
        // 本次请求未被限流  先判断本机是否还在线 (比如本节点已经无法被集群内其他节点探测到 那么认为已经下线了)
        if (serverStatusManager.getServerStatus() == ServerStatus.UP) {
            filterChain.doFilter(req, resp);
            return;
        }

        // requests from peer server should be let pass:
        // 获取本次发送请求的 agent
        String agent = WebUtils.getUserAgent(req);

        // 代表是 nacos集群内部的请求 还是要正常处理
        if (StringUtils.startsWith(agent, Constants.NACOS_SERVER_HEADER)) {
            filterChain.doFilter(req, resp);
            return;
        }

        // write operation should be let pass in WRITE_ONLY status:
        // 如果是只允许写入 且非get 请求 也允许处理
        if (serverStatusManager.getServerStatus() == ServerStatus.WRITE_ONLY && !HttpMethod.GET.equals(req.getMethod())) {
            filterChain.doFilter(req, resp);
            return;
        }

        // read operation should be let pass in READ_ONLY status:
        // 只读请求 + get请求方式也允许
        if (serverStatusManager.getServerStatus() == ServerStatus.READ_ONLY && HttpMethod.GET.equals(req.getMethod())) {
            filterChain.doFilter(req, resp);
            return;
        }

        // 返回失败信息   503(当前服务器不可用)
        resp.getWriter().write("server is " + serverStatusManager.getServerStatus().name() + " now, please try again later!");
        resp.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    }
}
