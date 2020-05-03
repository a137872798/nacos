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

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.naming.CommonParams;
import com.alibaba.nacos.common.constant.HttpHeaderConsts;
import com.alibaba.nacos.common.utils.IoUtils;
import com.alibaba.nacos.core.code.ControllerMethodsCache;
import com.alibaba.nacos.core.utils.ExceptionUtil;
import com.alibaba.nacos.core.utils.OverrideParameterRequestWrapper;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.misc.HttpClient;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import org.apache.commons.codec.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;

/**
 * @author nacos
 * nacos-naming server 的controller 上很多api 都携带了 CanDistro注解 这里会识别注解并作特定处理
 */
public class DistroFilter implements Filter {

    private static final int PROXY_CONNECT_TIMEOUT = 2000;
    private static final int PROXY_READ_TIMEOUT = 2000;

    @Autowired
    private DistroMapper distroMapper;

    @Autowired
    private ControllerMethodsCache controllerMethodsCache;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    /**
     * 对请求进行拦截
     * @param servletRequest
     * @param servletResponse
     * @param filterChain
     * @throws IOException
     * @throws ServletException
     */
    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest req = (HttpServletRequest) servletRequest;
        HttpServletResponse resp = (HttpServletResponse) servletResponse;

        String urlString = req.getRequestURI();

        // 将查询参数拼接到请求路径上
        if (StringUtils.isNotBlank(req.getQueryString())) {
            urlString += "?" + req.getQueryString();
        }

        try {
            String path = new URI(req.getRequestURI()).getPath();
            String serviceName = req.getParameter(CommonParams.SERVICE_NAME);
            // For client under 0.8.0:
            // 如果没有设置 serviceName 那么获取 dom属性作为服务名  兼容旧功能
            if (StringUtils.isBlank(serviceName)) {
                serviceName = req.getParameter("dom");
            }

            // 去除空格
            if (StringUtils.isNotBlank(serviceName)) {
                serviceName = serviceName.trim();
            }
            // 根据请求路径找到匹配的 method
            Method method = controllerMethodsCache.getMethod(req.getMethod(), path);

            if (method == null) {
                throw new NoSuchMethodException(req.getMethod() + " " + path);
            }

            // 找到组信息
            String groupName = req.getParameter(CommonParams.GROUP_NAME);
            if (StringUtils.isBlank(groupName)) {
                groupName = Constants.DEFAULT_GROUP;
            }

            // use groupName@@serviceName as new service name:
            String groupedServiceName = serviceName;
            // 拼接生成 groupName@@serviceName
            if (StringUtils.isNotBlank(serviceName) && !serviceName.contains(Constants.SERVICE_INFO_SPLITER)) {
                groupedServiceName = groupName + Constants.SERVICE_INFO_SPLITER + serviceName;
            }

            // proxy request to other server if necessary:
            // 使用该注解修饰的方法代表本次请求会重定向到其他地方
            // 后半段代表本机无法正常运行 需要去其他节点拉取数据
            if (method.isAnnotationPresent(CanDistro.class) && !distroMapper.responsible(groupedServiceName)) {

                String userAgent = req.getHeader(HttpHeaderConsts.USER_AGENT_HEADER);

                // 如果 user-Agent 是 nacos - Server   代表本次请求是由其他nacos服务器发过来的
                // 无法进行二次转发
                if (StringUtils.isNotBlank(userAgent) && userAgent.contains(UtilsAndCommons.NACOS_SERVER_HEADER)) {
                    // This request is sent from peer server, should not be redirected again:
                    Loggers.SRV_LOG.error("receive invalid redirect request from peer {}", req.getRemoteAddr());
                    resp.sendError(HttpServletResponse.SC_BAD_REQUEST,
                        "receive invalid redirect request from peer " + req.getRemoteAddr());
                    return;
                }

                List<String> headerList = new ArrayList<>(16);
                // 获取本次请求携带的所有请求头信息
                Enumeration<String> headers = req.getHeaderNames();
                while (headers.hasMoreElements()) {
                    String headerName = headers.nextElement();
                    headerList.add(headerName);
                    headerList.add(req.getHeader(headerName));
                }

                // 获取请求体  针对POST 请求
                String body = IoUtils.toString(req.getInputStream(), Charsets.UTF_8.name());

                // 选择将本次请求转发到其他节点
                HttpClient.HttpResult result =
                    HttpClient.request("http://" + distroMapper.mapSrv(groupedServiceName) + urlString, headerList,
                        StringUtils.isBlank(req.getQueryString()) ? HttpClient.translateParameterMap(req.getParameterMap()) : new HashMap<>(2)
                        , body, PROXY_CONNECT_TIMEOUT, PROXY_READ_TIMEOUT, Charsets.UTF_8.name(), req.getMethod());

                try {
                    // 设置字符集 并将结果写入到 outputStream中
                    resp.setCharacterEncoding("UTF-8");
                    resp.getWriter().write(result.content);
                    resp.setStatus(result.code);
                } catch (Exception ignore) {
                    Loggers.SRV_LOG.warn("[DISTRO-FILTER] request failed: " + distroMapper.mapSrv(groupedServiceName) + urlString);
                }
                return;
            }


            // 包装该对象
            OverrideParameterRequestWrapper requestWrapper = OverrideParameterRequestWrapper.buildRequest(req);
            // 填充其他参数
            requestWrapper.addParameter(CommonParams.SERVICE_NAME, groupedServiceName);
            // 将请求往下传播
            filterChain.doFilter(requestWrapper, resp);
        } catch (AccessControlException e) {
            resp.sendError(HttpServletResponse.SC_FORBIDDEN, "access denied: " + ExceptionUtil.getAllExceptionMsg(e));
            return;
        } catch (NoSuchMethodException e) {
            resp.sendError(HttpServletResponse.SC_NOT_IMPLEMENTED,
                "no such api:" + req.getMethod() + ":" + req.getRequestURI());
            return;
        } catch (Exception e) {
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                "Server failed," + ExceptionUtil.getAllExceptionMsg(e));
            return;
        }

    }

    @Override
    public void destroy() {

    }
}
