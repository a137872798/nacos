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
package com.alibaba.nacos.client.config;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.client.config.filter.impl.ConfigFilterChainManager;
import com.alibaba.nacos.client.config.filter.impl.ConfigRequest;
import com.alibaba.nacos.client.config.filter.impl.ConfigResponse;
import com.alibaba.nacos.client.config.http.HttpAgent;
import com.alibaba.nacos.client.config.http.MetricsHttpAgent;
import com.alibaba.nacos.client.config.http.ServerHttpAgent;
import com.alibaba.nacos.client.config.impl.ClientWorker;
import com.alibaba.nacos.client.config.impl.HttpSimpleClient.HttpResult;
import com.alibaba.nacos.client.config.impl.LocalConfigInfoProcessor;
import com.alibaba.nacos.client.config.utils.ContentUtils;
import com.alibaba.nacos.client.config.utils.ParamUtils;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.client.utils.ParamUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Config Impl
 *
 * @author Nacos
 * nacos 对应的配置服务实现类
 */
@SuppressWarnings("PMD.ServiceOrDaoClassShouldEndWithImplRule")
public class NacosConfigService implements ConfigService {

    private static final Logger LOGGER = LogUtils.logger(NacosConfigService.class);

    private static final long POST_TIMEOUT = 3000L;

    private static final String EMPTY = "";

    /**
     * http agent
     * 该对象以 http 请求的方式 获取数据  (也就是 nacos server 会开放一个 http的入口)
     */
    private HttpAgent agent;
    /**
     * longpolling
     * 以长连接方式 与 nacos server 通信
     */
    private ClientWorker worker;
    /**
     * 本次服务 尝试从哪个 namespace 获取配置
     */
    private String namespace;
    /**
     * 编码方式 比如 UTF-8
     */
    private String encode;
    /**
     * 该对象负责管理 交互相关的过滤器
     */
    private ConfigFilterChainManager configFilterChainManager = new ConfigFilterChainManager();

    /**
     * 使用一个 prop 对象进行初始化
     * @param properties
     * @throws NacosException
     */
    public NacosConfigService(Properties properties) throws NacosException {
        // 编码方式
        String encodeTmp = properties.getProperty(PropertyKeyConst.ENCODE);
        if (StringUtils.isBlank(encodeTmp)) {
            encode = Constants.ENCODE;
        } else {
            encode = encodeTmp.trim();
        }
        // 使用配置文件属性 初始化 namespace
        initNamespace(properties);
        agent = new MetricsHttpAgent(new ServerHttpAgent(properties));
        // 初始化 ServerListManager 内部的服务器列表
        agent.start();
        // 往该对象设置 要监听的配置会 该对象会通过长轮询的方式监听配置变化 并在合适的时候更新配置 触发监听器
        worker = new ClientWorker(agent, configFilterChainManager, properties);
    }

    /**
     * 初始化命名空间
     * @param properties
     */
    private void initNamespace(Properties properties) {
        // 简言之 从环境变量获取  namespace
        namespace = ParamUtil.parseNamespace(properties);
        properties.put(PropertyKeyConst.NAMESPACE, namespace);
    }

    /**
     * 通过 group 和dataId 去集群中某个节点读取配置  (集群间会自己做数据同步  所以不需要考虑当时配置写到哪个节点)
     * @param dataId    dataId
     * @param group     group
     * @param timeoutMs read timeout
     * @return
     * @throws NacosException
     */
    @Override
    public String getConfig(String dataId, String group, long timeoutMs) throws NacosException {
        return getConfigInner(namespace, dataId, group, timeoutMs);
    }

    /**
     * 在获取配置的同时 注册监听器
     * @param dataId    dataId
     * @param group     group
     * @param timeoutMs read timeout
     * @param listener {@link Listener}
     * @return
     * @throws NacosException
     */
    @Override
    public String getConfigAndSignListener(String dataId, String group, long timeoutMs, Listener listener) throws NacosException {
        String content = getConfig(dataId, group, timeoutMs);
        worker.addTenantListenersWithContent(dataId, group, content, Arrays.asList(listener));
        return content;
    }

    /**
     * 为某个监听的配置追加 监听器
     * @param dataId   dataId
     * @param group    group
     * @param listener listener
     * @throws NacosException
     */
    @Override
    public void addListener(String dataId, String group, Listener listener) throws NacosException {
        worker.addTenantListeners(dataId, group, Arrays.asList(listener));
    }

    /**
     * 将某个配置发布到配置中心
     * @param dataId  dataId
     * @param group   group
     * @param content content
     * @return
     * @throws NacosException
     */
    @Override
    public boolean publishConfig(String dataId, String group, String content) throws NacosException {
        return publishConfigInner(namespace, dataId, group, null, null, null, content);
    }

    @Override
    public boolean removeConfig(String dataId, String group) throws NacosException {
        return removeConfigInner(namespace, dataId, group, null);
    }

    @Override
    public void removeListener(String dataId, String group, Listener listener) {
        worker.removeTenantListener(dataId, group, listener);
    }

    /**
     * 从 nacos-config server 上获取配置信息
     * @param tenant
     * @param dataId
     * @param group
     * @param timeoutMs
     * @return
     * @throws NacosException
     */
    private String getConfigInner(String tenant, String dataId, String group, long timeoutMs) throws NacosException {
        group = null2defaultGroup(group);
        // 非空校验
        ParamUtils.checkKeyParam(dataId, group);
        ConfigResponse cr = new ConfigResponse();

        cr.setDataId(dataId);
        cr.setTenant(tenant);
        cr.setGroup(group);

        // 这里优先使用本地配置  这里是避免配置中心全部宕机   那么应该有一个本地配置与远端配置同步的过程
        // 这会不会有点过度啊  又不是基于CP的实现 难道真的会全部宕机
        String content = LocalConfigInfoProcessor.getFailover(agent.getName(), dataId, group, tenant);
        if (content != null) {
            LOGGER.warn("[{}] [get-config] get failover ok, dataId={}, group={}, tenant={}, config={}", agent.getName(),
                dataId, group, tenant, ContentUtils.truncateContent(content));
            cr.setContent(content);
            configFilterChainManager.doFilter(null, cr);
            content = cr.getContent();
            return content;
        }

        // 不存在降级配置时  尝试访问配置中心
        try {
            String[] ct = worker.getServerConfig(dataId, group, tenant, timeoutMs);
            cr.setContent(ct[0]);

            configFilterChainManager.doFilter(null, cr);
            content = cr.getContent();

            return content;
        } catch (NacosException ioe) {
            if (NacosException.NO_RIGHT == ioe.getErrCode()) {
                throw ioe;
            }
            LOGGER.warn("[{}] [get-config] get from server error, dataId={}, group={}, tenant={}, msg={}",
                agent.getName(), dataId, group, tenant, ioe.toString());
        }

        // 当从服务器拉取失败时  尝试从快照文件中获取   快照中记录的是上次成功拉取的配置信息    确保高可用
        LOGGER.warn("[{}] [get-config] get snapshot ok, dataId={}, group={}, tenant={}, config={}", agent.getName(),
            dataId, group, tenant, ContentUtils.truncateContent(content));
        content = LocalConfigInfoProcessor.getSnapshot(agent.getName(), dataId, group, tenant);
        cr.setContent(content);
        configFilterChainManager.doFilter(null, cr);
        content = cr.getContent();
        return content;
    }

    private String null2defaultGroup(String group) {
        return (null == group) ? Constants.DEFAULT_GROUP : group.trim();
    }

    /**
     * 从配置中心移除某个配置
     * @param tenant
     * @param dataId
     * @param group
     * @param tag
     * @return
     * @throws NacosException
     */
    private boolean removeConfigInner(String tenant, String dataId, String group, String tag) throws NacosException {
        group = null2defaultGroup(group);
        ParamUtils.checkKeyParam(dataId, group);
        String url = Constants.CONFIG_CONTROLLER_PATH;
        List<String> params = new ArrayList<String>();
        params.add("dataId");
        params.add(dataId);
        params.add("group");
        params.add(group);
        if (StringUtils.isNotEmpty(tenant)) {
            params.add("tenant");
            params.add(tenant);
        }
        if (StringUtils.isNotEmpty(tag)) {
            params.add("tag");
            params.add(tag);
        }
        HttpResult result = null;
        try {
            result = agent.httpDelete(url, null, params, encode, POST_TIMEOUT);
        } catch (IOException ioe) {
            LOGGER.warn("[remove] error, " + dataId + ", " + group + ", " + tenant + ", msg: " + ioe.toString());
            return false;
        }

        if (HttpURLConnection.HTTP_OK == result.code) {
            LOGGER.info("[{}] [remove] ok, dataId={}, group={}, tenant={}", agent.getName(), dataId, group, tenant);
            return true;
        } else if (HttpURLConnection.HTTP_FORBIDDEN == result.code) {
            LOGGER.warn("[{}] [remove] error, dataId={}, group={}, tenant={}, code={}, msg={}", agent.getName(), dataId,
                group, tenant, result.code, result.content);
            throw new NacosException(result.code, result.content);
        } else {
            LOGGER.warn("[{}] [remove] error, dataId={}, group={}, tenant={}, code={}, msg={}", agent.getName(), dataId,
                group, tenant, result.code, result.content);
            return false;
        }
    }

    /**
     * 将某个配置发送到配置中心   发往集群中某个节点
     * 发布配置的同步 并不会监听某个配置的变化
     * @param tenant
     * @param dataId
     * @param group
     * @param tag
     * @param appName
     * @param betaIps
     * @param content
     * @return
     * @throws NacosException
     */
    private boolean publishConfigInner(String tenant, String dataId, String group, String tag, String appName,
                                       String betaIps, String content) throws NacosException {
        group = null2defaultGroup(group);
        ParamUtils.checkParam(dataId, group, content);

        ConfigRequest cr = new ConfigRequest();
        cr.setDataId(dataId);
        cr.setTenant(tenant);
        cr.setGroup(group);
        cr.setContent(content);
        // 当配置发往服务器时  经过过滤器只要设置 req
        configFilterChainManager.doFilter(cr, null);
        content = cr.getContent();

        String url = Constants.CONFIG_CONTROLLER_PATH;
        List<String> params = new ArrayList<String>();
        params.add("dataId");
        params.add(dataId);
        params.add("group");
        params.add(group);
        params.add("content");
        params.add(content);
        if (StringUtils.isNotEmpty(tenant)) {
            params.add("tenant");
            params.add(tenant);
        }
        if (StringUtils.isNotEmpty(appName)) {
            params.add("appName");
            params.add(appName);
        }
        if (StringUtils.isNotEmpty(tag)) {
            params.add("tag");
            params.add(tag);
        }

        List<String> headers = new ArrayList<String>();
        // 代表测试用配置
        if (StringUtils.isNotEmpty(betaIps)) {
            headers.add("betaIps");
            headers.add(betaIps);
        }

        HttpResult result = null;
        try {
            result = agent.httpPost(url, headers, params, encode, POST_TIMEOUT);
        } catch (IOException ioe) {
            LOGGER.warn("[{}] [publish-single] exception, dataId={}, group={}, msg={}", agent.getName(), dataId,
                group, ioe.toString());
            return false;
        }

        // 代表发布配置成功
        if (HttpURLConnection.HTTP_OK == result.code) {
            LOGGER.info("[{}] [publish-single] ok, dataId={}, group={}, tenant={}, config={}", agent.getName(), dataId,
                group, tenant, ContentUtils.truncateContent(content));
            return true;
        } else if (HttpURLConnection.HTTP_FORBIDDEN == result.code) {
            LOGGER.warn("[{}] [publish-single] error, dataId={}, group={}, tenant={}, code={}, msg={}", agent.getName(),
                dataId, group, tenant, result.code, result.content);
            throw new NacosException(result.code, result.content);
        } else {
            LOGGER.warn("[{}] [publish-single] error, dataId={}, group={}, tenant={}, code={}, msg={}", agent.getName(),
                dataId, group, tenant, result.code, result.content);
            return false;
        }

    }

    @Override
    public String getServerStatus() {
        if (worker.isHealthServer()) {
            return "UP";
        } else {
            return "DOWN";
        }
    }

}
