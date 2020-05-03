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
package com.alibaba.nacos.client.naming.net;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.SystemPropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.CommonParams;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacos.api.naming.pojo.Service;
import com.alibaba.nacos.api.selector.AbstractSelector;
import com.alibaba.nacos.api.selector.ExpressionSelector;
import com.alibaba.nacos.api.selector.SelectorType;
import com.alibaba.nacos.client.config.impl.SpasAdapter;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.beat.BeatInfo;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.naming.utils.NetUtils;
import com.alibaba.nacos.client.naming.utils.SignUtil;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.client.security.SecurityProxy;
import com.alibaba.nacos.client.utils.AppNameUtils;
import com.alibaba.nacos.client.utils.TemplateUtils;
import com.alibaba.nacos.common.constant.HttpHeaderConsts;
import com.alibaba.nacos.common.utils.HttpMethod;
import com.alibaba.nacos.common.utils.IoUtils;
import com.alibaba.nacos.common.utils.UuidUtils;
import com.alibaba.nacos.common.utils.VersionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.*;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * @author nkorange
 */
public class NamingProxy {

    private static final int DEFAULT_SERVER_PORT = 8848;

    private int serverPort = DEFAULT_SERVER_PORT;

    private String namespaceId;

    private String endpoint;

    /**
     * 只有一个服务地址时才会设置该字段
     */
    private String nacosDomain;

    /**
     * 服务器地址列表
     */
    private List<String> serverList;

    /**
     * 代表从endpoint 拉取到的服务地址列表
     */
    private List<String> serversFromEndpoint = new ArrayList<String>();

    /**
     * 定期拉取 accessToken  (会使用一个定时器 定期的拉取 accessToken 的信息)
     */
    private SecurityProxy securityProxy;

    private long lastSrvRefTime = 0L;

    private long vipSrvRefInterMillis = TimeUnit.SECONDS.toMillis(30);

    private long securityInfoRefreshIntervalMills = TimeUnit.SECONDS.toMillis(5);

    private Properties properties;

    public NamingProxy(String namespaceId, String endpoint, String serverList, Properties properties) {

        // 获取到用户名密码 以换取 accessToken  TODO 权限校验的可以先不看
        securityProxy = new SecurityProxy(properties);
        this.properties = properties;
        // 代表默认会访问的服务器端口
        this.setServerPort(DEFAULT_SERVER_PORT);
        this.namespaceId = namespaceId;
        this.endpoint = endpoint;
        // 初始化服务器集群的地址
        if (StringUtils.isNotEmpty(serverList)) {
            this.serverList = Arrays.asList(serverList.split(","));
            if (this.serverList.size() == 1) {
                this.nacosDomain = serverList;
            }
        }

        // 如果是从地址服务获取集群地址 那么要开启定时刷新
        initRefreshTask();
    }

    private void initRefreshTask() {

        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(2, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("com.alibaba.nacos.client.naming.updater");
                t.setDaemon(true);
                return t;
            }
        });

        /**
         * 对应从地址服务拉取任务时设置
         */
        executorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                refreshSrvIfNeed();
            }
        }, 0, vipSrvRefInterMillis, TimeUnit.MILLISECONDS);


        /**
         * 定期刷新 accessToken
         */
        executorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                securityProxy.login(getServerList());
            }
        }, 0, securityInfoRefreshIntervalMills, TimeUnit.MILLISECONDS);

        // 首次启动时强制拉取地址
        refreshSrvIfNeed();
        // 强制获取accessToken
        securityProxy.login(getServerList());
    }

    /**
     * 使用 httpClient 拉取服务地址
     * @return
     */
    public List<String> getServerListFromEndpoint() {

        try {
            // 即使不设置 endpoint 也会拉取   不能做下非空校验么???
            String urlString = "http://" + endpoint + "/nacos/serverlist";
            List<String> headers = builderHeaders();

            HttpClient.HttpResult result = HttpClient.httpGet(urlString, headers, null, UtilAndComs.ENCODING);
            if (HttpURLConnection.HTTP_OK != result.code) {
                throw new IOException("Error while requesting: " + urlString + "'. Server returned: "
                    + result.code);
            }

            String content = result.content;
            List<String> list = new ArrayList<String>();
            for (String line : IoUtils.readLines(new StringReader(content))) {
                if (!line.trim().isEmpty()) {
                    list.add(line.trim());
                }
            }

            return list;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 定期从endpoint 拉取 serverAddressList
     */
    private void refreshSrvIfNeed() {
        try {

            if (!CollectionUtils.isEmpty(serverList)) {
                NAMING_LOGGER.debug("server list provided by user: " + serverList);
                return;
            }

            if (System.currentTimeMillis() - lastSrvRefTime < vipSrvRefInterMillis) {
                return;
            }

            List<String> list = getServerListFromEndpoint();

            if (CollectionUtils.isEmpty(list)) {
                throw new Exception("Can not acquire Nacos list");
            }

            if (!CollectionUtils.isEqualCollection(list, serversFromEndpoint)) {
                NAMING_LOGGER.info("[SERVER-LIST] server list is updated: " + list);
            }

            serversFromEndpoint = list;
            lastSrvRefTime = System.currentTimeMillis();
        } catch (Throwable e) {
            NAMING_LOGGER.warn("failed to update server list", e);
        }
    }

    /**
     * 将某一服务元数据 注册到 namingService
     * @param serviceName
     * @param groupName
     * @param instance
     * @throws NacosException
     */
    public void registerService(String serviceName, String groupName, Instance instance) throws NacosException {

        NAMING_LOGGER.info("[REGISTER-SERVICE] {} registering service {} with instance: {}",
            namespaceId, serviceName, instance);

        final Map<String, String> params = new HashMap<String, String>(9);
        params.put(CommonParams.NAMESPACE_ID, namespaceId);
        // 也就是粗粒度匹配  而不是按照接口级进行匹配  (不过这种精度确实没必要)
        params.put(CommonParams.SERVICE_NAME, serviceName);
        params.put(CommonParams.GROUP_NAME, groupName);
        params.put(CommonParams.CLUSTER_NAME, instance.getClusterName());
        params.put("ip", instance.getIp());
        params.put("port", String.valueOf(instance.getPort()));
        params.put("weight", String.valueOf(instance.getWeight()));
        params.put("enable", String.valueOf(instance.isEnabled()));
        params.put("healthy", String.valueOf(instance.isHealthy()));
        params.put("ephemeral", String.valueOf(instance.isEphemeral()));
        params.put("metadata", JSON.toJSONString(instance.getMetadata()));

        // restful 风格
        reqAPI(UtilAndComs.NACOS_URL_INSTANCE, params, HttpMethod.POST);

    }

    /**
     * 注销某服务
     * @param serviceName  该信息用于描述 应用信息 比如属于用户服务 订单服务等
     * @param instance  端点信息 比如 ip: port
     * @throws NacosException
     */
    public void deregisterService(String serviceName, Instance instance) throws NacosException {

        NAMING_LOGGER.info("[DEREGISTER-SERVICE] {} deregistering service {} with instance: {}",
            namespaceId, serviceName, instance);

        final Map<String, String> params = new HashMap<String, String>(8);
        params.put(CommonParams.NAMESPACE_ID, namespaceId);
        params.put(CommonParams.SERVICE_NAME, serviceName);
        params.put(CommonParams.CLUSTER_NAME, instance.getClusterName());
        params.put("ip", instance.getIp());
        params.put("port", String.valueOf(instance.getPort()));
        params.put("ephemeral", String.valueOf(instance.isEphemeral()));

        reqAPI(UtilAndComs.NACOS_URL_INSTANCE, params, HttpMethod.DELETE);
    }

    public void updateInstance(String serviceName, String groupName, Instance instance) throws NacosException {
        NAMING_LOGGER.info("[UPDATE-SERVICE] {} update service {} with instance: {}",
            namespaceId, serviceName, instance);

        final Map<String, String> params = new HashMap<String, String>(8);
        params.put(CommonParams.NAMESPACE_ID, namespaceId);
        params.put(CommonParams.SERVICE_NAME, serviceName);
        params.put(CommonParams.GROUP_NAME, groupName);
        params.put(CommonParams.CLUSTER_NAME, instance.getClusterName());
        params.put("ip", instance.getIp());
        params.put("port", String.valueOf(instance.getPort()));
        params.put("weight", String.valueOf(instance.getWeight()));
        params.put("enabled", String.valueOf(instance.isEnabled()));
        params.put("ephemeral", String.valueOf(instance.isEphemeral()));
        params.put("metadata", JSON.toJSONString(instance.getMetadata()));

        reqAPI(UtilAndComs.NACOS_URL_INSTANCE, params, HttpMethod.PUT);
    }

    public Service queryService(String serviceName, String groupName) throws NacosException {
        NAMING_LOGGER.info("[QUERY-SERVICE] {} query service : {}, {}",
            namespaceId, serviceName, groupName);

        final Map<String, String> params = new HashMap<String, String>(3);
        params.put(CommonParams.NAMESPACE_ID, namespaceId);
        params.put(CommonParams.SERVICE_NAME, serviceName);
        params.put(CommonParams.GROUP_NAME, groupName);

        String result = reqAPI(UtilAndComs.NACOS_URL_SERVICE, params, HttpMethod.GET);
        JSONObject jsonObject = JSON.parseObject(result);
        return jsonObject.toJavaObject(Service.class);
    }

    /**
     *
     * @param service
     * @param selector   相当于一种查询条件
     * @throws NacosException
     */
    public void createService(Service service, AbstractSelector selector) throws NacosException {

        NAMING_LOGGER.info("[CREATE-SERVICE] {} creating service : {}",
            namespaceId, service);

        final Map<String, String> params = new HashMap<String, String>(6);
        params.put(CommonParams.NAMESPACE_ID, namespaceId);
        params.put(CommonParams.SERVICE_NAME, service.getName());
        params.put(CommonParams.GROUP_NAME, service.getGroupName());
        params.put("protectThreshold", String.valueOf(service.getProtectThreshold()));
        params.put("metadata", JSON.toJSONString(service.getMetadata()));
        // 这里填充了一个 选择器
        params.put("selector", JSON.toJSONString(selector));

        reqAPI(UtilAndComs.NACOS_URL_SERVICE, params, HttpMethod.POST);

    }

    public boolean deleteService(String serviceName, String groupName) throws NacosException {
        NAMING_LOGGER.info("[DELETE-SERVICE] {} deleting service : {} with groupName : {}",
            namespaceId, serviceName, groupName);

        final Map<String, String> params = new HashMap<String, String>(6);
        params.put(CommonParams.NAMESPACE_ID, namespaceId);
        params.put(CommonParams.SERVICE_NAME, serviceName);
        params.put(CommonParams.GROUP_NAME, groupName);

        String result = reqAPI(UtilAndComs.NACOS_URL_SERVICE, params, HttpMethod.DELETE);
        return "ok".equals(result);
    }

    public void updateService(Service service, AbstractSelector selector) throws NacosException {
        NAMING_LOGGER.info("[UPDATE-SERVICE] {} updating service : {}",
            namespaceId, service);

        final Map<String, String> params = new HashMap<String, String>(6);
        params.put(CommonParams.NAMESPACE_ID, namespaceId);
        params.put(CommonParams.SERVICE_NAME, service.getName());
        params.put(CommonParams.GROUP_NAME, service.getGroupName());
        params.put("protectThreshold", String.valueOf(service.getProtectThreshold()));
        params.put("metadata", JSON.toJSONString(service.getMetadata()));
        params.put("selector", JSON.toJSONString(selector));

        reqAPI(UtilAndComs.NACOS_URL_SERVICE, params, HttpMethod.PUT);
    }

    public String queryList(String serviceName, String clusters, int udpPort, boolean healthyOnly)
        throws NacosException {

        final Map<String, String> params = new HashMap<String, String>(8);
        params.put(CommonParams.NAMESPACE_ID, namespaceId);
        params.put(CommonParams.SERVICE_NAME, serviceName);
        params.put("clusters", clusters);
        params.put("udpPort", String.valueOf(udpPort));
        params.put("clientIP", NetUtils.localIP());
        params.put("healthyOnly", String.valueOf(healthyOnly));

        return reqAPI(UtilAndComs.NACOS_URL_BASE + "/instance/list", params, HttpMethod.GET);
    }

    /**
     * 发送续约请求
     * @param beatInfo
     * @param lightBeatEnabled  是否是轻量级心跳  少发送一些数据
     * @return
     * @throws NacosException
     */
    public JSONObject sendBeat(BeatInfo beatInfo, boolean lightBeatEnabled) throws NacosException {

        if (NAMING_LOGGER.isDebugEnabled()) {
            NAMING_LOGGER.debug("[BEAT] {} sending beat to server: {}", namespaceId, beatInfo.toString());
        }
        Map<String, String> params = new HashMap<String, String>(8);
        String body = StringUtils.EMPTY;
        if (!lightBeatEnabled) {
            try {
                body = "beat=" + URLEncoder.encode(JSON.toJSONString(beatInfo), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new NacosException(NacosException.SERVER_ERROR, "encode beatInfo error", e);
            }
        }
        params.put(CommonParams.NAMESPACE_ID, namespaceId);
        params.put(CommonParams.SERVICE_NAME, beatInfo.getServiceName());
        params.put(CommonParams.CLUSTER_NAME, beatInfo.getCluster());
        params.put("ip", beatInfo.getIp());
        params.put("port", String.valueOf(beatInfo.getPort()));
        String result = reqAPI(UtilAndComs.NACOS_URL_BASE + "/instance/beat", params, body, HttpMethod.PUT);
        return JSON.parseObject(result);
    }

    /**
     * 判断 nacos server 是否是正常的  如果不正常那么不是根据就无法访问到吗  (还是说有着启动 但是未运作的状态)
     * @return
     */
    public boolean serverHealthy() {

        try {
            String result = reqAPI(UtilAndComs.NACOS_URL_BASE + "/operator/metrics",
                new HashMap<String, String>(2), HttpMethod.GET);
            JSONObject json = JSON.parseObject(result);
            String serverStatus = json.getString("status");
            return "UP".equals(serverStatus);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 拉取某个组下的服务列表 (分页)
     * @param pageNo
     * @param pageSize
     * @param groupName
     * @return
     * @throws NacosException
     */
    public ListView<String> getServiceList(int pageNo, int pageSize, String groupName) throws NacosException {
        return getServiceList(pageNo, pageSize, groupName, null);
    }

    /**
     * select 作为一种参数
     * @param pageNo
     * @param pageSize
     * @param groupName
     * @param selector
     * @return
     * @throws NacosException
     */
    public ListView<String> getServiceList(int pageNo, int pageSize, String groupName, AbstractSelector selector) throws NacosException {

        Map<String, String> params = new HashMap<String, String>(4);
        params.put("pageNo", String.valueOf(pageNo));
        params.put("pageSize", String.valueOf(pageSize));
        params.put(CommonParams.NAMESPACE_ID, namespaceId);
        params.put(CommonParams.GROUP_NAME, groupName);

        if (selector != null) {
            switch (SelectorType.valueOf(selector.getType())) {
                case none:
                    break;
                    // 代表基于label的一个过滤器   用于快速定位服务
                case label:
                    ExpressionSelector expressionSelector = (ExpressionSelector) selector;
                    params.put("selector", JSON.toJSONString(expressionSelector));
                    break;
                default:
                    break;
            }
        }

        String result = reqAPI(UtilAndComs.NACOS_URL_BASE + "/service/list", params, HttpMethod.GET);

        JSONObject json = JSON.parseObject(result);
        ListView<String> listView = new ListView<String>();
        listView.setCount(json.getInteger("count"));
        listView.setData(JSON.parseObject(json.getString("doms"), new TypeReference<List<String>>() {
        }));

        return listView;
    }

    public String reqAPI(String api, Map<String, String> params, String method) throws NacosException {
        return reqAPI(api, params, StringUtils.EMPTY, method);
    }

    public String reqAPI(String api, Map<String, String> params, String body, String method) throws NacosException {
        return reqAPI(api, params, body, getServerList(), method);
    }

    private List<String> getServerList() {
        List<String> snapshot = serversFromEndpoint;
        if (!CollectionUtils.isEmpty(serverList)) {
            snapshot = serverList;
        }
        return snapshot;
    }

    public String callServer(String api, Map<String, String> params, String body, String curServer) throws NacosException {
        return callServer(api, params, body, curServer, HttpMethod.GET);
    }

    /**
     * 基于 restful 风格 发送http请求 往服务器上注册元数据
     * @param api
     * @param params
     * @param body
     * @param curServer
     * @param method
     * @return
     * @throws NacosException
     */
    public String callServer(String api, Map<String, String> params, String body, String curServer, String method)
        throws NacosException {
        long start = System.currentTimeMillis();
        long end = 0;
        // 安全校验的可以不看 不影响主流程
        injectSecurityInfo(params);
        // 生成固定的请求头
        List<String> headers = builderHeaders();

        String url;
        if (curServer.startsWith(UtilAndComs.HTTPS) || curServer.startsWith(UtilAndComs.HTTP)) {
            url = curServer + api;
        } else {
            // 添加http 前缀
            if (!curServer.contains(UtilAndComs.SERVER_ADDR_IP_SPLITER)) {
                curServer = curServer + UtilAndComs.SERVER_ADDR_IP_SPLITER + serverPort;
            }
            url = HttpClient.getPrefix() + curServer + api;
        }

        HttpClient.HttpResult result = HttpClient.request(url, headers, params, body, UtilAndComs.ENCODING, method);
        end = System.currentTimeMillis();

        MetricsMonitor.getNamingRequestMonitor(method, url, String.valueOf(result.code))
            .observe(end - start);

        if (HttpURLConnection.HTTP_OK == result.code) {
            return result.content;
        }

        if (HttpURLConnection.HTTP_NOT_MODIFIED == result.code) {
            return StringUtils.EMPTY;
        }

        throw new NacosException(result.code, result.content);
    }

    /**
     * 代表向一组 nacos server 注册服务元数据
     * @param api  代表访问的api 比如 /instance
     * @param params  服务实例信息
     * @param body
     * @param servers  发往的服务列表  (照理说只要往一个服务节点发送就可以了 如果使用了分布式协调算法的话)
     * @param method   restful 对应的请求方式
     * @return
     * @throws NacosException
     */
    public String reqAPI(String api, Map<String, String> params, String body, List<String> servers, String method) throws NacosException {

        // 追加命名空间
        params.put(CommonParams.NAMESPACE_ID, getNamespaceId());

        if (CollectionUtils.isEmpty(servers) && StringUtils.isEmpty(nacosDomain)) {
            throw new NacosException(NacosException.INVALID_PARAM, "no server available");
        }

        NacosException exception = new NacosException();

        if (servers != null && !servers.isEmpty()) {

            Random random = new Random(System.currentTimeMillis());
            int index = random.nextInt(servers.size());

            for (int i = 0; i < servers.size(); i++) {
                String server = servers.get(index);
                try {
                    // 随机选择一个地址进行发送就可以  多个server 之间会自动做同步
                    return callServer(api, params, body, server, method);
                } catch (NacosException e) {
                    exception = e;
                    if (NAMING_LOGGER.isDebugEnabled()) {
                        NAMING_LOGGER.debug("request {} failed.", server, e);
                    }
                }
                // 失败进行重试
                index = (index + 1) % servers.size();
            }
        }

        // 单节点不断进行重试
        if (StringUtils.isNotBlank(nacosDomain)) {
            for (int i = 0; i < UtilAndComs.REQUEST_DOMAIN_RETRY_COUNT; i++) {
                try {
                    return callServer(api, params, body, nacosDomain, method);
                } catch (NacosException e) {
                    exception = e;
                    if (NAMING_LOGGER.isDebugEnabled()) {
                        NAMING_LOGGER.debug("request {} failed.", nacosDomain, e);
                    }
                }
            }
        }

        NAMING_LOGGER.error("request: {} failed, servers: {}, code: {}, msg: {}",
            api, servers, exception.getErrCode(), exception.getErrMsg());

        // 当所有节点都宕机时 续约任务直接失败 所以实例信息丢失是允许的
        throw new NacosException(exception.getErrCode(), "failed to req API:/api/" + api + " after all servers(" + servers + ") tried: "
            + exception.getMessage());

    }

    /**
     * 追加安全信息
     * @param params
     */
    private void injectSecurityInfo(Map<String, String> params) {

        // Inject token if exist:
        if (StringUtils.isNotBlank(securityProxy.getAccessToken())) {
            params.put(Constants.ACCESS_TOKEN, securityProxy.getAccessToken());
        }

        // Inject ak/sk if exist:
        String ak = getAccessKey();
        String sk = getSecretKey();
        params.put("app", AppNameUtils.getAppName());
        if (StringUtils.isNotBlank(ak) && StringUtils.isNotBlank(sk)) {
            try {
                String signData = getSignData(params.get("serviceName"));
                String signature = SignUtil.sign(signData, sk);
                params.put("signature", signature);
                params.put("data", signData);
                params.put("ak", ak);
            } catch (Exception e) {
                NAMING_LOGGER.error("inject ak/sk failed.", e);
            }
        }
    }

    /**
     * 生成固定的请求头
     * @return
     */
    public List<String> builderHeaders() {
        List<String> headers = Arrays.asList(
            HttpHeaderConsts.CLIENT_VERSION_HEADER, VersionUtils.VERSION,
            HttpHeaderConsts.USER_AGENT_HEADER, UtilAndComs.VERSION,
            "Accept-Encoding", "gzip,deflate,sdch",
            "Connection", "Keep-Alive",
            "RequestId", UuidUtils.generateUuid(), "Request-Module", "Naming");
        return headers;
    }

    private static String getSignData(String serviceName) {
        return StringUtils.isNotEmpty(serviceName)
            ? System.currentTimeMillis() + "@@" + serviceName
            : String.valueOf(System.currentTimeMillis());
    }

    /**
     * accessKey 和 accessToken 不是一个东西 不要搞混
     * @return
     */
    public String getAccessKey() {
        if (properties == null) {

            return SpasAdapter.getAk();
        }

        return TemplateUtils.stringEmptyAndThenExecute(properties.getProperty(PropertyKeyConst.ACCESS_KEY), new Callable<String>() {

            @Override
            public String call() {
                return SpasAdapter.getAk();
            }
        });
    }

    public String getSecretKey() {
        if (properties == null) {

            return SpasAdapter.getSk();
        }

        return TemplateUtils.stringEmptyAndThenExecute(properties.getProperty(PropertyKeyConst.SECRET_KEY), new Callable<String>() {
            @Override
            public String call() throws Exception {
                return SpasAdapter.getSk();
            }
        });
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
        setServerPort(DEFAULT_SERVER_PORT);
    }

    public String getNamespaceId() {
        return namespaceId;
    }

    public void setServerPort(int serverPort) {
        this.serverPort = serverPort;

        String sp = System.getProperty(SystemPropertyKeyConst.NAMING_SERVER_PORT);
        if (StringUtils.isNotBlank(sp)) {
            this.serverPort = Integer.parseInt(sp);
        }
    }

}

