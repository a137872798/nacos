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

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.SystemPropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.client.config.impl.EventDispatcher.ServerlistChangeEvent;
import com.alibaba.nacos.client.config.impl.HttpSimpleClient.HttpResult;
import com.alibaba.nacos.client.utils.*;
import com.alibaba.nacos.common.utils.IoUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;



/**
 * Serverlist Manager
 *
 * @author Nacos
 * 该对象管理所有服务器列表
 */
public class ServerListManager {

    private static final Logger LOGGER = LogUtils.logger(ServerListManager.class);
    private static final String HTTPS = "https://";
    private static final String HTTP = "http://";

    public ServerListManager() {
        isFixed = false;
        isStarted = false;
        name = DEFAULT_NAME;
    }

    public ServerListManager(List<String> fixed) {
        this(fixed, null);
    }

    public ServerListManager(List<String> fixed, String namespace) {
        isFixed = true;
        isStarted = true;
        List<String> serverAddrs = new ArrayList<String>();
        for (String serverAddr : fixed) {
            String[] serverAddrArr = serverAddr.split(":");
            if (serverAddrArr.length == 1) {
                serverAddrs.add(serverAddrArr[0] + ":" + ParamUtil.getDefaultServerPort());
            } else {
                serverAddrs.add(serverAddr);
            }
        }
        serverUrls = new ArrayList<String>(serverAddrs);
        if (StringUtils.isBlank(namespace)) {
            name = FIXED_NAME + "-" + getFixedNameSuffix(serverAddrs.toArray(new String[serverAddrs.size()]));
        } else {
            this.namespace = namespace;
            name = FIXED_NAME + "-" + getFixedNameSuffix(serverAddrs.toArray(new String[serverAddrs.size()])) + "-"
                + namespace;
        }
    }

    public ServerListManager(String host, int port) {
        isFixed = false;
        isStarted = false;
        name = CUSTOM_NAME + "-" + host + "-" + port;
        addressServerUrl = String.format("http://%s:%d/%s/%s", host, port, contentPath, serverListName);
    }

    public ServerListManager(String endpoint) throws NacosException {
        this(endpoint, null);
    }

    public ServerListManager(String endpoint, String namespace) throws NacosException {
        isFixed = false;
        isStarted = false;
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.ENDPOINT, endpoint);
        endpoint = initEndpoint(properties);

        if (StringUtils.isBlank(endpoint)) {
            throw new NacosException(NacosException.CLIENT_INVALID_PARAM, "endpoint is blank");
        }
        if (StringUtils.isBlank(namespace)) {
            name = endpoint;
            addressServerUrl = String.format("http://%s:%d/%s/%s", endpoint, endpointPort, contentPath,
                serverListName);
        } else {
            if (StringUtils.isBlank(endpoint)) {
                throw new NacosException(NacosException.CLIENT_INVALID_PARAM, "endpoint is blank");
            }
            name = endpoint + "-" + namespace;
            this.namespace = namespace;
            this.tenant = namespace;
            addressServerUrl = String.format("http://%s:%d/%s/%s?namespace=%s", endpoint, endpointPort, contentPath,
                serverListName, namespace);
        }
    }

    /**
     * 服务器列表管理
     * @param properties
     * @throws NacosException
     */
    public ServerListManager(Properties properties) throws NacosException {
        isStarted = false;
        // 获取nacos 服务器地址  (就像注册中心的地址是写死的一样  )
        serverAddrsStr = properties.getProperty(PropertyKeyConst.SERVER_ADDR);
        String namespace = properties.getProperty(PropertyKeyConst.NAMESPACE);
        // 使用相关参数进行初始化
        initParam(properties);
        if (StringUtils.isNotEmpty(serverAddrsStr)) {
            isFixed = true;
            List<String> serverAddrs = new ArrayList<String>();
            String[] serverAddrsArr = serverAddrsStr.split(",");
            for (String serverAddr: serverAddrsArr) {
                // 确保该地址是使用 https/http 协议的
                if (serverAddr.startsWith(HTTPS) || serverAddr.startsWith(HTTP)) {
                    serverAddrs.add(serverAddr);
                } else {
                    String[] serverAddrArr = serverAddr.split(":");
                    // 如果没有指定 port 那么加上http前缀 以及默认port
                    if (serverAddrArr.length == 1) {
                        serverAddrs.add(HTTP + serverAddrArr[0] + ":" + ParamUtil.getDefaultServerPort());
                    } else {
                        // 只增加 http前缀
                        serverAddrs.add(HTTP + serverAddr);
                    }
                }
            }
            serverUrls = serverAddrs;
            if (StringUtils.isBlank(namespace)) {
                name = FIXED_NAME + "-" + getFixedNameSuffix(serverUrls.toArray(new String[serverUrls.size()]));
            } else {
                this.namespace = namespace;
                this.tenant = namespace;
                name = FIXED_NAME + "-" + getFixedNameSuffix(serverUrls.toArray(new String[serverUrls.size()])) + "-"
                    + namespace;
            }
        } else {
            // 如果是基于endpoint 进行初始化 那么 serverList 就是动态变化的  是一个地址服务的 url
            if (StringUtils.isBlank(endpoint)) {
                throw new NacosException(NacosException.CLIENT_INVALID_PARAM, "endpoint is blank");
            }
            isFixed = false;
            if (StringUtils.isBlank(namespace)) {
                name = endpoint;
                addressServerUrl = String.format("http://%s:%d/%s/%s", endpoint, endpointPort, contentPath,
                    serverListName);
            } else {
                this.namespace = namespace;
                this.tenant = namespace;
                name = endpoint + "-" + namespace;
                addressServerUrl = String.format("http://%s:%d/%s/%s?namespace=%s", endpoint, endpointPort,
                    contentPath, serverListName, namespace);
            }
        }

    }

    private void initParam(Properties properties) {
        endpoint = initEndpoint(properties);

        String contentPathTmp = properties.getProperty(PropertyKeyConst.CONTEXT_PATH);
        if (!StringUtils.isBlank(contentPathTmp)) {
            contentPath = contentPathTmp;
        }
        String serverListNameTmp = properties.getProperty(PropertyKeyConst.CLUSTER_NAME);
        if (!StringUtils.isBlank(serverListNameTmp)) {
            serverListName = serverListNameTmp;
        }
    }

    private String initEndpoint(final Properties properties) {

        String endpointPortTmp = TemplateUtils.stringEmptyAndThenExecute(System.getenv(PropertyKeyConst.SystemEnv.ALIBABA_ALIWARE_ENDPOINT_PORT), new Callable<String>() {
            @Override
            public String call() {
                return properties.getProperty(PropertyKeyConst.ENDPOINT_PORT);
            }
        });

        if (StringUtils.isNotBlank(endpointPortTmp)) {
            endpointPort = Integer.parseInt(endpointPortTmp);
        }

        String endpointTmp = properties.getProperty(PropertyKeyConst.ENDPOINT);

        // Whether to enable domain name resolution rules
        String isUseEndpointRuleParsing =
            properties.getProperty(PropertyKeyConst.IS_USE_ENDPOINT_PARSING_RULE,
                System.getProperty(SystemPropertyKeyConst.IS_USE_ENDPOINT_PARSING_RULE,
                    String.valueOf(ParamUtil.USE_ENDPOINT_PARSING_RULE_DEFAULT_VALUE)));
        if (Boolean.parseBoolean(isUseEndpointRuleParsing)) {
            String endpointUrl = ParamUtil.parsingEndpointRule(endpointTmp);
            if (StringUtils.isNotBlank(endpointUrl)) {
                serverAddrsStr = "";
            }
            return endpointUrl;
        }

        return StringUtils.isNotBlank(endpointTmp) ? endpointTmp : "";
    }

    /**
     * 生成获取服务器列表的任务
     * @throws NacosException
     */
    public synchronized void start() throws NacosException {

        // isStarted 代表启动完成  isFixed 代表从配置文件中读取到了 服务器列表
        if (isStarted || isFixed) {
            return;
        }

        // 剩下的情况就变成了 从 地址服务(endpoint)获取某集群下所有server地址
        GetServerListTask getServersTask = new GetServerListTask(addressServerUrl);
        for (int i = 0; i < initServerlistRetryTimes && serverUrls.isEmpty(); ++i) {
            getServersTask.run();
            try {
                this.wait((i + 1) * 100L);
            } catch (Exception e) {
                LOGGER.warn("get serverlist fail,url: {}", addressServerUrl);
            }
        }

        // 超过重试次数还没有拉取到服务列表 直接抛出异常
        if (serverUrls.isEmpty()) {
            LOGGER.error("[init-serverlist] fail to get NACOS-server serverlist! env: {}, url: {}", name,
                addressServerUrl);
            throw new NacosException(NacosException.SERVER_ERROR,
                "fail to get NACOS-server serverlist! env:" + name + ", not connnect url:" + addressServerUrl);
        }

        // 定期从地址服务拉取 集群服务器列表 同时发生变更时 发出一个事件
        TimerService.scheduleWithFixedDelay(getServersTask, 0L, 30L, TimeUnit.SECONDS);
        isStarted = true;
    }

    public List<String> getServerUrls() {
        return serverUrls;
    }

    Iterator<String> iterator() {
        if (serverUrls.isEmpty()) {
            LOGGER.error("[{}] [iterator-serverlist] No server address defined!", name);
        }
        return new ServerAddressIterator(serverUrls);
    }

    /**
     * 定期拉取服务器列表   也就是将获取服务器列表也做成动态的 而不是写死在配置文件
     */
    class GetServerListTask implements Runnable {
        /**
         * 本次会从哪里获取到 服务器列表
         */
        final String url;

        GetServerListTask(String url) {
            this.url = url;
        }

        @Override
        public void run() {
            /**
             * get serverlist from nameserver
             */
            try {
                // 更新本地服务列表
                updateIfChanged(getApacheServerList(url, name));
            } catch (Exception e) {
                LOGGER.error("[" + name + "][update-serverlist] failed to update serverlist from address server!",
                    e);
            }
        }
    }

    /**
     * 更新 serverUrls
     * @param newList
     */
    private void updateIfChanged(List<String> newList) {
        if (null == newList || newList.isEmpty()) {
            LOGGER.warn("[update-serverlist] current serverlist from address server is empty!!!");
            return;
        }

        List<String> newServerAddrList = new ArrayList<String>();
        // 加上 http 协议
        for (String server : newList) {
            if (server.startsWith(HTTP) || server.startsWith(HTTPS)) {
                newServerAddrList.add(server);
            } else {
                newServerAddrList.add(HTTP + server);
            }
        }

        /**
         * no change
         */
        if (newServerAddrList.equals(serverUrls)) {
            return;
        }
        // 当发生变化时 发出一个事件
        serverUrls = new ArrayList<String>(newServerAddrList);
        iterator = iterator();
        currentServerAddr = iterator.next();

        // 通过事件总线通知到设置在下面的监听器   用户可以设置钩子
        EventDispatcher.fireEvent(new ServerlistChangeEvent());
        LOGGER.info("[{}] [update-serverlist] serverlist updated to {}", name, serverUrls);
    }

    /**
     * 拉取服务器列表
     * @param url
     * @param name
     * @return
     */
    private List<String> getApacheServerList(String url, String name) {
        try {
            HttpResult httpResult = HttpSimpleClient.httpGet(url, null, null, null, 3000);

            if (HttpURLConnection.HTTP_OK == httpResult.code) {
                if (DEFAULT_NAME.equals(name)) {
                    EnvUtil.setSelfEnv(httpResult.headers);
                }
                // 按行读取数据
                List<String> lines = IoUtils.readLines(new StringReader(httpResult.content));
                // 转换成 ip : port
                List<String> result = new ArrayList<String>(lines.size());
                for (String serverAddr : lines) {
                    if (org.apache.commons.lang3.StringUtils.isNotBlank(serverAddr)) {
                        String[] ipPort = serverAddr.trim().split(":");
                        String ip = ipPort[0].trim();
                        if (ipPort.length == 1) {
                            result.add(ip + ":" + ParamUtil.getDefaultServerPort());
                        } else {
                            result.add(serverAddr);
                        }
                    }
                }
                return result;
            } else {
                LOGGER.error("[check-serverlist] error. addressServerUrl: {}, code: {}", addressServerUrl,
                    httpResult.code);
                return null;
            }
        } catch (IOException e) {
            LOGGER.error("[check-serverlist] exception. url: " + url, e);
            return null;
        }
    }

    String getUrlString() {
        return serverUrls.toString();
    }

    /**
     * 将所有 nacos server 的地址串起来
     * @param serverIps
     * @return
     */
    String getFixedNameSuffix(String... serverIps) {
        StringBuilder sb = new StringBuilder();
        String split = "";
        for (String serverIp : serverIps) {
            sb.append(split);
            serverIp = serverIp.replaceAll("http(s)?://", "");
            sb.append(serverIp.replaceAll(":", "_"));
            split = "-";
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "ServerManager-" + name + "-" + getUrlString();
    }

    public boolean contain(String ip) {

        return serverUrls.contains(ip);
    }

    public void refreshCurrentServerAddr() {
        iterator = iterator();
        currentServerAddr = iterator.next();
    }

    public String getCurrentServerAddr() {
        if (StringUtils.isBlank(currentServerAddr)) {
            iterator = iterator();
            currentServerAddr = iterator.next();
        }
        return currentServerAddr;
    }

    public void updateCurrentServerAddr(String currentServerAddr) {
        this.currentServerAddr = currentServerAddr;
    }

    public Iterator<String> getIterator() {
        return iterator;
    }

    public String getContentPath() {
        return contentPath;
    }

    public String getName() {
        return name;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getTenant() {
        return tenant;
    }

    /**
     * The name of the different environment
     * 包含了所有nacos server 的地址
     */
    private String name;
    private String namespace = "";
    private String tenant = "";
    static public final String DEFAULT_NAME = "default";
    static public final String CUSTOM_NAME = "custom";
    static public final String FIXED_NAME = "fixed";
    private int initServerlistRetryTimes = 5;
    /**
     * Connection timeout and socket timeout with other servers
     */
    static final int TIMEOUT = 5000;

    final boolean isFixed;
    boolean isStarted = false;
    private String endpoint;
    private int endpointPort = 8080;
    private String contentPath = ParamUtil.getDefaultContextPath();
    private String serverListName = ParamUtil.getDefaultNodesPath();
    /**
     * 本次 nacos 的一组服务器地址
     */
    volatile List<String> serverUrls = new ArrayList<String>();

    private volatile String currentServerAddr;

    private Iterator<String> iterator;
    public String serverPort = ParamUtil.getDefaultServerPort();

    /**
     * 这是地址服务的 url  代表可以通过访问该url 获得集群内服务地址
     */
    public String addressServerUrl;

    private String serverAddrsStr;

}

/**
 * Sort the address list, with the same room priority.
 * 通过该迭代器获取服务器列表
 */
class ServerAddressIterator implements Iterator<String> {

    // 指定了 serverUrls 的优先级
    static class RandomizedServerAddress implements Comparable<RandomizedServerAddress> {
        static Random random = new Random();

        String serverIp;
        int priority = 0;
        int seed;

        public RandomizedServerAddress(String ip) {
            try {
                this.serverIp = ip;
                /**
                 * change random scope from 32 to Integer.MAX_VALUE to fix load balance issue
                 */
                this.seed = random.nextInt(Integer.MAX_VALUE);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int compareTo(RandomizedServerAddress other) {
            if (this.priority != other.priority) {
                return other.priority - this.priority;
            } else {
                return other.seed - this.seed;
            }
        }
    }

    public ServerAddressIterator(List<String> source) {
        sorted = new ArrayList<RandomizedServerAddress>();
        for (String address : source) {
            // 将每个地址包装成RandomizedServerAddress  这样当优先级相同时 也能排序
            sorted.add(new RandomizedServerAddress(address));
        }
        // 获取排序后结果的迭代器 并委托转发到该对象上
        Collections.sort(sorted);
        iter = sorted.iterator();
    }

    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }

    @Override
    public String next() {
        return iter.next().serverIp;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    final List<RandomizedServerAddress> sorted;
    final Iterator<RandomizedServerAddress> iter;
}

