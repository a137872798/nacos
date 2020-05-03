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
package com.alibaba.nacos.client.config.http;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.client.config.impl.HttpSimpleClient;
import com.alibaba.nacos.client.config.impl.HttpSimpleClient.HttpResult;
import com.alibaba.nacos.client.config.impl.ServerListManager;
import com.alibaba.nacos.client.config.impl.SpasAdapter;
import com.alibaba.nacos.client.identify.STSConfig;
import com.alibaba.nacos.client.security.SecurityProxy;
import com.alibaba.nacos.client.utils.JSONUtils;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.client.utils.ParamUtil;
import com.alibaba.nacos.client.utils.TemplateUtils;
import com.alibaba.nacos.common.utils.IoUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * Server Agent
 *
 * @author water.lyl
 * 该对象 通过 http协议 与 nacos server 通信
 */
public class ServerHttpAgent implements HttpAgent {

    private static final Logger LOGGER = LogUtils.logger(ServerHttpAgent.class);

    /**
     * 对传输数据进行加密 确保安全
     */
    private SecurityProxy securityProxy;

    private String namespaceId;

    private long securityInfoRefreshIntervalMills = TimeUnit.SECONDS.toMillis(5);

    /**
     * @param path          相对于web应用根，以/开头
     * @param headers
     * @param paramValues
     * @param encoding
     * @param readTimeoutMs
     * @return
     * @throws IOException
     */
    @Override
    public HttpResult httpGet(String path, List<String> headers, List<String> paramValues, String encoding,
                              long readTimeoutMs) throws IOException {
        final long endTime = System.currentTimeMillis() + readTimeoutMs;
        final boolean isSSL = false;
        // 给参数设置额外的 权限相关信息
        injectSecurityInfo(paramValues);
        // 一开始可以选择多个 服务器 这里是随机获取一个
        String currentServerAddr = serverListMgr.getCurrentServerAddr();
        int maxRetry = this.maxRetry;

        do {
            try {
                // 这啥 spas 反正也是一种角色校验的 先不管
                List<String> newHeaders = getSpasHeaders(paramValues);
                if (headers != null) {
                    newHeaders.addAll(headers);
                }
                // 使用 HttpSimpleClient对象发起请求
                HttpResult result = HttpSimpleClient.httpGet(
                    getUrl(currentServerAddr, path), newHeaders, paramValues, encoding,
                    readTimeoutMs, isSSL);
                // 代表出现异常 只是打印日志
                if (result.code == HttpURLConnection.HTTP_INTERNAL_ERROR
                    || result.code == HttpURLConnection.HTTP_BAD_GATEWAY
                    || result.code == HttpURLConnection.HTTP_UNAVAILABLE) {
                    LOGGER.error("[NACOS ConnectException] currentServerAddr: {}, httpCode: {}",
                        serverListMgr.getCurrentServerAddr(), result.code);
                } else {
                    // Update the currently available server addr 更新成可用的地址
                    serverListMgr.updateCurrentServerAddr(currentServerAddr);
                    return result;
                }
            } catch (ConnectException ce) {
                LOGGER.error("[NACOS ConnectException httpGet] currentServerAddr:{}, err : {}", serverListMgr.getCurrentServerAddr(), ce.getMessage());
            } catch (SocketTimeoutException stoe) {
                LOGGER.error("[NACOS SocketTimeoutException httpGet] currentServerAddr:{}， err : {}", serverListMgr.getCurrentServerAddr(), stoe.getMessage());
            } catch (IOException ioe) {
                LOGGER.error("[NACOS IOException httpGet] currentServerAddr: " + serverListMgr.getCurrentServerAddr(), ioe);
                throw ioe;
            }

            // 失败时尝试更换nacos server地址后进行重试
            if (serverListMgr.getIterator().hasNext()) {
                currentServerAddr = serverListMgr.getIterator().next();
            } else {
                maxRetry--;
                if (maxRetry < 0) {
                    throw new ConnectException("[NACOS HTTP-GET] The maximum number of tolerable server reconnection errors has been reached");
                }
                // 代表已经遍历到尾部了 那么就从头开始遍历 (内部会重新创建一个迭代器)
                serverListMgr.refreshCurrentServerAddr();
            }

        } while (System.currentTimeMillis() <= endTime);

        LOGGER.error("no available server");
        throw new ConnectException("no available server");
    }

    /**
     * 发送post请求
     * @param path http path
     * @param headers http headers
     * @param paramValues http paramValues http
     * @param encoding http encode
     * @param readTimeoutMs http timeout
     * @return
     * @throws IOException
     */
    @Override
    public HttpResult httpPost(String path, List<String> headers, List<String> paramValues, String encoding,
                               long readTimeoutMs) throws IOException {
        final long endTime = System.currentTimeMillis() + readTimeoutMs;
        boolean isSSL = false;
        injectSecurityInfo(paramValues);
        // 这里只会发往单个节点  那么写入成功但是同步失败怎么办
        String currentServerAddr = serverListMgr.getCurrentServerAddr();
        int maxRetry = this.maxRetry;

        do {

            try {
                List<String> newHeaders = getSpasHeaders(paramValues);
                if (headers != null) {
                    newHeaders.addAll(headers);
                }

                // 这里获取 传输层的结果
                HttpResult result = HttpSimpleClient.httpPost(
                    getUrl(currentServerAddr, path), newHeaders, paramValues, encoding,
                    readTimeoutMs, isSSL);
                if (result.code == HttpURLConnection.HTTP_INTERNAL_ERROR
                    || result.code == HttpURLConnection.HTTP_BAD_GATEWAY
                    || result.code == HttpURLConnection.HTTP_UNAVAILABLE) {
                    LOGGER.error("[NACOS ConnectException] currentServerAddr: {}, httpCode: {}",
                        currentServerAddr, result.code);
                } else {
                    // Update the currently available server addr
                    // 这里是考虑到重试的情况 当前服务器地址已经发生变更
                    serverListMgr.updateCurrentServerAddr(currentServerAddr);
                    return result;
                }
            } catch (ConnectException ce) {
                LOGGER.error("[NACOS ConnectException httpPost] currentServerAddr: {}, err : {}", currentServerAddr, ce.getMessage());
            } catch (SocketTimeoutException stoe) {
                LOGGER.error("[NACOS SocketTimeoutException httpPost] currentServerAddr: {}， err : {}", currentServerAddr, stoe.getMessage());
            } catch (IOException ioe) {
                LOGGER.error("[NACOS IOException httpPost] currentServerAddr: " + currentServerAddr, ioe);
                throw ioe;
            }

            // 非io失败 尝试更换服务器地址 并重试
            if (serverListMgr.getIterator().hasNext()) {
                currentServerAddr = serverListMgr.getIterator().next();
            } else {
                maxRetry--;
                if (maxRetry < 0) {
                    throw new ConnectException("[NACOS HTTP-POST] The maximum number of tolerable server reconnection errors has been reached");
                }
                serverListMgr.refreshCurrentServerAddr();
            }

        } while (System.currentTimeMillis() <= endTime);

        LOGGER.error("no available server, currentServerAddr : {}", currentServerAddr);
        throw new ConnectException("no available server, currentServerAddr : " + currentServerAddr);
    }

    @Override
    public HttpResult httpDelete(String path, List<String> headers, List<String> paramValues, String encoding,
                                 long readTimeoutMs) throws IOException {
        final long endTime = System.currentTimeMillis() + readTimeoutMs;
        boolean isSSL = false;
        injectSecurityInfo(paramValues);
        String currentServerAddr = serverListMgr.getCurrentServerAddr();
        int maxRetry = this.maxRetry;

        do {
            try {
                List<String> newHeaders = getSpasHeaders(paramValues);
                if (headers != null) {
                    newHeaders.addAll(headers);
                }
                HttpResult result = HttpSimpleClient.httpDelete(
                    getUrl(currentServerAddr, path), newHeaders, paramValues, encoding,
                    readTimeoutMs, isSSL);
                if (result.code == HttpURLConnection.HTTP_INTERNAL_ERROR
                    || result.code == HttpURLConnection.HTTP_BAD_GATEWAY
                    || result.code == HttpURLConnection.HTTP_UNAVAILABLE) {
                    LOGGER.error("[NACOS ConnectException] currentServerAddr: {}, httpCode: {}",
                        serverListMgr.getCurrentServerAddr(), result.code);
                } else {
                    // Update the currently available server addr
                    serverListMgr.updateCurrentServerAddr(currentServerAddr);
                    return result;
                }
            } catch (ConnectException ce) {
                LOGGER.error("[NACOS ConnectException httpDelete] currentServerAddr:{}, err : {}", serverListMgr.getCurrentServerAddr(), ce.getMessage());
            } catch (SocketTimeoutException stoe) {
                LOGGER.error("[NACOS SocketTimeoutException httpDelete] currentServerAddr:{}， err : {}", serverListMgr.getCurrentServerAddr(), stoe.getMessage());
            } catch (IOException ioe) {
                LOGGER.error("[NACOS IOException httpDelete] currentServerAddr: " + serverListMgr.getCurrentServerAddr(), ioe);
                throw ioe;
            }

            if (serverListMgr.getIterator().hasNext()) {
                currentServerAddr = serverListMgr.getIterator().next();
            } else {
                maxRetry--;
                if (maxRetry < 0) {
                    throw new ConnectException("[NACOS HTTP-DELETE] The maximum number of tolerable server reconnection errors has been reached");
                }
                serverListMgr.refreshCurrentServerAddr();
            }

        } while (System.currentTimeMillis() <= endTime);

        LOGGER.error("no available server");
        throw new ConnectException("no available server");
    }

    /**
     * 拼接url
     * @param serverAddr
     * @param relativePath
     * @return
     */
    private String getUrl(String serverAddr, String relativePath) {
        String contextPath = serverListMgr.getContentPath().startsWith("/") ?
                serverListMgr.getContentPath() : "/" + serverListMgr.getContentPath();
        return serverAddr + contextPath + relativePath;
    }

    public static String getAppname() {
        return ParamUtil.getAppName();
    }

    public ServerHttpAgent(ServerListManager mgr) {
        serverListMgr = mgr;
    }

    public ServerHttpAgent(ServerListManager mgr, Properties properties) {
        serverListMgr = mgr;
        init(properties);
    }

    /**
     * 通过一个prop 对象进行初始化
     * @param properties
     * @throws NacosException
     */
    public ServerHttpAgent(Properties properties) throws NacosException {
        // 根据配置初始化服务器列表
        serverListMgr = new ServerListManager(properties);
        // 初始化一个需要验证 username  password 的对象
        securityProxy = new SecurityProxy(properties);
        // 获取命名空间
        namespaceId = properties.getProperty(PropertyKeyConst.NAMESPACE);
        // 初始化一些属性
        init(properties);
        // 使用 proxy对象登录 实际上就是通过username password 访问服务器指定的 url路径获取accessToken (如果对端没有要求username/password 就可以直接获取accessToken吧)
        // 更新失败时不做任何提示
        securityProxy.login(serverListMgr.getServerUrls());

        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("com.alibaba.nacos.client.config.security.updater");
                t.setDaemon(true);
                return t;
            }
        });

        // 定期刷新accessToken
        executorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                securityProxy.login(serverListMgr.getServerUrls());
            }
        }, 0, securityInfoRefreshIntervalMills, TimeUnit.MILLISECONDS);
    }

    /**
     * 启动 nacos client 时 会通过SecurityProxy 去某个节点上获取accessToken
     * @param params
     */
    private void injectSecurityInfo(List<String> params) {
        if (StringUtils.isNotBlank(securityProxy.getAccessToken())) {
            params.add(Constants.ACCESS_TOKEN);
            params.add(securityProxy.getAccessToken());
        }
        // 设置租户信息(也就是namespaceId)
        if (StringUtils.isNotBlank(namespaceId) && !params.contains(SpasAdapter.TENANT_KEY)) {
            params.add(SpasAdapter.TENANT_KEY);
            params.add(namespaceId);
        }
    }

    /**
     * 初始化一些其他信息
     * @param properties
     */
    private void init(Properties properties) {
        initEncode(properties);
        initAkSk(properties);
        initMaxRetry(properties);
    }

    /**
     * 默认编码选择 UTF-8
     * @param properties
     */
    private void initEncode(Properties properties) {
        encode = TemplateUtils.stringEmptyAndThenExecute(properties.getProperty(PropertyKeyConst.ENCODE), new Callable<String>() {
            @Override
            public String call() throws Exception {
                return Constants.ENCODE;
            }
        });
    }

    private void initAkSk(Properties properties) {
        String ramRoleName = properties.getProperty(PropertyKeyConst.RAM_ROLE_NAME);
        // 设置角色名称
        if (!StringUtils.isBlank(ramRoleName)) {
            STSConfig.getInstance().setRamRoleName(ramRoleName);
        }

        // 从认证模块获取 accessKey
        String ak = properties.getProperty(PropertyKeyConst.ACCESS_KEY);
        if (StringUtils.isBlank(ak)) {
            accessKey = SpasAdapter.getAk();
        } else {
            accessKey = ak;
        }

        // 获取密钥
        String sk = properties.getProperty(PropertyKeyConst.SECRET_KEY);
        if (StringUtils.isBlank(sk)) {
            secretKey = SpasAdapter.getSk();
        } else {
            secretKey = sk;
        }
    }

    private void initMaxRetry(Properties properties) {
        maxRetry = NumberUtils.toInt(String.valueOf(properties.get(PropertyKeyConst.MAX_RETRY)), Constants.MAX_RETRY);
    }

    @Override
    public synchronized void start() throws NacosException {
        serverListMgr.start();
    }

    private List<String> getSpasHeaders(List<String> paramValues) throws IOException {
        List<String> newHeaders = new ArrayList<String>();
        // STS 临时凭证鉴权的优先级高于 AK/SK 鉴权
        if (STSConfig.getInstance().isSTSOn()) {
            STSCredential sTSCredential = getSTSCredential();
            accessKey = sTSCredential.accessKeyId;
            secretKey = sTSCredential.accessKeySecret;
            newHeaders.add("Spas-SecurityToken");
            newHeaders.add(sTSCredential.securityToken);
        }

        if (StringUtils.isNotEmpty(accessKey) && StringUtils.isNotEmpty(secretKey)) {
            newHeaders.add("Spas-AccessKey");
            newHeaders.add(accessKey);
            List<String> signHeaders = SpasAdapter.getSignHeaders(paramValues, secretKey);
            if (signHeaders != null) {
                newHeaders.addAll(signHeaders);
            }
        }
        return newHeaders;
    }

    private STSCredential getSTSCredential() throws IOException {
        boolean cacheSecurityCredentials = STSConfig.getInstance().isCacheSecurityCredentials();
        if (cacheSecurityCredentials && sTSCredential != null) {
            long currentTime = System.currentTimeMillis();
            long expirationTime = sTSCredential.expiration.getTime();
            int timeToRefreshInMillisecond = STSConfig.getInstance().getTimeToRefreshInMillisecond();
            if (expirationTime - currentTime > timeToRefreshInMillisecond) {
                return sTSCredential;
            }
        }
        String stsResponse = getSTSResponse();
        STSCredential stsCredentialTmp = JSONUtils.deserializeObject(stsResponse,
            new TypeReference<STSCredential>() {
            });
        sTSCredential = stsCredentialTmp;
        LOGGER.info("[getSTSCredential] code:{}, accessKeyId:{}, lastUpdated:{}, expiration:{}", sTSCredential.getCode(),
            sTSCredential.getAccessKeyId(), sTSCredential.getLastUpdated(), sTSCredential.getExpiration());
        return sTSCredential;
    }

    private static String getSTSResponse() throws IOException {
        String securityCredentials = STSConfig.getInstance().getSecurityCredentials();
        if (securityCredentials != null) {
            return securityCredentials;
        }
        String securityCredentialsUrl = STSConfig.getInstance().getSecurityCredentialsUrl();
        HttpURLConnection conn = null;
        int respCode;
        String response;
        try {
            conn = (HttpURLConnection) new URL(securityCredentialsUrl).openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(ParamUtil.getConnectTimeout() > 100 ? ParamUtil.getConnectTimeout() : 100);
            conn.setReadTimeout(1000);
            conn.connect();
            respCode = conn.getResponseCode();
            if (HttpURLConnection.HTTP_OK == respCode) {
                response = IoUtils.toString(conn.getInputStream(), Constants.ENCODE);
            } else {
                response = IoUtils.toString(conn.getErrorStream(), Constants.ENCODE);
            }
        } catch (IOException e) {
            LOGGER.error("can not get security credentials", e);
            throw e;
        } finally {
            IoUtils.closeQuietly(conn);
        }
        if (HttpURLConnection.HTTP_OK == respCode) {
            return response;
        }
        LOGGER.error("can not get security credentials, securityCredentialsUrl: {}, responseCode: {}, response: {}",
            securityCredentialsUrl, respCode, response);
        throw new IOException(
            "can not get security credentials, responseCode: " + respCode + ", response: " + response);
    }

    @Override
    public String getName() {
        return serverListMgr.getName();
    }

    @Override
    public String getNamespace() {
        return serverListMgr.getNamespace();
    }

    @Override
    public String getTenant() {
        return serverListMgr.getTenant();
    }

    @Override
    public String getEncode() {
        return encode;
    }

    @SuppressWarnings("PMD.ClassNamingShouldBeCamelRule")
    private static class STSCredential {
        @JsonProperty(value = "AccessKeyId")
        private String accessKeyId;
        @JsonProperty(value = "AccessKeySecret")
        private String accessKeySecret;
        @JsonProperty(value = "Expiration")
        private Date expiration;
        @JsonProperty(value = "SecurityToken")
        private String securityToken;
        @JsonProperty(value = "LastUpdated")
        private Date lastUpdated;
        @JsonProperty(value = "Code")
        private String code;

        public String getAccessKeyId() {
            return accessKeyId;
        }

        public Date getExpiration() {
            return expiration;
        }

        public Date getLastUpdated() {
            return lastUpdated;
        }

        public String getCode() {
            return code;
        }

        @Override
        public String toString() {
            return "STSCredential{" +
                "accessKeyId='" + accessKeyId + '\'' +
                ", accessKeySecret='" + accessKeySecret + '\'' +
                ", expiration=" + expiration +
                ", securityToken='" + securityToken + '\'' +
                ", lastUpdated=" + lastUpdated +
                ", code='" + code + '\'' +
                '}';
        }
    }

    private String accessKey;
    private String secretKey;
    private String encode;
    private int maxRetry = 3;
    private volatile STSCredential sTSCredential;
    final ServerListManager serverListMgr;

}
