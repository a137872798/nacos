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
package com.alibaba.nacos.client.security;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.client.naming.net.HttpClient;
import com.alibaba.nacos.common.utils.HttpMethod;
import org.apache.commons.codec.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Security proxy to update security information
 *
 * @author nkorange
 * @since 1.2.0
 * 该对象是专门用于更新安全信息的
 */
public class SecurityProxy {

    private static final Logger SECURITY_LOGGER = LoggerFactory.getLogger(SecurityProxy.class);

    private static final String LOGIN_URL = "/v1/auth/users/login";

    /**
     * 代表应用路径  在后面追加 Login_url
     */
    private String contextPath;

    // 指定用户名密码

    /**
     * User's name
     */
    private String username;

    /**
     * User's password
     */
    private String password;

    /**
     * A token to take with when sending request to Nacos server
     * 访问 nacos server 时 需要携带一个 accessToken
     */
    private String accessToken;

    /**
     * TTL of token in seconds
     * 代表token有效时长
     */
    private long tokenTtl;

    /**
     * Last timestamp refresh security info from server
     */
    private long lastRefreshTime;

    /**
     * time window to refresh security info in seconds
     * 多久更新一次安全层信息
     */
    private long tokenRefreshWindow;

    /**
     * Construct from properties, keeping flexibility
     *
     * @param properties a bunch of properties to read
     *
     */
    public SecurityProxy(Properties properties) {
        username = properties.getProperty(PropertyKeyConst.USERNAME, StringUtils.EMPTY);
        password = properties.getProperty(PropertyKeyConst.PASSWORD, StringUtils.EMPTY);
        contextPath = properties.getProperty(PropertyKeyConst.CONTEXT_PATH, "/nacos");
        contextPath = contextPath.startsWith("/") ? contextPath : "/" + contextPath;
    }

    /**
     * 指定一组服务地址 并进行登录
     * @param servers
     * @return
     */
    public boolean login(List<String> servers) {

        try {
            if ((System.currentTimeMillis() - lastRefreshTime) < TimeUnit.SECONDS.toMillis(tokenTtl - tokenRefreshWindow)) {
                return true;
            }

            for (String server : servers) {
                // 当更新成功时  刷新lastRefreshTime
                if (login(server)) {
                    lastRefreshTime = System.currentTimeMillis();
                    return true;
                }
            }
        } catch (Throwable ignore) {
        }

        return false;
    }

    /**
     * 登录到某个服务器
     * @param server
     * @return
     */
    public boolean login(String server) {

        if (StringUtils.isNotBlank(username)) {
            Map<String, String> params = new HashMap<String, String>(2);
            params.put("username", username);
            String body = "password=" + password;
            String url = "http://" + server + contextPath + LOGIN_URL;

            if (server.contains(Constants.HTTP_PREFIX)) {
                url = server + contextPath + LOGIN_URL;
            }

            // 将参数携带在请求头上并发起 http请求
            HttpClient.HttpResult result = HttpClient.request(url, new ArrayList<String>(2),
                params, body, Charsets.UTF_8.name(), HttpMethod.POST);

            if (result.code != HttpURLConnection.HTTP_OK) {
                SECURITY_LOGGER.error("login failed: {}", JSON.toJSONString(result));
                return false;
            }

            // 登录成功时 获取accessToken
            JSONObject obj = JSON.parseObject(result.content);
            if (obj.containsKey(Constants.ACCESS_TOKEN)) {
                accessToken = obj.getString(Constants.ACCESS_TOKEN);
                tokenTtl = obj.getIntValue(Constants.TOKEN_TTL);
                tokenRefreshWindow = tokenTtl / 10;
            }
        }
        return true;
    }

    public String getAccessToken() {
        return accessToken;
    }
}
