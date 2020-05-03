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
package com.alibaba.nacos.config.server.service;

import com.alibaba.nacos.common.utils.IoUtils;
import com.alibaba.nacos.config.server.constant.Constants;
import com.alibaba.nacos.config.server.monitor.MetricsMonitor;
import com.alibaba.nacos.config.server.service.notify.NotifyService;
import com.alibaba.nacos.config.server.service.notify.NotifyService.HttpResult;
import com.alibaba.nacos.config.server.utils.PropertyUtil;
import com.alibaba.nacos.config.server.utils.RunningConfigUtils;
import com.alibaba.nacos.config.server.utils.event.EventDispatcher;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.context.WebServerInitializedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.alibaba.nacos.config.server.utils.LogUtil.defaultLog;
import static com.alibaba.nacos.config.server.utils.LogUtil.fatalLog;
import static com.alibaba.nacos.core.utils.SystemUtils.*;

/**
 * Serverlist service
 *
 * @author Nacos
 * 该对象有2个任务 一个是监听当前集群下所有的服务器节点
 * 还有一个是确保这些节点是可用的
 * 而从client的角度来看就没有这么多问题 只要一个服务器列表 挨个访问 直到成功就可以了
 * 而 服务器集群为了保证数据的一致性 需要确保其他服务器能正常访问
 */
@Service
public class ServerListService implements ApplicationListener<WebServerInitializedEvent> {


    private final ServletContext servletContext;

    @Value("${server.port:8848}")
    private int port;

    @Value("${useAddressServer}")
    private Boolean isUseAddressServer = true;

    public ServerListService(ServletContext servletContext) {
        this.servletContext = servletContext;
    }

    @PostConstruct
    public void init() {
        // 获取地址服务的url
        String envDomainName = System.getenv("address_server_domain");
        if (StringUtils.isBlank(envDomainName)) {
            domainName = System.getProperty("address.server.domain", "jmenv.tbsite.net");
        } else {
            domainName = envDomainName;
        }
        // 获取当前节点的port信息
        String envAddressPort = System.getenv("address_server_port");
        if (StringUtils.isBlank(envAddressPort)) {
            addressPort = System.getProperty("address.server.port", "8080");
        } else {
            addressPort = envAddressPort;
        }
        // 通过指定集群名称的方式 获取某个集群下所有服务器
        addressUrl = System.getProperty("address.server.url",
            servletContext.getContextPath() + "/" + RunningConfigUtils.getClusterName());
        // 拼接完整的信息
        addressServerUrl = "http://" + domainName + ":" + addressPort + addressUrl;
        // 生成环境url
        envIdUrl = "http://" + domainName + ":" + addressPort + "/env";

        defaultLog.info("ServerListService address-server port:" + addressPort);
        defaultLog.info("ADDRESS_SERVER_URL:" + addressServerUrl);
        // 是否需要健康检查  默认为true
        isHealthCheck = PropertyUtil.isHealthCheck();
        // 获取健康检查最大失败次数
        maxFailCount = PropertyUtil.getMaxHealthCheckFailCount();
        fatalLog.warn("useAddressServer:{}", isUseAddressServer);
        // 定期更新集群内节点
        GetServerListTask task = new GetServerListTask();
        task.run();
        if (CollectionUtils.isEmpty(serverList)) {
            fatalLog.error("########## cannot get serverlist, so exit.");
            throw new RuntimeException("cannot get serverlist, so exit.");
        } else {
            TimerTaskService.scheduleWithFixedDelay(task, 0L, 5L, TimeUnit.SECONDS);
        }

    }



    public List<String> getServerList() {
        return new ArrayList<String>(serverList);
    }

    public static void setServerList(List<String> serverList) {
        ServerListService.serverList = serverList;
    }

    public static List<String> getServerListUnhealth() {
        return new ArrayList<String>(serverListUnhealth);
    }

    public static Boolean isFirstIp() {
        return serverList.get(0).contains(LOCAL_IP);
    }

    public boolean isHealthCheck() {
        return isHealthCheck;
    }

    /**
     * serverList has changed
     */
    static public class ServerListChangeEvent implements EventDispatcher.Event {
    }

    /**
     * 该对象通过一个定时任务 定期访问 地址服务器 或者定时读取配置文件 读取当下集群所有server
     * @param newList
     */
    private void updateIfChanged(List<String> newList) {
        // 代表与上次相比没有变化
        if (CollectionUtils.isEmpty(newList)||newList.equals(serverList)) {
            return;
        }

        boolean isContainSelfIp = newList.stream().anyMatch(ipPortTmp -> ipPortTmp.contains(LOCAL_IP));

        if (isContainSelfIp) {
            isInIpList = true;
        } else {
            // 强制追加本节点到集群中
            isInIpList = false;
            String selfAddr = getFormatServerAddr(LOCAL_IP);
            newList.add(selfAddr);
            fatalLog.error("########## [serverlist] self ip {} not in serverlist {}", selfAddr, newList);
        }

        serverList = new ArrayList<String>(newList);

        // 如果某些节点已经不包含在集群内了 那么就从 unhealth中移除
        if(!serverListUnhealth.isEmpty()){

            List<String> unhealthyRemoved = serverListUnhealth.stream()
                .filter(unhealthyIp -> !newList.contains(unhealthyIp)).collect(Collectors.toList());

            serverListUnhealth.removeAll(unhealthyRemoved);

            List<String> unhealthyCountRemoved = serverIp2unhealthCount.keySet().stream()
                .filter(key -> !newList.contains(key)).collect(Collectors.toList());

            for (String unhealthyCountTmp : unhealthyCountRemoved) {
                serverIp2unhealthCount.remove(unhealthyCountTmp);
            }


        }

        defaultLog.warn("[serverlist] updated to {}", serverList);

        /**
         * 非并发fireEvent
         * 触发订阅 ChangeEvent的监听器   框架内部没有监听该事件的组件 由用户进行拓展
         */
        EventDispatcher.fireEvent(new ServerListChangeEvent());
    }

    /**
     * 保证不返回NULL
     * 读取当前集群下所有节点地址
     *
     * @return serverlist
     */
    private List<String> getApacheServerList() {
        // 如果是单机模式 只需要返回本节点ip
        if (STANDALONE_MODE) {
            List<String> serverIps = new ArrayList<String>();
            serverIps.add(getFormatServerAddr(LOCAL_IP));
            return serverIps;
        }

        // 优先从文件读取服务列表
        try {
            List<String> serverIps = new ArrayList<String>();
            // 读取集群配置文件     jraft 好像支持动态扩容   也就是为所有节点追加一个 server地址信息 同样要满足写入超过半数
            List<String> serverAddrLines = readClusterConf();

            if (!CollectionUtils.isEmpty(serverAddrLines)) {
                for (String serverAddr : serverAddrLines) {
                    if (StringUtils.isNotBlank(serverAddr.trim())) {
                        serverIps.add(getFormatServerAddr(serverAddr));
                    }
                }
            }
            if (serverIps.size() > 0) {
                return serverIps;
            }
        } catch (Exception e) {
            defaultLog.error("nacos-XXXX", "[serverlist] failed to get serverlist from disk!", e);
        }

        // 如果不是从配置文件中读取 而是选择访问某个专用的地址服务器 (比如该服务器就是提供拉取集群地址的)
        if (isUseAddressServer()) {
            try {
                HttpResult result = NotifyService.invokeURL(addressServerUrl, null, null);

                if (HttpServletResponse.SC_OK == result.code) {
                    isAddressServerHealth = true;
                    addressServerFailCount = 0;
                    // 读取结果并转换成地址信息
                    List<String> lines = IoUtils.readLines(new StringReader(result.content));
                    List<String> ips = new ArrayList<String>(lines.size());
                    for (String serverAddr : lines) {
                        if (StringUtils.isNotBlank(serverAddr)) {
                            ips.add(getFormatServerAddr(serverAddr));
                        }
                    }
                    return ips;
                } else {
                    addressServerFailCount++;
                    if (addressServerFailCount >= maxFailCount) {
                        isAddressServerHealth = false;
                    }
                    defaultLog.error("[serverlist] failed to get serverlist, error code {}", result.code);
                    return Collections.emptyList();
                }
            } catch (IOException e) {
                addressServerFailCount++;
                if (addressServerFailCount >= maxFailCount) {
                    isAddressServerHealth = false;
                }
                defaultLog.error("[serverlist] exception, " + e.toString(), e);
                return Collections.emptyList();
            }

        } else {
            List<String> serverIps = new ArrayList<String>();
            serverIps.add(getFormatServerAddr(LOCAL_IP));
            return serverIps;
        }
    }

    /**
     * 将配置文件中的服务器地址 格式化成 ip + port 的形式
     * @param serverAddr
     * @return
     */
    private String getFormatServerAddr(String serverAddr) {
        if (StringUtils.isBlank(serverAddr)) {
            throw new IllegalArgumentException("invalid serverlist");
        }
        String[] ipPort = serverAddr.trim().split(":");
        String ip = ipPort[0].trim();
        if (ipPort.length == 1 && port != 0) {
            return (ip + ":" + port);
        } else {
            return serverAddr;
        }
    }



    class GetServerListTask implements Runnable {
        @Override
        public void run() {
            try {
                updateIfChanged(getApacheServerList());
            } catch (Exception e) {
                defaultLog.error("[serverlist] failed to get serverlist, " + e.toString(), e);
            }
        }
    }

    /**
     * 检测集群内其他节点能否正常访问
     */
    private void checkServerHealth() {
        long startCheckTime = System.currentTimeMillis();
        for (String serverIp : serverList) {
            // Compatible with old codes,use status.taobao
            String url = "http://" + serverIp + servletContext.getContextPath() + Constants.HEALTH_CONTROLLER_PATH;
            // "/nacos/health";
            HttpGet request = new HttpGet(url);
            httpclient.execute(request, new AsyncCheckServerHealthCallBack(serverIp));
        }
        long endCheckTime = System.currentTimeMillis();
        long cost = endCheckTime - startCheckTime;
        defaultLog.debug("checkServerHealth cost: {}", cost);
    }

    /**
     * 处理其他节点健康状态的回调
     */
    class AsyncCheckServerHealthCallBack implements FutureCallback<HttpResponse> {

        private String serverIp;

        public AsyncCheckServerHealthCallBack(String serverIp) {
            this.serverIp = serverIp;
        }

        @Override
        public void completed(HttpResponse response) {
            // 代表可用 从 unhealth列表中移除  看来能正常访问到就认为是健康实例  及时对应的 database不可用
            if (response.getStatusLine().getStatusCode() == HttpServletResponse.SC_OK) {
                serverListUnhealth.remove(serverIp);
                // 这是 apache相关的清理 先不管
                HttpClientUtils.closeQuietly(response);
            }
        }

        @Override
        public void failed(Exception ex) {
            computeFailCount();
        }

        @Override
        public void cancelled() {
            computeFailCount();
                }

        private void computeFailCount() {
            int failCount = serverIp2unhealthCount.compute(serverIp,(key,oldValue)->oldValue == null?1:oldValue+1);
            if (failCount > maxFailCount) {
                // 超过最大次数后加入不可用名单
                if (!serverListUnhealth.contains(serverIp)) {
                    serverListUnhealth.add(serverIp);
                }
                defaultLog.error("unhealthIp:{}, unhealthCount:{}", serverIp, failCount);
                MetricsMonitor.getUnhealthException().increment();
            }
        }
    }

    class CheckServerHealthTask implements Runnable {

        @Override
        public void run() {
            checkServerHealth();
        }

    }

    private Boolean isUseAddressServer() {
        return isUseAddressServer;
    }

    static class CheckServerThreadFactory implements ThreadFactory {

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, "com.alibaba.nacos.CheckServerThreadFactory");
            thread.setDaemon(true);
            return thread;
        }
    }

    public static boolean isAddressServerHealth() {
        return isAddressServerHealth;
    }

    public static boolean isInIpList() {
        return isInIpList;
    }

    // ==========================

    /**
     * 和其他server的连接超时和socket超时
     */
    static final int TIMEOUT = 5000;
    private int maxFailCount = 12;
    private static volatile List<String> serverList = new ArrayList<String>();
    /**
     * 当前无法被访问的服务实例
     */
    private static volatile List<String> serverListUnhealth = Collections.synchronizedList(new ArrayList<String>());
    /**
     * 标记当前集群服务是否可用
     */
    private static volatile boolean isAddressServerHealth = true;
    private static volatile int addressServerFailCount = 0;
    private static volatile boolean isInIpList = true;

    /**
     * ip unhealth count
     * 记录某个serviceIp 的失败次数
     */
    private static  Map<String, Integer> serverIp2unhealthCount = new ConcurrentHashMap<>();
    private RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(PropertyUtil.getNotifyConnectTimeout())
        .setSocketTimeout(PropertyUtil.getNotifySocketTimeout()).build();

    private CloseableHttpAsyncClient httpclient = HttpAsyncClients.custom().setDefaultRequestConfig(requestConfig)
        .build();


    public String domainName;
    public String addressPort;
    public String addressUrl;
    public String envIdUrl;
    /**
     * 地址服务器的  url
     */
    public String addressServerUrl;
    private boolean isHealthCheck = true;

    /**
     * 当服务器启动时 开始检测其他server能否正常访问了
     * @param event
     */
    @Override
    public void onApplicationEvent(WebServerInitializedEvent event) {
        if (port == 0) {
            port = event.getWebServer().getPort();
            List<String> newList = new ArrayList<String>();
            for (String serverAddrTmp : serverList) {
                newList.add(getFormatServerAddr(serverAddrTmp));
            }
            setServerList(new ArrayList<String>(newList));
        }
        // 启动apache.httpClient
        httpclient.start();
        // 定时触发心跳任务
        CheckServerHealthTask checkServerHealthTask = new CheckServerHealthTask();
        TimerTaskService.scheduleWithFixedDelay(checkServerHealthTask, 0L, 5L, TimeUnit.SECONDS);

    }

}
