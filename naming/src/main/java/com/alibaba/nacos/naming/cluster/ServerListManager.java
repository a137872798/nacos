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
package com.alibaba.nacos.naming.cluster;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.core.utils.SystemUtils;
import com.alibaba.nacos.naming.boot.RunningConfig;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.cluster.servers.ServerChangeListener;
import com.alibaba.nacos.naming.misc.*;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.nacos.core.utils.SystemUtils.*;

/**
 * The manager to globally refresh and operate server list.
 *
 * @author nkorange
 * @since 1.0.0
 * 管理当前集群下所有的节点 以及检测心跳
 */
@Component("serverListManager")
public class ServerListManager {

    private static final int STABLE_PERIOD = 60 * 1000;

    /**
     * 一个简单的bean对象 这里使用单例模式
     */
    @Autowired
    private SwitchDomain switchDomain;

    /**
     * 这里管理了所有监听器  (针对集群内server变化)
     */
    private List<ServerChangeListener> listeners = new ArrayList<>();

    /**
     * 所有服务实例
     */
    private List<Server> servers = new ArrayList<>();

    /**
     * 认定 健康的服务实例
     */
    private List<Server> healthyServers = new ArrayList<>();

    /**
     * 现在 key 只能是 unknown 以后可能会增加
     */
    private Map<String, List<Server>> distroConfig = new ConcurrentHashMap<>();

    /**
     * key  是 server ip + port  value 是最后一次心跳时间
     */
    private Map<String, Long> distroBeats = new ConcurrentHashMap<>(16);

    /**
     * 存放当前所有的  server.site
     */
    private Set<String> liveSites = new HashSet<>();

    /**
     * 代表本服务器属于哪个 site  默认是 unknown
     */
    private final static String LOCALHOST_SITE = UtilsAndCommons.UNKNOWN_SITE;

    private long lastHealthServerMillis = 0L;

    private boolean autoDisabledHealthCheck = false;

    private Synchronizer synchronizer = new ServerStatusSynchronizer();

    /**
     * 追加一个监听器
     * @param listener
     */
    public void listen(ServerChangeListener listener) {
        listeners.add(listener);
    }

    @PostConstruct
    public void init() {
        // GlobalExecutor 负责维护全局范围的定时器
        GlobalExecutor.registerServerListUpdater(new ServerListUpdater());   // 该对象会定时从 集群配置文件中重新读取当前所有服务器 并覆盖 servers 通知监听器
        // 检测集群内其他节点状态的
        GlobalExecutor.registerServerStatusReporter(new ServerStatusReporter(), 2000);
    }

    /**
     * 刷新当前服务列表
     * @return
     */
    private List<Server> refreshServerList() {

        List<Server> result = new ArrayList<>();

        // 是否单机模式
        if (STANDALONE_MODE) {
            Server server = new Server();
            server.setIp(NetUtils.getLocalAddress());
            server.setServePort(RunningConfig.getServerPort());
            result.add(server);
            // 将本机作为结果返回
            return result;
        }

        List<String> serverList = new ArrayList<>();
        try {
            // 从集群配置文件中读取当前有哪些服务器
            serverList = readClusterConf();
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("failed to get config: " + CLUSTER_CONF_FILE_PATH, e);
        }

        if (Loggers.SRV_LOG.isDebugEnabled()) {
            Loggers.SRV_LOG.debug("SERVER-LIST from cluster.conf: {}", result);
        }

        //use system env  如果配置中没有尝试从环境变量中获取
        if (CollectionUtils.isEmpty(serverList)) {
            serverList = SystemUtils.getIPsBySystemEnv(UtilsAndCommons.SELF_SERVICE_CLUSTER_ENV);
            if (Loggers.SRV_LOG.isDebugEnabled()) {
                Loggers.SRV_LOG.debug("SERVER-LIST from system variable: {}", result);
            }
        }

        // 将服务地址转换成服务实例后填充到列表
        if (CollectionUtils.isNotEmpty(serverList)) {

            for (int i = 0; i < serverList.size(); i++) {

                String ip;
                int port;
                String server = serverList.get(i);
                if (server.contains(UtilsAndCommons.IP_PORT_SPLITER)) {
                    ip = server.split(UtilsAndCommons.IP_PORT_SPLITER)[0];
                    port = Integer.parseInt(server.split(UtilsAndCommons.IP_PORT_SPLITER)[1]);
                } else {
                    ip = server;
                    port = RunningConfig.getServerPort();
                }

                Server member = new Server();
                member.setIp(ip);
                member.setServePort(port);
                result.add(member);
            }
        }

        return result;
    }

    /**
     * 通过 key 来寻找 server
     * @param s
     * @return
     */
    public boolean contains(String s) {
        for (Server server : servers) {
            if (server.getKey().equals(s)) {
                return true;
            }
        }
        return false;
    }

    public List<Server> getServers() {
        return servers;
    }

    public List<Server> getHealthyServers() {
        return healthyServers;
    }

    /**
     * 当集群内 服务器列表发生变化时
     */
    private void notifyListeners() {

        GlobalExecutor.notifyServerListChange(new Runnable() {
            @Override
            public void run() {
                for (ServerChangeListener listener : listeners) {
                    // 使用最新的 服务器列表来通知监听器
                    listener.onChangeServerList(servers);
                    listener.onChangeHealthyServerList(healthyServers);
                }
            }
        });
    }

    public Map<String, List<Server>> getDistroConfig() {
        return distroConfig;
    }

    /**
     * 代表收到某个服务节点的心跳信息
     * @param configInfo
     */
    public synchronized void onReceiveServerStatus(String configInfo) {

        Loggers.SRV_LOG.info("receive config info: {}", configInfo);

        String[] configs = configInfo.split("\r\n");
        if (configs.length == 0) {
            return;
        }

        List<Server> newHealthyList = new ArrayList<>();
        List<Server> tmpServerList = new ArrayList<>();

        for (String config : configs) {
            tmpServerList.clear();
            // site:ip:lastReportTime:weight
            String[] params = config.split("#");
            //  收到的配置应该包含4个部分
            if (params.length <= 3) {
                Loggers.SRV_LOG.warn("received malformed distro map data: {}", config);
                continue;
            }

            Server server = new Server();

            server.setSite(params[0]);
            server.setIp(params[1].split(UtilsAndCommons.IP_PORT_SPLITER)[0]);
            server.setServePort(Integer.parseInt(params[1].split(UtilsAndCommons.IP_PORT_SPLITER)[1]));
            server.setLastRefTime(Long.parseLong(params[2]));

            // key 就是 ip + port     不允许收到不存在 列表中的 server
            if (!contains(server.getKey())) {
                throw new IllegalArgumentException("server: " + server.getKey() + " is not in serverlist");
            }

            // 尝试更新心跳
            Long lastBeat = distroBeats.get(server.getKey());
            long now = System.currentTimeMillis();
            if (null != lastBeat) {
                // 如果收到心跳的时间太长  这时会更改成失活
                server.setAlive(now - lastBeat < switchDomain.getDistroServerExpiredMillis());
            }
            // 更新上次心跳时间
            distroBeats.put(server.getKey(), now);

            Date date = new Date(Long.parseLong(params[2]));
            server.setLastRefTimeStr(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date));

            // 设置服务器当前权重
            server.setWeight(params.length == 4 ? Integer.parseInt(params[3]) : 1);
            // 尝试将当前节点存储到 map 中
            List<Server> list = distroConfig.get(server.getSite());
            if (list == null || list.size() <= 0) {
                list = new ArrayList<>();
                list.add(server);
                distroConfig.put(server.getSite(), list);
            }

            for (Server s : list) {
                String serverId = s.getKey() + "_" + s.getSite();
                String newServerId = server.getKey() + "_" + server.getSite();

                // 就是为了打印一行日志
                if (serverId.equals(newServerId)) {
                    if (s.isAlive() != server.isAlive() || s.getWeight() != server.getWeight()) {
                        Loggers.SRV_LOG.warn("server beat out of date, current: {}, last: {}",
                            JSON.toJSONString(server), JSON.toJSONString(s));
                    }
                    tmpServerList.add(server);
                    continue;
                }
                tmpServerList.add(s);
            }

            if (!tmpServerList.contains(server)) {
                tmpServerList.add(server);
            }

            distroConfig.put(server.getSite(), tmpServerList);

            // -------------
        }
        liveSites.addAll(distroConfig.keySet());
    }

    /**
     * 剔除所有下线的server
     */
    public void clean() {
        // 先剔除 本机下线的server
        cleanInvalidServers();

        // 向其他节点发起请求 让他们剔除下线server  对应 ap
        for (Map.Entry<String, List<Server>> entry : distroConfig.entrySet()) {
            // 代表当前还存活的所有 server
            for (Server server : entry.getValue()) {
                //request other server to clean invalid servers
                if (!server.getKey().equals(NetUtils.localServer())) {
                    requestOtherServerCleanInvalidServers(server.getKey());
                }
            }

        }
    }

    public Set<String> getLiveSites() {
        return liveSites;
    }

    /**
     * 清理失活server
     */
    private void cleanInvalidServers() {
        for (Map.Entry<String, List<Server>> entry : distroConfig.entrySet()) {
            List<Server> currentServers = entry.getValue();
            if (null == currentServers) {
                distroConfig.remove(entry.getKey());
                continue;
            }

            currentServers.removeIf(server -> !server.isAlive());
        }
    }

    /**
     * 让其他节点 清理无线server
     * @param serverIP
     */
    private void requestOtherServerCleanInvalidServers(String serverIP) {
        Map<String, String> params = new HashMap<String, String>(1);

        // 这个参数是什么鬼 ???
        params.put("action", "without-diamond-clean");
        try {
            // 忽略失败情况
            NamingProxy.reqAPI("distroStatus", params, serverIP, false);
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("[DISTRO-STATUS-CLEAN] Failed to request to clean server status to " + serverIP, e);
        }
    }

    /**
     * 该对象用于定期更新集群内服务器列表
     */
    public class ServerListUpdater implements Runnable {

        @Override
        public void run() {
            try {
                // 定期从配置文件中读取当前集群所有服务器信息 也就是中途更改配置文件也能观察到变化
                List<Server> refreshedServers = refreshServerList();
                List<Server> oldServers = servers;

                // 忽略空的情况
                if (CollectionUtils.isEmpty(refreshedServers)) {
                    Loggers.RAFT.warn("refresh server list failed, ignore it.");
                    return;
                }

                boolean changed = false;

                // 找到新增的服务
                List<Server> newServers = (List<Server>) CollectionUtils.subtract(refreshedServers, oldServers);
                if (CollectionUtils.isNotEmpty(newServers)) {
                    servers.addAll(newServers);
                    changed = true;
                    Loggers.RAFT.info("server list is updated, new: {} servers: {}", newServers.size(), newServers);
                }

                // 找到被删除的服务
                List<Server> deadServers = (List<Server>) CollectionUtils.subtract(oldServers, refreshedServers);
                if (CollectionUtils.isNotEmpty(deadServers)) {
                    servers.removeAll(deadServers);
                    changed = true;
                    Loggers.RAFT.info("server list is updated, dead: {}, servers: {}", deadServers.size(), deadServers);
                }

                if (changed) {
                    // 发生变化的话 通知监听器
                    notifyListeners();
                }

            } catch (Exception e) {
                Loggers.RAFT.info("error while updating server list.", e);
            }
        }
    }

    /**
     * 检测集群内其他节点能否正常工作
     */
    private class ServerStatusReporter implements Runnable {

        @Override
        public void run() {
            try {

                // 本次的端口要是无效 就忽略
                if (RunningConfig.getServerPort() <= 0) {
                    return;
                }

                // 检测当前健康实例是否发生了变化 是的话触发监听器
                checkDistroHeartbeat();

                // 核数越多优先级越高???
                int weight = Runtime.getRuntime().availableProcessors() / 2;
                if (weight <= 0) {
                    weight = 1;
                }

                long curTime = System.currentTimeMillis();
                String status = LOCALHOST_SITE + "#" + NetUtils.localServer() + "#" + curTime + "#" + weight + "\r\n";

                // 下面开始检测每个节点能否正常连接

                //send status to itself   先通知自身
                onReceiveServerStatus(status);

                List<Server> allServers = getServers();

                if (!contains(NetUtils.localServer())) {
                    Loggers.SRV_LOG.error("local ip is not in serverlist, ip: {}, serverlist: {}", NetUtils.localServer(), allServers);
                    return;
                }

                if (allServers.size() > 0 && !NetUtils.localServer().contains(UtilsAndCommons.LOCAL_HOST_IP)) {
                    // 将自身信息同步到其他节点  实际上就是发送心跳包
                    for (com.alibaba.nacos.naming.cluster.servers.Server server : allServers) {
                        if (server.getKey().equals(NetUtils.localServer())) {
                            continue;
                        }

                        Message msg = new Message();
                        msg.setData(status);

                        synchronizer.send(server.getKey(), msg);

                    }
                }
            } catch (Exception e) {
                Loggers.SRV_LOG.error("[SERVER-STATUS] Exception while sending server status", e);
            } finally {
                // 设置下次任务时间
                GlobalExecutor.registerServerStatusReporter(this, switchDomain.getServerStatusSynchronizationPeriodMillis());
            }

        }
    }

    /**
     * 检查心跳信息
     */
    private void checkDistroHeartbeat() {

        Loggers.SRV_LOG.debug("check distro heartbeat.");

        // 此时应该所有服务的 site 都是 LOCALHOST_SITE  可能以后会增加
        List<Server> servers = distroConfig.get(LOCALHOST_SITE);
        if (CollectionUtils.isEmpty(servers)) {
            return;
        }

        List<Server> newHealthyList = new ArrayList<>(servers.size());
        long now = System.currentTimeMillis();
        for (Server s: servers) {
            // 找到这些服务器对应的心跳时间
            Long lastBeat = distroBeats.get(s.getKey());
            if (null == lastBeat) {
                continue;
            }
            // 该方法会轮询 一旦发现某个节点长时间没收到心跳 更新成失活
            s.setAlive(now - lastBeat < switchDomain.getDistroServerExpiredMillis());
        }

        //local site servers
        List<String> allLocalSiteSrvs = new ArrayList<>();
        for (Server server : servers) {

            // 跳过端口号为0 的节点
            if (server.getKey().endsWith(":0")) {
                continue;
            }

            // 这里将 额外权重从 switchDomain 转移到服务实例内
            server.setAdWeight(switchDomain.getAdWeight(server.getKey()) == null ? 0 : switchDomain.getAdWeight(server.getKey()));

            for (int i = 0; i < server.getWeight() + server.getAdWeight(); i++) {

                if (!allLocalSiteSrvs.contains(server.getKey())) {

                    allLocalSiteSrvs.add(server.getKey());
                }

                // 这里存放一个 可用的 server
                if (server.isAlive() && !newHealthyList.contains(server)) {
                    newHealthyList.add(server);
                }
            }
        }

        Collections.sort(newHealthyList);
        // 当前可用服务的比率
        float curRatio = (float) newHealthyList.size() / allLocalSiteSrvs.size();

        // 当可用率低于某个值  且一定时间没执行心跳检测 且开启了心跳检测
        if (autoDisabledHealthCheck
            && curRatio > switchDomain.getDistroThreshold()
            && System.currentTimeMillis() - lastHealthServerMillis > STABLE_PERIOD) {
            Loggers.SRV_LOG.info("[NACOS-DISTRO] distro threshold restored and " +
                "stable now, enable health check. current ratio: {}", curRatio);

            // 开启心跳检测
            switchDomain.setHealthCheckEnabled(true);

            // we must set this variable, otherwise it will conflict with user's action
            autoDisabledHealthCheck = false;
        }

        // 代表当前健康的 server 发生了变化  就不能再进行心跳检测了
        if (!CollectionUtils.isEqualCollection(healthyServers, newHealthyList)) {
            // for every change disable healthy check for some while
            Loggers.SRV_LOG.info("[NACOS-DISTRO] healthy server list changed, old: {}, new: {}",
                healthyServers, newHealthyList);
            // 关闭健康检查
            if (switchDomain.isHealthCheckEnabled() && switchDomain.isAutoChangeHealthCheckEnabled()) {
                Loggers.SRV_LOG.info("[NACOS-DISTRO] disable health check for {} ms from now on.", STABLE_PERIOD);

                switchDomain.setHealthCheckEnabled(false);
                autoDisabledHealthCheck = true;

                lastHealthServerMillis = System.currentTimeMillis();
            }

            // 更新当前健康实例 并通知监听器
            healthyServers = newHealthyList;
            notifyListeners();
        }
    }
}
