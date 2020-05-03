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
package com.alibaba.nacos.naming.push;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.pojo.Subscriber;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.util.VersionUtil;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

/**
 * @author nacos
 * 该对象负责监听服务实例的变化  并将信息通知到 nacos-naming client
 */
@Component
public class PushService implements ApplicationContextAware, ApplicationListener<ServiceChangeEvent> {

    @Autowired
    private SwitchDomain switchDomain;

    private ApplicationContext applicationContext;

    private static final long ACK_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(10L);

    private static final int MAX_RETRY_TIMES = 1;

    /**
     * string描述了目标client的地址信息以及本次心跳包的发起时间戳  value 代表从client收到的心跳包信息
     */
    private static volatile ConcurrentMap<String, Receiver.AckEntry> ackMap
        = new ConcurrentHashMap<String, Receiver.AckEntry>();

    /**
     * key1  通过service 进行定位  key2 通过cluster进行定位  value 负责与某个集群下所有instance 通信
     */
    private static ConcurrentMap<String, ConcurrentMap<String, PushClient>> clientMap
        = new ConcurrentHashMap<String, ConcurrentMap<String, PushClient>>();

    /**
     * 记录某个心跳包发送的时间戳
     */
    private static volatile ConcurrentHashMap<String, Long> udpSendTimeMap = new ConcurrentHashMap<String, Long>();

    /**
     * 记录某个心跳包从推送到接收 过了多少时间
     */
    public static volatile ConcurrentHashMap<String, Long> pushCostMap = new ConcurrentHashMap<String, Long>();

    /**
     * 成功推送总数
     */
    private static int totalPush = 0;

    /**
     * 失败推送总数
     */
    private static int failedPush = 0;

    /**
     * 上一次推送的时间戳
     */
    private static ConcurrentHashMap<String, Long> lastPushMillisMap = new ConcurrentHashMap<>();

    // UDP 套接字
    private static DatagramSocket udpSocket;

    // 存放待执行的任务 主要是去重用的 避免某个 service  触发了多次事件
    private static Map<String, Future> futureMap = new ConcurrentHashMap<>();
    private static ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("com.alibaba.nacos.naming.push.retransmitter");
            return t;
        }
    });

    private static ScheduledExecutorService udpSender = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("com.alibaba.nacos.naming.push.udpSender");
            return t;
        }
    });

    static {
        try {
            // 创建 udp  需要明确一点  发送udp 数据包 需要指定对端的地址
            udpSocket = new DatagramSocket();

            // 创建接收者  该对象负责监听发送到本对象的所有数据包
            Receiver receiver = new Receiver();

            Thread inThread = new Thread(receiver);
            inThread.setDaemon(true);
            inThread.setName("com.alibaba.nacos.naming.push.receiver");
            inThread.start();

            executorService.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    try {
                        // 移除僵尸客户端  也就是将无响应的客户端移除
                        removeClientIfZombie();
                    } catch (Throwable e) {
                        Loggers.PUSH.warn("[NACOS-PUSH] failed to remove client zombie");
                    }
                }
            }, 0, 20, TimeUnit.SECONDS);

        } catch (SocketException e) {
            Loggers.SRV_LOG.error("[NACOS-PUSH] failed to init push service");
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    /**
     * 将变化信息通过udp 通知到client
     * @param event
     */
    @Override
    public void onApplicationEvent(ServiceChangeEvent event) {
        Service service = event.getService();
        String serviceName = service.getName();
        String namespaceId = service.getNamespaceId();

        // 当检测到 serviceChangeEvent 时   延迟1秒触发该方法
        Future future = udpSender.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    Loggers.PUSH.info(serviceName + " is changed, add it to push queue.");
                    // 如果client 并没有订阅就不需要通知
                    ConcurrentMap<String, PushClient> clients = clientMap.get(UtilsAndCommons.assembleFullServiceName(namespaceId, serviceName));
                    if (MapUtils.isEmpty(clients)) {
                        return;
                    }

                    // key 是压缩数据 value 是原始数据
                    Map<String, Object> cache = new HashMap<>(16);
                    long lastRefTime = System.nanoTime();

                    // 下面这段就是为每个 client 对象生成对应的ack 包
                    for (PushClient client : clients.values()) {
                        // 检测客户端是否长时间没有收到心跳 避免无谓的网络IO开销
                        if (client.zombie()) {
                            Loggers.PUSH.debug("client is zombie: " + client.toString());
                            clients.remove(client.toString());
                            Loggers.PUSH.debug("client is zombie: " + client.toString());
                            continue;
                        }

                        // ack 信息
                        Receiver.AckEntry ackEntry;
                        Loggers.PUSH.debug("push serviceName: {} to client: {}", serviceName, client.toString());
                        // 将服务名和代理信息生成key
                        String key = getPushCacheKey(serviceName, client.getIp(), client.getAgent());
                        byte[] compressData = null;
                        Map<String, Object> data = null;
                        // 通过缓存来减少对象的创建和销毁
                        // 实际上该key只使用到了 serviceName 和 agent
                        if (switchDomain.getDefaultPushCacheMillis() >= 20000 && cache.containsKey(key)) {
                            org.javatuples.Pair pair = (org.javatuples.Pair) cache.get(key);
                            // 第一个代表压缩数据 第二个代表原始数据
                            compressData = (byte[]) (pair.getValue0());
                            data = (Map<String, Object>) pair.getValue1();

                            Loggers.PUSH.debug("[PUSH-CACHE] cache hit: {}:{}", serviceName, client.getAddrStr());
                        }

                        if (compressData != null) {
                            // 就是使用client 信息 以及数据体 包装成一个 ackEntry
                            ackEntry = prepareAckEntry(client, compressData, data, lastRefTime);
                        } else {
                            // 这里从 client 连接的 dataSource 生成数据  之后将client信息和data包装成 ack实体
                            ackEntry = prepareAckEntry(client, prepareHostsData(client), lastRefTime);
                            if (ackEntry != null) {
                                // origin.getData() 代表压缩数据   ack.data 代表原始数据  将数据存储到缓存中
                                cache.put(key, new org.javatuples.Pair<>(ackEntry.origin.getData(), ackEntry.data));
                            }
                        }

                        Loggers.PUSH.info("serviceName: {} changed, schedule push for: {}, agent: {}, key: {}",
                            client.getServiceName(), client.getAddrStr(), client.getAgent(), (ackEntry == null ? null : ackEntry.key));

                        // 使用udp发送心跳包
                        udpPush(ackEntry);
                    }
                } catch (Exception e) {
                    Loggers.PUSH.error("[NACOS-PUSH] failed to push serviceName: {} to client, error: {}", serviceName, e);

                } finally {
                    futureMap.remove(UtilsAndCommons.assembleFullServiceName(namespaceId, serviceName));
                }

            }
        }, 1000, TimeUnit.MILLISECONDS);

        // 将future 存到 map中
        futureMap.put(UtilsAndCommons.assembleFullServiceName(namespaceId, serviceName), future);

    }

    public int getTotalPush() {
        return totalPush;
    }

    public void setTotalPush(int totalPush) {
        PushService.totalPush = totalPush;
    }

    /**
     * 增加一个连接到某个节点的client
     * @param namespaceId
     * @param serviceName
     * @param clusters
     * @param agent
     * @param socketAddr
     * @param dataSource
     * @param tenant
     * @param app
     */
    public void addClient(String namespaceId,
                          String serviceName,
                          String clusters,
                          String agent,
                          InetSocketAddress socketAddr,
                          DataSource dataSource,
                          String tenant,
                          String app) {

        PushClient client = new PushClient(namespaceId,
            serviceName,
            clusters,
            agent,
            socketAddr,
            dataSource,
            tenant,
            app);
        addClient(client);
    }

    /**
     * 添加映射关系
     * @param client
     */
    public void addClient(PushClient client) {
        // client is stored by key 'serviceName' because notify event is driven by serviceName change
        String serviceKey = UtilsAndCommons.assembleFullServiceName(client.getNamespaceId(), client.getServiceName());
        ConcurrentMap<String, PushClient> clients =
            clientMap.get(serviceKey);
        if (clients == null) {
            clientMap.putIfAbsent(serviceKey, new ConcurrentHashMap<String, PushClient>(1024));
            clients = clientMap.get(serviceKey);
        }

        PushClient oldClient = clients.get(client.toString());
        if (oldClient != null) {
            // 如果之前存在客户端信息 进行刷新  也就是更新 lastRef
            oldClient.refresh();
        } else {
            PushClient res = clients.putIfAbsent(client.toString(), client);
            if (res != null) {
                Loggers.PUSH.warn("client: {} already associated with key {}", res.getAddrStr(), res.toString());
            }
            Loggers.PUSH.debug("client: {} added for serviceName: {}", client.getAddrStr(), client.getServiceName());
        }
    }

    /**
     * 这里将某个服务下每个client 看作一个 订阅者
     * @param serviceName
     * @param namespaceId
     * @return
     */
    public List<Subscriber> getClients(String serviceName, String namespaceId) {
        String serviceKey = UtilsAndCommons.assembleFullServiceName(namespaceId, serviceName);
        ConcurrentMap<String, PushClient> clientConcurrentMap = clientMap.get(serviceKey);
        if (Objects.isNull(clientConcurrentMap)) {
            return null;
        }
        List<Subscriber> clients = new ArrayList<Subscriber>();
        clientConcurrentMap.forEach((key, client) -> {
            clients.add(new Subscriber(client.getAddrStr(), client.getAgent(), client.getApp(), client.getIp(), namespaceId, serviceName));
        });
        return clients;
    }

    /**
     *  fuzzy search subscriber
     * @param serviceName
     * @param namespaceId
     * @return
     * 模糊寻找client
     */
    public List<Subscriber> getClientsFuzzy(String serviceName, String namespaceId) {
        List<Subscriber> clients = new ArrayList<Subscriber>();
        clientMap.forEach((outKey, clientConcurrentMap) -> {
            //get groupedName from key
            String serviceFullName = outKey.split(UtilsAndCommons.NAMESPACE_SERVICE_CONNECTOR)[1];
            //get groupName
            String groupName = NamingUtils.getGroupName(serviceFullName);
            //get serviceName
            String name = NamingUtils.getServiceName(serviceFullName);
            //fuzzy match
            if (outKey.startsWith(namespaceId) && name.indexOf(NamingUtils.getServiceName(serviceName)) >= 0 && groupName.indexOf(NamingUtils.getGroupName(serviceName)) >= 0) {
                clientConcurrentMap.forEach((key, client) -> {
                    clients.add(new Subscriber(client.getAddrStr(), client.getAgent(), client.getApp(), client.getIp(), namespaceId, serviceFullName));
                });
            }
        });
        return clients;
    }

    /**
     * 定期移除掉无响应的 nacos-naming client
     */
    public static void removeClientIfZombie() {

        int size = 0;
        for (Map.Entry<String, ConcurrentMap<String, PushClient>> entry : clientMap.entrySet()) {
            ConcurrentMap<String, PushClient> clientConcurrentMap = entry.getValue();
            for (Map.Entry<String, PushClient> entry1 : clientConcurrentMap.entrySet()) {
                PushClient client = entry1.getValue();
                if (client.zombie()) {
                    clientConcurrentMap.remove(entry1.getKey());
                }
            }

            size += clientConcurrentMap.size();
        }

        if (Loggers.PUSH.isDebugEnabled()) {
            Loggers.PUSH.debug("[NACOS-PUSH] clientMap size: {}", size);
        }

    }

    /**
     * 生成ack 信息
     * @param client   需要检测心跳的目标客户端
     * @param dataBytes    压缩数据
     * @param data     未压缩数据
     * @param lastRefTime     当前时间 或者说应该为client设置的最后收到心跳包的时间
     * @return
     */
    private static Receiver.AckEntry prepareAckEntry(PushClient client, byte[] dataBytes, Map<String, Object> data,
                                                     long lastRefTime) {
        // 将 ip port 和 lastRefTime 拼接成一个特殊的key
        String key = getACKKey(client.getSocketAddr().getAddress().getHostAddress(),
            client.getSocketAddr().getPort(),
            lastRefTime);
        DatagramPacket packet = null;
        try {
            // 生成数据包对象 包含了压缩数据  数据长度 和 客户端地址
            packet = new DatagramPacket(dataBytes, dataBytes.length, client.socketAddr);
            Receiver.AckEntry ackEntry = new Receiver.AckEntry(key, packet);
            // we must store the key be fore send, otherwise there will be a chance the
            // ack returns before we put in
            // 设置data 信息
            ackEntry.data = data;

            return ackEntry;
        } catch (Exception e) {
            Loggers.PUSH.error("[NACOS-PUSH] failed to prepare data: {} to client: {}, error: {}",
                data, client.getSocketAddr(), e);
        }

        return null;
    }

    public static String getPushCacheKey(String serviceName, String clientIP, String agent) {
        return serviceName + UtilsAndCommons.CACHE_KEY_SPLITER + agent;
    }

    /**
     * 当感知到 service 发生变化时  发布一个事件对象
     * @param service
     */
    public void serviceChanged(Service service) {
        // merge some change events to reduce the push frequency:
        // 代表针对当前 service 还有任务未执行  用于去重
        if (futureMap.containsKey(UtilsAndCommons.assembleFullServiceName(service.getNamespaceId(), service.getName()))) {
            return;
        }

        this.applicationContext.publishEvent(new ServiceChangeEvent(this, service));
    }

    public boolean canEnablePush(String agent) {

        if (!switchDomain.isPushEnabled()) {
            return false;
        }

        // 代表对端使用了什么语言
        ClientInfo clientInfo = new ClientInfo(agent);

        if (ClientInfo.ClientType.JAVA == clientInfo.type
            && clientInfo.version.compareTo(VersionUtil.parseVersion(switchDomain.getPushJavaVersion())) >= 0) {
            return true;
        } else if (ClientInfo.ClientType.DNS == clientInfo.type
            && clientInfo.version.compareTo(VersionUtil.parseVersion(switchDomain.getPushPythonVersion())) >= 0) {
            return true;
        } else if (ClientInfo.ClientType.C == clientInfo.type
            && clientInfo.version.compareTo(VersionUtil.parseVersion(switchDomain.getPushCVersion())) >= 0) {
            return true;
        } else if (ClientInfo.ClientType.GO == clientInfo.type
            && clientInfo.version.compareTo(VersionUtil.parseVersion(switchDomain.getPushGoVersion())) >= 0) {
            return true;
        }

        return false;
    }

    public static List<Receiver.AckEntry> getFailedPushes() {
        return new ArrayList<Receiver.AckEntry>(ackMap.values());
    }

    public int getFailedPushCount() {
        return ackMap.size() + failedPush;
    }

    public void setFailedPush(int failedPush) {
        PushService.failedPush = failedPush;
    }


    public static void resetPushState() {
        ackMap.clear();
    }

    /**
     * 代表会连接到某个节点用于发送心跳包
     */
    public class PushClient {
        // 节点本身的描述信息
        private String namespaceId;
        private String serviceName;
        private String clusters;
        private String agent;
        private String tenant;
        private String app;
        private InetSocketAddress socketAddr;
        /**
         * 该对象负责提供 通知到 client的数据
         */
        private DataSource dataSource;
        /**
         * 这里可以指定额外的参数
         */
        private Map<String, String[]> params;

        public Map<String, String[]> getParams() {
            return params;
        }

        public void setParams(Map<String, String[]> params) {
            this.params = params;
        }

        public long lastRefTime = System.currentTimeMillis();

        public PushClient(String namespaceId,
                          String serviceName,
                          String clusters,
                          String agent,
                          InetSocketAddress socketAddr,
                          DataSource dataSource,
                          String tenant,
                          String app) {
            this.namespaceId = namespaceId;
            this.serviceName = serviceName;
            this.clusters = clusters;
            this.agent = agent;
            this.socketAddr = socketAddr;
            this.dataSource = dataSource;
            this.tenant = tenant;
            this.app = app;
        }

        public DataSource getDataSource() {
            return dataSource;
        }

        /**
         * 代表长时间没有收到心跳包了   间隔时间默认是10秒
         * @return
         */
        public boolean zombie() {
            return System.currentTimeMillis() - lastRefTime > switchDomain.getPushCacheMillis(serviceName);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("serviceName: ").append(serviceName)
                .append(", clusters: ").append(clusters)
                .append(", address: ").append(socketAddr)
                .append(", agent: ").append(agent);
            return sb.toString();
        }

        public String getAgent() {
            return agent;
        }

        public String getAddrStr() {
            return socketAddr.getAddress().getHostAddress() + ":" + socketAddr.getPort();
        }

        public String getIp() {
            return socketAddr.getAddress().getHostAddress();
        }

        @Override
        public int hashCode() {
            return Objects.hash(serviceName, clusters, socketAddr);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof PushClient)) {
                return false;
            }

            PushClient other = (PushClient) obj;

            return serviceName.equals(other.serviceName) && clusters.equals(other.clusters) && socketAddr.equals(other.socketAddr);
        }

        public String getClusters() {
            return clusters;
        }

        public void setClusters(String clusters) {
            this.clusters = clusters;
        }

        public String getNamespaceId() {
            return namespaceId;
        }

        public void setNamespaceId(String namespaceId) {
            this.namespaceId = namespaceId;
        }

        public String getServiceName() {
            return serviceName;
        }

        public void setServiceName(String serviceName) {
            this.serviceName = serviceName;
        }

        public String getTenant() {
            return tenant;
        }

        public void setTenant(String tenant) {
            this.tenant = tenant;
        }

        public String getApp() {
            return app;
        }

        public void setApp(String app) {
            this.app = app;
        }

        public InetSocketAddress getSocketAddr() {
            return socketAddr;
        }

        public void refresh() {
            lastRefTime = System.currentTimeMillis();
        }
    }

    private static byte[] compressIfNecessary(byte[] dataBytes) throws IOException {
        // enable compression when data is larger than 1KB
        int maxDataSizeUncompress = 1024;
        if (dataBytes.length < maxDataSizeUncompress) {
            return dataBytes;
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(out);
        gzip.write(dataBytes);
        gzip.close();

        return out.toByteArray();
    }

    private static Map<String, Object> prepareHostsData(PushClient client) throws Exception {
        Map<String, Object> cmd = new HashMap<String, Object>(2);
        cmd.put("type", "dom");
        cmd.put("data", client.getDataSource().getData(client));

        return cmd;
    }

    /**
     * 生成ack 实体
     * @param client
     * @param data
     * @param lastRefTime
     * @return
     */
    private static Receiver.AckEntry prepareAckEntry(PushClient client, Map<String, Object> data, long lastRefTime) {
        if (MapUtils.isEmpty(data)) {
            Loggers.PUSH.error("[NACOS-PUSH] pushing empty data for client is not allowed: {}", client);
            return null;
        }

        data.put("lastRefTime", lastRefTime);

        // we apply lastRefTime as sequence num for further ack
        String key = getACKKey(client.getSocketAddr().getAddress().getHostAddress(),
            client.getSocketAddr().getPort(),
            lastRefTime);

        String dataStr = JSON.toJSONString(data);

        try {
            byte[] dataBytes = dataStr.getBytes(StandardCharsets.UTF_8);
            // 当数据超过一定长度时进行压缩
            dataBytes = compressIfNecessary(dataBytes);

            DatagramPacket packet = new DatagramPacket(dataBytes, dataBytes.length, client.socketAddr);

            // we must store the key be fore send, otherwise there will be a chance the
            // ack returns before we put in
            Receiver.AckEntry ackEntry = new Receiver.AckEntry(key, packet);
            ackEntry.data = data;

            return ackEntry;
        } catch (Exception e) {
            Loggers.PUSH.error("[NACOS-PUSH] failed to prepare data: {} to client: {}, error: {}",
                data, client.getSocketAddr(), e);
            return null;
        }
    }

    private static Receiver.AckEntry udpPush(Receiver.AckEntry ackEntry) {
        if (ackEntry == null) {
            Loggers.PUSH.error("[NACOS-PUSH] ackEntry is null.");
            return null;
        }

        // 如果超过了最大重试次数   一旦接收到ack 会从容器中移除该对象 否则每次使用旧对象同时增加重试次数 等达到上限时提示失败信息 同时不再设置到定时任务中
        if (ackEntry.getRetryTimes() > MAX_RETRY_TIMES) {
            Loggers.PUSH.warn("max re-push times reached, retry times {}, key: {}", ackEntry.retryTimes, ackEntry.key);
            // 从相关容器中移除
            ackMap.remove(ackEntry.key);
            udpSendTimeMap.remove(ackEntry.key);
            failedPush += 1;
            return ackEntry;
        }

        try {
            // key 内包含了目标地址 以及 lastRefTime
            if (!ackMap.containsKey(ackEntry.key)) {
                totalPush++;
            }
            // 将对象存储到map中  当收到回复心跳包时 会从容器中移除
            ackMap.put(ackEntry.key, ackEntry);
            udpSendTimeMap.put(ackEntry.key, System.currentTimeMillis());

            Loggers.PUSH.info("send udp packet: " + ackEntry.key);
            // 发送数据包
            udpSocket.send(ackEntry.origin);

            ackEntry.increaseRetryTime();

            // 一定时间后 再判断该ack是否被client 处理
            executorService.schedule(new Retransmitter(ackEntry), TimeUnit.NANOSECONDS.toMillis(ACK_TIMEOUT_NANOS),
                TimeUnit.MILLISECONDS);

            return ackEntry;
        } catch (Exception e) {
            Loggers.PUSH.error("[NACOS-PUSH] failed to push data: {} to client: {}, error: {}",
                ackEntry.data, ackEntry.origin.getAddress().getHostAddress(), e);
            ackMap.remove(ackEntry.key);
            udpSendTimeMap.remove(ackEntry.key);
            failedPush += 1;

            return null;
        }
    }

    private static String getACKKey(String host, int port, long lastRefTime) {
        return StringUtils.strip(host) + "," + port + "," + lastRefTime;
    }

    /**
     * 在一定延时后重新发送ackEntry
     */
    public static class Retransmitter implements Runnable {
        Receiver.AckEntry ackEntry;

        public Retransmitter(Receiver.AckEntry ackEntry) {
            this.ackEntry = ackEntry;
        }

        @Override
        public void run() {
            if (ackMap.containsKey(ackEntry.key)) {
                Loggers.PUSH.info("retry to push data, key: " + ackEntry.key);
                udpPush(ackEntry);
            }
        }
    }

    /**
     * 专门负责接收 client 返回的ack信息
     */
    public static class Receiver implements Runnable {
        @Override
        public void run() {
            while (true) {
                byte[] buffer = new byte[1024 * 64];
                // UDP 编程
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                try {
                    // 监听收到的数据 并填充到packet中
                    udpSocket.receive(packet);

                    String json = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8).trim();
                    AckPacket ackPacket = JSON.parseObject(json, AckPacket.class);

                    // 数据包中记录了发送源节点信息
                    InetSocketAddress socketAddress = (InetSocketAddress) packet.getSocketAddress();
                    String ip = socketAddress.getAddress().getHostAddress();
                    int port = socketAddress.getPort();

                    // 这个 lastRefTime 应该是往client 发送数据包时指定的时间戳 然后client 返回数据时保留了这个属性 这样server 可以通过判断
                    // 该值是否超时
                    if (System.nanoTime() - ackPacket.lastRefTime > ACK_TIMEOUT_NANOS) {
                        Loggers.PUSH.warn("ack takes too long from {} ack json: {}", packet.getSocketAddress(), json);
                    }

                    String ackKey = getACKKey(ip, port, ackPacket.lastRefTime);
                    // 收到心跳后 从map中移除之前的映射关系     就是一个响应池
                    AckEntry ackEntry = ackMap.remove(ackKey);
                    if (ackEntry == null) {
                        throw new IllegalStateException("unable to find ackEntry for key: " + ackKey
                            + ", ack json: " + json);
                    }

                    // 代表从发出到收到回复 一共花费多少时间
                    long pushCost = System.currentTimeMillis() - udpSendTimeMap.get(ackKey);

                    Loggers.PUSH.info("received ack: {} from: {}:{}, cost: {} ms, unacked: {}, total push: {}",
                        json, ip, port, pushCost, ackMap.size(), totalPush);

                    pushCostMap.put(ackKey, pushCost);

                    udpSendTimeMap.remove(ackKey);

                } catch (Throwable e) {
                    Loggers.PUSH.error("[NACOS-PUSH] error while receiving ack data", e);
                }
            }
        }


        public static class AckEntry {

            public AckEntry(String key, DatagramPacket packet) {
                this.key = key;
                this.origin = packet;
            }

            public void increaseRetryTime() {
                retryTimes.incrementAndGet();
            }

            public int getRetryTimes() {
                return retryTimes.get();
            }

            public String key;
            public DatagramPacket origin;
            private AtomicInteger retryTimes = new AtomicInteger(0);
            public Map<String, Object> data;
        }

        /**
         * 收到的回复心跳包
         */
        public static class AckPacket {
            public String type;
            public long lastRefTime;

            public String data;
        }
    }


}
