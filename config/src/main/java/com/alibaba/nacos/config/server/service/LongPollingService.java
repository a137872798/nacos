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

import com.alibaba.nacos.config.server.model.SampleResult;
import com.alibaba.nacos.config.server.monitor.MetricsMonitor;
import com.alibaba.nacos.config.server.utils.GroupKey;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.alibaba.nacos.config.server.utils.MD5Util;
import com.alibaba.nacos.config.server.utils.RequestUtil;
import com.alibaba.nacos.config.server.utils.event.EventDispatcher.AbstractEventListener;
import com.alibaba.nacos.config.server.utils.event.EventDispatcher.Event;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.*;
import java.util.concurrent.*;

import static com.alibaba.nacos.config.server.utils.LogUtil.memoryLog;
import static com.alibaba.nacos.config.server.utils.LogUtil.pullLog;

/**
 * 长轮询服务
 *
 * @author Nacos
 * 该对象首先是一个监听器  能够根据nacos内部的事件执行对应逻辑
 */
@Service
public class LongPollingService extends AbstractEventListener {

    private static final int FIXED_POLLING_INTERVAL_MS = 10000;

    private static final int SAMPLE_PERIOD = 100;

    private static final int SAMPLE_TIMES = 3;

    private static final String TRUE_STR = "true";

    /**
     * 对应当前正在长轮询的ip
     */
    private Map<String, Long> retainIps = new ConcurrentHashMap<String, Long>();

    private static boolean isFixedPolling() {
        return SwitchService.getSwitchBoolean(SwitchService.FIXED_POLLING, false);
    }

    private static int getFixedPollingInterval() {
        return SwitchService.getSwitchInteger(SwitchService.FIXED_POLLING_INTERVAL, FIXED_POLLING_INTERVAL_MS);
    }

    public boolean isClientLongPolling(String clientIp) {
        return getClientPollingRecord(clientIp) != null;
    }

    /**
     * 注意该对象 属于配置中心服务器了  同时它会记录所有从它这获取配置的 clientIp
     * @param clientIp
     * @return
     */
    public Map<String, String> getClientSubConfigInfo(String clientIp) {
        // 每个client 对应一个长轮询任务对象
        ClientLongPolling record = getClientPollingRecord(clientIp);

        if (record == null) {
            return Collections.<String, String>emptyMap();
        }

        return record.clientMd5Map;
    }

    /**
     * 通过某一租户 某一数据id 定位到关联的 subscribeInfo
     *
     * @param dataId
     * @param group
     * @param tenant
     * @return
     */
    public SampleResult getSubscribleInfo(String dataId, String group, String tenant) {
        // 拼接字符串生成唯一标识
        String groupKey = GroupKey.getKeyTenant(dataId, group, tenant);
        SampleResult sampleResult = new SampleResult();
        Map<String, String> lisentersGroupkeyStatus = new HashMap<String, String>(50);

        // 遍历所有订阅者信息
        for (ClientLongPolling clientLongPolling : allSubs) {
            // 代表目标对象订阅了某个 data
            if (clientLongPolling.clientMd5Map.containsKey(groupKey)) {
                lisentersGroupkeyStatus.put(clientLongPolling.ip, clientLongPolling.clientMd5Map.get(groupKey));
            }
        }
        // 将对应的结果设置到map中 并返回
        sampleResult.setLisentersGroupkeyStatus(lisentersGroupkeyStatus);
        return sampleResult;
    }

    public SampleResult getSubscribleInfoByIp(String clientIp) {
        SampleResult sampleResult = new SampleResult();
        Map<String, String> lisentersGroupkeyStatus = new HashMap<String, String>(50);

        for (ClientLongPolling clientLongPolling : allSubs) {
            if (clientLongPolling.ip.equals(clientIp)) {
                // 一个ip可能有多个监听
                // 这里是把某个ip 下所有数据都填充进去了
                if (!lisentersGroupkeyStatus.equals(clientLongPolling.clientMd5Map)) {
                    lisentersGroupkeyStatus.putAll(clientLongPolling.clientMd5Map);
                }
            }
        }
        sampleResult.setLisentersGroupkeyStatus(lisentersGroupkeyStatus);
        return sampleResult;
    }

    /**
     * 聚合采样结果中的采样ip和监听配置的信息；合并策略用后面的覆盖前面的是没有问题的
     *
     * @param sampleResults sample Results
     * @return Results
     * 尝试将某组 样本结果进行合并
     */
    public SampleResult mergeSampleResult(List<SampleResult> sampleResults) {
        SampleResult mergeResult = new SampleResult();
        Map<String, String> lisentersGroupkeyStatus = new HashMap<String, String>(50);
        for (SampleResult sampleResult : sampleResults) {
            Map<String, String> lisentersGroupkeyStatusTmp = sampleResult.getLisentersGroupkeyStatus();
            for (Map.Entry<String, String> entry : lisentersGroupkeyStatusTmp.entrySet()) {
                lisentersGroupkeyStatus.put(entry.getKey(), entry.getValue());
            }
        }
        mergeResult.setLisentersGroupkeyStatus(lisentersGroupkeyStatus);
        return mergeResult;
    }

    /**
     * 将当前所有订阅者 对应的 应用名 以及对应的 数据信息返回
     * @return
     */
    public Map<String, Set<String>> collectApplicationSubscribeConfigInfos() {
        // 没有订阅者 直接返回
        if (allSubs == null || allSubs.isEmpty()) {
            return null;
        }
        HashMap<String, Set<String>> app2Groupkeys = new HashMap<String, Set<String>>(50);
        for (ClientLongPolling clientLongPolling : allSubs) {
            // 跳过未设置应用名的 或者未知应用
            if (StringUtils.isEmpty(clientLongPolling.appName) || "unknown".equalsIgnoreCase(
                clientLongPolling.appName)) {
                continue;
            }
            // 以应用名作为key 将clientMd5Map的数据转移到set中
            Set<String> appSubscribeConfigs = app2Groupkeys.get(clientLongPolling.appName);
            Set<String> clientSubscribeConfigs = clientLongPolling.clientMd5Map.keySet();
            if (appSubscribeConfigs == null) {
                appSubscribeConfigs = new HashSet<String>(clientSubscribeConfigs.size());
            }
            appSubscribeConfigs.addAll(clientSubscribeConfigs);
            app2Groupkeys.put(clientLongPolling.appName, appSubscribeConfigs);
        }

        return app2Groupkeys;
    }

    /**
     * 连续获取多个样本 之后进行整合后返回
     * @param dataId
     * @param group
     * @param tenant
     * @return
     */
    public SampleResult getCollectSubscribleInfo(String dataId, String group, String tenant) {
        List<SampleResult> sampleResultLst = new ArrayList<SampleResult>(50);
        for (int i = 0; i < SAMPLE_TIMES; i++) {
            SampleResult sampleTmp = getSubscribleInfo(dataId, group, tenant);
            if (sampleTmp != null) {
                sampleResultLst.add(sampleTmp);
            }
            if (i < SAMPLE_TIMES - 1) {
                try {
                    Thread.sleep(SAMPLE_PERIOD);
                } catch (InterruptedException e) {
                    LogUtil.clientLog.error("sleep wrong", e);
                }
            }
        }

        SampleResult sampleResult = mergeSampleResult(sampleResultLst);
        return sampleResult;
    }

    /**
     * 获取某个ip 订阅的所有config 同样先获取多个样本后进行整合
     * @param ip
     * @return
     */
    public SampleResult getCollectSubscribleInfoByIp(String ip) {
        SampleResult sampleResult = new SampleResult();
        sampleResult.setLisentersGroupkeyStatus(new HashMap<String, String>(50));
        for (int i = 0; i < SAMPLE_TIMES; i++) {
            SampleResult sampleTmp = getSubscribleInfoByIp(ip);
            if (sampleTmp != null) {
                if (sampleTmp.getLisentersGroupkeyStatus() != null
                    && !sampleResult.getLisentersGroupkeyStatus().equals(sampleTmp.getLisentersGroupkeyStatus())) {
                    sampleResult.getLisentersGroupkeyStatus().putAll(sampleTmp.getLisentersGroupkeyStatus());
                }
            }
            if (i < SAMPLE_TIMES - 1) {
                try {
                    Thread.sleep(SAMPLE_PERIOD);
                } catch (InterruptedException e) {
                    LogUtil.clientLog.error("sleep wrong", e);
                }
            }
        }
        return sampleResult;
    }

    /**
     * 根据客户端ip 找到长轮询对象
     * @param clientIp
     * @return
     */
    private ClientLongPolling getClientPollingRecord(String clientIp) {
        // 当前不存在订阅者的情况 直接返回
        if (allSubs == null) {
            return null;
        }

        for (ClientLongPolling clientLongPolling : allSubs) {
            HttpServletRequest request = (HttpServletRequest) clientLongPolling.asyncContext.getRequest();

            if (clientIp.equals(RequestUtil.getRemoteIp(request))) {
                return clientLongPolling;
            }
        }

        return null;
    }

    /**
     * 接收请求对象 生成一个长轮询对象
     * @param req
     * @param rsp
     * @param clientMd5Map
     * @param probeRequestSize
     */
    public void addLongPollingClient(HttpServletRequest req, HttpServletResponse rsp, Map<String, String> clientMd5Map,
                                     int probeRequestSize) {

        // 首先从req中获取长轮询超时时长
        String str = req.getHeader(LongPollingService.LONG_POLLING_HEADER);
        // 获取 hangUp 标识
        String noHangUpFlag = req.getHeader(LongPollingService.LONG_POLLING_NO_HANG_UP_HEADER);
        // 获取应用名
        String appName = req.getHeader(RequestUtil.CLIENT_APPNAME_HEADER);
        // 判断访问的是否是 vipServer
        String tag = req.getHeader("Vipserver-Tag");
        // 获取长轮询间隔时间
        int delayTime = SwitchService.getSwitchInteger(SwitchService.FIXED_DELAY_TIME, 500);
        /**
         * 提前500ms返回响应，为避免客户端超时 @qiaoyi.dingqy 2013.10.22改动  add delay time for LoadBalance
         */
        long timeout = Math.max(10000, Long.parseLong(str) - delayTime);
        // 代表按照固定时间  那么此时本机与client是否一致不重要
        if (isFixedPolling()) {
            timeout = Math.max(10000, getFixedPollingInterval());
            // do nothing but set fix polling timeout
        } else {
            long start = System.currentTimeMillis();
            List<String> changedGroups = MD5Util.compareMd5(req, rsp, clientMd5Map);
            // 代表某些配置已经发生了变化
            if (changedGroups.size() > 0) {
                // 直接返回变化的配置
                generateResponse(req, rsp, changedGroups);
                LogUtil.clientLog.info("{}|{}|{}|{}|{}|{}|{}",
                    System.currentTimeMillis() - start, "instant", RequestUtil.getRemoteIp(req), "polling",
                    clientMd5Map.size(), probeRequestSize, changedGroups.size());
                return;
            // 代表是否允许阻塞等待长轮询 不允许的情况直接返回  并将结果写回到对端
            } else if (noHangUpFlag != null && noHangUpFlag.equalsIgnoreCase(TRUE_STR)) {
                LogUtil.clientLog.info("{}|{}|{}|{}|{}|{}|{}", System.currentTimeMillis() - start, "nohangup",
                    RequestUtil.getRemoteIp(req), "polling", clientMd5Map.size(), probeRequestSize,
                    changedGroups.size());
                return;
            }
        }
        String ip = RequestUtil.getRemoteIp(req);
        // 一定要由HTTP线程调用，否则离开后容器会立即发送响应     此时本线程流程已经完成整个Servlet  但是发现设置了异步标识所以没有将数据流写入到res中 (没有释放它)
        final AsyncContext asyncContext = req.startAsync();
        // AsyncContext.setTimeout()的超时时间不准，所以只能自己控制
        asyncContext.setTimeout(0L);

        // 当client 在server 发起了一个长轮询请求 这里采用的实现方案是 使用tomcat的异步请求 在满足条件 或者超时时 才返回res
        scheduler.execute(
            new ClientLongPolling(asyncContext, clientMd5Map, ip, probeRequestSize, timeout, appName, tag));
    }

    /**
     * 返回该兴趣的事件
     * @return
     */
    @Override
    public List<Class<? extends Event>> interest() {
        List<Class<? extends Event>> eventTypes = new ArrayList<Class<? extends Event>>();
        eventTypes.add(LocalDataChangeEvent.class);
        return eventTypes;
    }

    @Override
    public void onEvent(Event event) {
        // 如果强制采用了 按照时间来拉取数据的话  不需要基于事件来触发 应该就是等待下次poll时间
        if (isFixedPolling()) {
            // ignore
        } else {
            // 否则基于事件触发   应该是客户端订阅了服务器的数据 那么一旦服务器本地的数据发生变化就要通知到客户端
            if (event instanceof LocalDataChangeEvent) {
                LocalDataChangeEvent evt = (LocalDataChangeEvent)event;
                scheduler.execute(new DataChangeTask(evt.groupKey, evt.isBeta, evt.betaIps));
            }
        }
    }

    static public boolean isSupportLongPolling(HttpServletRequest req) {
        return null != req.getHeader(LONG_POLLING_HEADER);
    }

    /**
     * 该对象的构造函数
     */
    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    public LongPollingService() {
        allSubs = new ConcurrentLinkedQueue<ClientLongPolling>();

        scheduler = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("com.alibaba.nacos.LongPolling");
                return t;
            }
        });
        scheduler.scheduleWithFixedDelay(new StatTask(), 0L, 10L, TimeUnit.SECONDS);
    }

    // =================

    static public final String LONG_POLLING_HEADER = "Long-Pulling-Timeout";
    static public final String LONG_POLLING_NO_HANG_UP_HEADER = "Long-Pulling-Timeout-No-Hangup";

    /**
     * 该服务器维护的所有客户端 的长轮询任务都是通过该定时器
     */
    final ScheduledExecutorService scheduler;

    /**
     * 所有订阅者会被包装成 该对象
     */
    final Queue<ClientLongPolling> allSubs;

    // ================= 当检测到本地数据变更事件时 执行该任务

    class DataChangeTask implements Runnable {
        @Override
        public void run() {
            try {
                ConfigService.getContentBetaMd5(groupKey);
                // 遍历所有订阅者 应该是要找到订阅该数据的client
                for (Iterator<ClientLongPolling> iter = allSubs.iterator(); iter.hasNext(); ) {
                    ClientLongPolling clientSub = iter.next();
                    // 代表该client 订阅了该数据
                    if (clientSub.clientMd5Map.containsKey(groupKey)) {
                        // 如果beta发布且不在beta列表直接跳过
                        // 如果是测试发布的配置 并且本ip 不属于测试ip 直接跳过
                        if (isBeta && !betaIps.contains(clientSub.ip)) {
                            continue;
                        }

                        // 如果tag发布且不在tag列表直接跳过
                        if (StringUtils.isNotBlank(tag) && !tag.equals(clientSub.tag)) {
                            continue;
                        }

                        // 将clientIp 信息保存到某个map中
                        getRetainIps().put(clientSub.ip, System.currentTimeMillis());
                        iter.remove();
                        LogUtil.clientLog.info("{}|{}|{}|{}|{}|{}|{}",
                            (System.currentTimeMillis() - changeTime),
                            "in-advance",
                            RequestUtil.getRemoteIp((HttpServletRequest)clientSub.asyncContext.getRequest()),
                            "polling",
                            clientSub.clientMd5Map.size(), clientSub.probeRequestSize, groupKey);
                        // 这里发送结果  同时关闭即将执行的 长轮询任务
                        clientSub.sendResponse(Arrays.asList(groupKey));
                    }
                }
            } catch (Throwable t) {
                LogUtil.defaultLog.error("data change error:" + t.getMessage(), t.getCause());
            }
        }

        DataChangeTask(String groupKey) {
            this(groupKey, false, null);
        }

        DataChangeTask(String groupKey, boolean isBeta, List<String> betaIps) {
            this(groupKey, isBeta, betaIps, null);
        }

        /**
         * event 包含的相关属性
         * @param groupKey
         * @param isBeta
         * @param betaIps
         * @param tag
         */
        DataChangeTask(String groupKey, boolean isBeta, List<String> betaIps, String tag) {
            this.groupKey = groupKey;
            this.isBeta = isBeta;
            this.betaIps = betaIps;
            this.tag = tag;
        }

        final String groupKey;
        final long changeTime = System.currentTimeMillis();
        final boolean isBeta;
        final List<String> betaIps;
        final String tag;
    }

    // =================

    class StatTask implements Runnable {
        @Override
        public void run() {
            memoryLog.info("[long-pulling] client count " + allSubs.size());
            MetricsMonitor.getLongPollingMonitor().set(allSubs.size());
        }
    }

    // =================  长轮询任务对象    实际上这个任务不应该叫长轮询任务  而是一个延迟查询  client 先到server 添加一个延时查询任务  在一定延时后 服务器反差当前配置
    // 并将变化信息通过client存根 返回到client

    class ClientLongPolling implements Runnable {

        @Override
        public void run() {
            // 在一定延时后 检查配置是否发生变化
            asyncTimeoutFuture = scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    try {
                        // 将该clientIp 以及时间戳映射对保存在 Long;PollingService 中
                        getRetainIps().put(ClientLongPolling.this.ip, System.currentTimeMillis());
                        /**
                         * 一旦执行任务的时候 就可以将当前对象从订阅容器中移除了
                         */
                        allSubs.remove(ClientLongPolling.this);

                        // 是否按照固定的时间间隔轮询
                        if (isFixedPolling()) {
                            LogUtil.clientLog.info("{}|{}|{}|{}|{}|{}",
                                (System.currentTimeMillis() - createTime),
                                "fix", RequestUtil.getRemoteIp((HttpServletRequest)asyncContext.getRequest()),
                                "polling",
                                clientMd5Map.size(), probeRequestSize);
                            // 将请求的配置项 与当前本地一级缓存的配置项 比较 md5 用于判断是否发生了变化
                            List<String> changedGroups = MD5Util.compareMd5(
                                (HttpServletRequest)asyncContext.getRequest(),
                                (HttpServletResponse)asyncContext.getResponse(), clientMd5Map);
                            // 将变化结果返回到客户端
                            if (changedGroups.size() > 0) {
                                sendResponse(changedGroups);
                            } else {
                                // 没有变化也要返回一个res
                                sendResponse(null);
                            }
                        } else {
                            LogUtil.clientLog.info("{}|{}|{}|{}|{}|{}",
                                (System.currentTimeMillis() - createTime),
                                "timeout", RequestUtil.getRemoteIp((HttpServletRequest)asyncContext.getRequest()),
                                "polling",
                                clientMd5Map.size(), probeRequestSize);
                            sendResponse(null);
                        }
                    } catch (Throwable t) {
                        LogUtil.defaultLog.error("long polling error:" + t.getMessage(), t.getCause());
                    }

                }

            }, timeoutTime, TimeUnit.MILLISECONDS);

            // 将当前任务 添加到 订阅任务容器中
            allSubs.add(this);
        }

        // 长轮询本身是被动的 但是 主动检测到本地某个配置发生变化(基于事件机制)  那么就可以提前写入结果
        void sendResponse(List<String> changedGroups) {
            /**
             *  取消超时任务
             */
            if (null != asyncTimeoutFuture) {
                asyncTimeoutFuture.cancel(false);
            }
            generateResponse(changedGroups);
        }

        void generateResponse(List<String> changedGroups) {
            if (null == changedGroups) {
                /**
                 * 告诉容器发送HTTP响应
                 * 这里相当于释放一个空的 res 到对端
                 */
                asyncContext.complete();
                return;
            }

            // 获取res对象 用于写入结果
            HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();

            try {
                String respString = MD5Util.compareMd5ResultString(changedGroups);

                // 禁用缓存
                response.setHeader("Pragma", "no-cache");
                response.setDateHeader("Expires", 0);
                response.setHeader("Cache-Control", "no-cache,no-store");
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().println(respString);
                asyncContext.complete();
            } catch (Exception se) {
                pullLog.error(se.toString(), se);
                asyncContext.complete();
            }
        }

        ClientLongPolling(AsyncContext ac, Map<String, String> clientMd5Map, String ip, int probeRequestSize,
                          long timeoutTime, String appName, String tag) {
            this.asyncContext = ac;
            this.clientMd5Map = clientMd5Map;
            this.probeRequestSize = probeRequestSize;
            this.createTime = System.currentTimeMillis();
            this.ip = ip;
            this.timeoutTime = timeoutTime;
            this.appName = appName;
            this.tag = tag;
        }

        // =================

        /**
         * 这个是tomcat的异步上下文  当开启异步请求时会生成一个该对象
         */
        final AsyncContext asyncContext;
        final Map<String, String> clientMd5Map;
        // 一些client的描述信息
        final long createTime;
        final String ip;
        final String appName;
        final String tag;
        final int probeRequestSize;
        final long timeoutTime;

        // 对应某个长轮询对象的拉取任务
        Future<?> asyncTimeoutFuture;
    }

    /**
     * 根据md5校验成功的数据  填充res 对象
     * @param request
     * @param response
     * @param changedGroups
     */
    void generateResponse(HttpServletRequest request, HttpServletResponse response, List<String> changedGroups) {
        if (null == changedGroups) {
            return;
        }

        try {
            String respString = MD5Util.compareMd5ResultString(changedGroups);
            // 禁用缓存
            response.setHeader("Pragma", "no-cache");
            response.setDateHeader("Expires", 0);
            response.setHeader("Cache-Control", "no-cache,no-store");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println(respString);
        } catch (Exception se) {
            pullLog.error(se.toString(), se);
        }
    }

    public Map<String, Long> getRetainIps() {
        return retainIps;
    }

    public void setRetainIps(Map<String, Long> retainIps) {
        this.retainIps = retainIps;
    }

}
