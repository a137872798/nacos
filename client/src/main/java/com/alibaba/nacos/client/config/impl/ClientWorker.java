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
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.config.ConfigType;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.client.config.common.GroupKey;
import com.alibaba.nacos.client.config.filter.impl.ConfigFilterChainManager;
import com.alibaba.nacos.client.config.http.HttpAgent;
import com.alibaba.nacos.client.config.impl.HttpSimpleClient.HttpResult;
import com.alibaba.nacos.client.config.utils.ContentUtils;
import com.alibaba.nacos.client.config.utils.MD5;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.client.utils.ParamUtil;
import com.alibaba.nacos.client.utils.TenantUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URLDecoder;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.nacos.api.common.Constants.LINE_SEPARATOR;
import static com.alibaba.nacos.api.common.Constants.WORD_SEPARATOR;
import static com.alibaba.nacos.api.common.Constants.CONFIG_TYPE;

/**
 * Longpolling
 *
 * @author Nacos
 * 该对象作为 config的 客户端对象 需要监听服务器上配置的变化
 */
public class ClientWorker {

    private static final Logger LOGGER = LogUtils.logger(ClientWorker.class);

    /**
     * 为某个配置设置监听器
     * @param dataId
     * @param group
     * @param listeners
     */
    public void addListeners(String dataId, String group, List<? extends Listener> listeners) {
        // 如果没有指定组 使用defaultGroup
        group = null2defaultGroup(group);
        // 生成对应的缓存对象 并添加到map中    这里没有立即拉取数据 而是为cache设置taskId 这样定时器会去拉取数据
        // 不过cache 对象创建时 就会从本地文件读取配置  (优先走降级之后走快照) 一般情况文件是不存在的
        CacheData cache = addCacheDataIfAbsent(dataId, group);
        for (Listener listener : listeners) {
            cache.addListener(listener);
        }
    }

    /**
     * 从某个配置上移除监听器
     * @param dataId
     * @param group
     * @param listener
     */
    public void removeListener(String dataId, String group, Listener listener) {
        group = null2defaultGroup(group);
        CacheData cache = getCache(dataId, group);
        if (null != cache) {
            cache.removeListener(listener);
            // 配置没有监听器的话 就不需要维护缓存对象了
            if (cache.getListeners().isEmpty()) {
                removeCache(dataId, group);
            }
        }
    }

    public void addTenantListeners(String dataId, String group, List<? extends Listener> listeners) throws NacosException {
        group = null2defaultGroup(group);
        String tenant = agent.getTenant();
        CacheData cache = addCacheDataIfAbsent(dataId, group, tenant);
        for (Listener listener : listeners) {
            cache.addListener(listener);
        }
    }

    /**
     * 为某个配置追加监听器
     * @param dataId
     * @param group
     * @param content
     * @param listeners
     * @throws NacosException
     */
    public void addTenantListenersWithContent(String dataId, String group, String content, List<? extends Listener> listeners) throws NacosException {
        group = null2defaultGroup(group);
        // 获取当前租户信息   也就是namespace
        String tenant = agent.getTenant();
        // 为对应配置信息增加缓存
        CacheData cache = addCacheDataIfAbsent(dataId, group, tenant);
        cache.setContent(content);
        // 为缓存对象设置监听器
        for (Listener listener : listeners) {
            cache.addListener(listener);
        }
    }

    public void removeTenantListener(String dataId, String group, Listener listener) {
        group = null2defaultGroup(group);
        String tenant = agent.getTenant();
        CacheData cache = getCache(dataId, group, tenant);
        if (null != cache) {
            cache.removeListener(listener);
            if (cache.getListeners().isEmpty()) {
                removeCache(dataId, group, tenant);
            }
        }
    }

    /**
     * 写时拷贝技术
     * @param dataId
     * @param group
     */
    void removeCache(String dataId, String group) {
        String groupKey = GroupKey.getKey(dataId, group);
        synchronized (cacheMap) {
            Map<String, CacheData> copy = new HashMap<String, CacheData>(cacheMap.get());
            copy.remove(groupKey);
            cacheMap.set(copy);
        }
        LOGGER.info("[{}] [unsubscribe] {}", agent.getName(), groupKey);

        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.get().size());
    }

    void removeCache(String dataId, String group, String tenant) {
        String groupKey = GroupKey.getKeyTenant(dataId, group, tenant);
        synchronized (cacheMap) {
            Map<String, CacheData> copy = new HashMap<String, CacheData>(cacheMap.get());
            copy.remove(groupKey);
            cacheMap.set(copy);
        }
        LOGGER.info("[{}] [unsubscribe] {}", agent.getName(), groupKey);

        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.get().size());
    }

    /**
     * 将 dataId 和 group 封装成缓存对象
     * @param dataId
     * @param group
     * @return
     */
    public CacheData addCacheDataIfAbsent(String dataId, String group) {
        // 如果 缓存已经存在 直接返回
        CacheData cache = getCache(dataId, group);
        if (null != cache) {
            return cache;
        }

        String key = GroupKey.getKey(dataId, group);
        cache = new CacheData(configFilterChainManager, agent.getName(), dataId, group);

        synchronized (cacheMap) {
            CacheData cacheFromMap = getCache(dataId, group);
            // multiple listeners on the same dataid+group and race condition,so double check again
            //other listener thread beat me to set to cacheMap
            if (null != cacheFromMap) {
                cache = cacheFromMap;
                //reset so that server not hang this check
                // 竞争失败的情况下允许重置 init 标识
                cache.setInitializing(true);
            } else {
                // 每次根据当前容器长度 分配合适的task
                int taskId = cacheMap.get().size() / (int) ParamUtil.getPerTaskConfigSize();
                cache.setTaskId(taskId);
            }

            // 写拷技术
            Map<String, CacheData> copy = new HashMap<String, CacheData>(cacheMap.get());
            copy.put(key, cache);
            cacheMap.set(copy);
        }

        LOGGER.info("[{}] [subscribe] {}", agent.getName(), key);

        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.get().size());

        return cache;
    }


    /**
     * 添加一个缓存数据
     * @param dataId
     * @param group
     * @param tenant
     * @return
     * @throws NacosException
     */
    public CacheData addCacheDataIfAbsent(String dataId, String group, String tenant) throws NacosException {
        CacheData cache = getCache(dataId, group, tenant);
        if (null != cache) {
            return cache;
        }
        String key = GroupKey.getKeyTenant(dataId, group, tenant);
        synchronized (cacheMap) {
            CacheData cacheFromMap = getCache(dataId, group, tenant);
            // multiple listeners on the same dataid+group and race condition,so
            // double check again
            // other listener thread beat me to set to cacheMap
            if (null != cacheFromMap) {
                cache = cacheFromMap;
                // reset so that server not hang this check
                cache.setInitializing(true);
            } else {
                cache = new CacheData(configFilterChainManager, agent.getName(), dataId, group, tenant);
                // fix issue # 1317
                // 如果设置了 初始化时就要从 remote 同步配置 那么发起http请求并获取结果 也就是一般创建cache都是要通过定时器更新的
                if (enableRemoteSyncConfig) {
                    // [0] 配置文本 [1] 配置类型 (prop,yml)
                    String[] ct = getServerConfig(dataId, group, tenant, 3000L);
                    cache.setContent(ct[0]);
                }
            }

            Map<String, CacheData> copy = new HashMap<String, CacheData>(cacheMap.get());
            copy.put(key, cache);
            cacheMap.set(copy);
        }
        LOGGER.info("[{}] [subscribe] {}", agent.getName(), key);

        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.get().size());

        return cache;
    }

    public CacheData getCache(String dataId, String group) {
        return getCache(dataId, group, TenantUtil.getUserTenantForAcm());
    }

    public CacheData getCache(String dataId, String group, String tenant) {
        if (null == dataId || null == group) {
            throw new IllegalArgumentException();
        }
        // dataId group tenant 组成唯一的 key
        return cacheMap.get().get(GroupKey.getKeyTenant(dataId, group, tenant));
    }

    /**
     * 根据 配置id  group 和租户信息 从服务器上获取配置
     * @param dataId
     * @param group
     * @param tenant
     * @param readTimeout
     * @return
     * @throws NacosException
     */
    public String[] getServerConfig(String dataId, String group, String tenant, long readTimeout)
        throws NacosException {
        String[] ct = new String[2];
        if (StringUtils.isBlank(group)) {
            group = Constants.DEFAULT_GROUP;
        }

        HttpResult result = null;
        try {
            List<String> params = null;
            if (StringUtils.isBlank(tenant)) {
                params = new ArrayList<String>(Arrays.asList("dataId", dataId, "group", group));
            } else {
                params = new ArrayList<String>(Arrays.asList("dataId", dataId, "group", group, "tenant", tenant));
            }
            // 生成参数后发起get 请求  这样就获取了 配置项信息
            result = agent.httpGet(Constants.CONFIG_CONTROLLER_PATH, null, params, agent.getEncode(), readTimeout);
        } catch (IOException e) {
            String message = String.format(
                "[%s] [sub-server] get server config exception, dataId=%s, group=%s, tenant=%s", agent.getName(),
                dataId, group, tenant);
            LOGGER.error(message, e);
            throw new NacosException(NacosException.SERVER_ERROR, e);
        }

        switch (result.code) {
            case HttpURLConnection.HTTP_OK:
                // 每次成功拉取到配置信息时都更新快照数据
                LocalConfigInfoProcessor.saveSnapshot(agent.getName(), dataId, group, tenant, result.content);
                // ct0 是本次返回的结果
                ct[0] = result.content;
                // yml/prop/text
                if (result.headers.containsKey(CONFIG_TYPE)) {
                    ct[1] = result.headers.get(CONFIG_TYPE).get(0);
                } else {
                    // 默认是text
                    ct[1] = ConfigType.TEXT.getType();
                }
                return ct;
            case HttpURLConnection.HTTP_NOT_FOUND:
                // 代表配置还不存在 删除快照
                LocalConfigInfoProcessor.saveSnapshot(agent.getName(), dataId, group, tenant, null);
                return ct;
            case HttpURLConnection.HTTP_CONFLICT: {
                LOGGER.error(
                    "[{}] [sub-server-error] get server config being modified concurrently, dataId={}, group={}, "
                        + "tenant={}", agent.getName(), dataId, group, tenant);
                throw new NacosException(NacosException.CONFLICT,
                    "data being modified, dataId=" + dataId + ",group=" + group + ",tenant=" + tenant);
            }
            case HttpURLConnection.HTTP_FORBIDDEN: {
                LOGGER.error("[{}] [sub-server-error] no right, dataId={}, group={}, tenant={}", agent.getName(), dataId,
                    group, tenant);
                throw new NacosException(result.code, result.content);
            }
            default: {
                LOGGER.error("[{}] [sub-server-error]  dataId={}, group={}, tenant={}, code={}", agent.getName(), dataId,
                    group, tenant, result.code);
                throw new NacosException(result.code,
                    "http error, code=" + result.code + ",dataId=" + dataId + ",group=" + group + ",tenant=" + tenant);
            }
        }
    }

    /**
     * 检验本地配置  如果没有设置 failoverFile 该方法实际不执行任何逻辑
     * @param cacheData
     */
    private void checkLocalConfig(CacheData cacheData) {
        final String dataId = cacheData.dataId;
        final String group = cacheData.group;
        final String tenant = cacheData.tenant;
        // 找到降级文件
        File path = LocalConfigInfoProcessor.getFailoverFile(agent.getName(), dataId, group, tenant);

        // 没有 -> 有
        // 每个cache 对象被创建时 默认都是false
        if (!cacheData.isUseLocalConfigInfo() && path.exists()) {
            // 读取降级文件的数据
            String content = LocalConfigInfoProcessor.getFailover(agent.getName(), dataId, group, tenant);
            // 生成md5
            String md5 = MD5.getInstance().getMD5String(content);
            // 并且标记当前缓存对象为本地设置
            cacheData.setUseLocalConfigInfo(true);
            cacheData.setLocalConfigInfoVersion(path.lastModified());
            cacheData.setContent(content);

            LOGGER.warn("[{}] [failover-change] failover file created. dataId={}, group={}, tenant={}, md5={}, content={}",
                agent.getName(), dataId, group, tenant, md5, ContentUtils.truncateContent(content));
            return;
        }

        // 有 -> 没有。不通知业务监听器，从server拿到配置后通知。
        // 当前配置虽然使用本地数据 但是本地数据此时不存在了
        if (cacheData.isUseLocalConfigInfo() && !path.exists()) {
            cacheData.setUseLocalConfigInfo(false);
            LOGGER.warn("[{}] [failover-change] failover file deleted. dataId={}, group={}, tenant={}", agent.getName(),
                dataId, group, tenant);
            return;
        }

        // 本地配置的版本发生变化  更新当前配置
        if (cacheData.isUseLocalConfigInfo() && path.exists()
            && cacheData.getLocalConfigInfoVersion() != path.lastModified()) {
            String content = LocalConfigInfoProcessor.getFailover(agent.getName(), dataId, group, tenant);
            String md5 = MD5.getInstance().getMD5String(content);
            cacheData.setUseLocalConfigInfo(true);
            cacheData.setLocalConfigInfoVersion(path.lastModified());
            cacheData.setContent(content);
            LOGGER.warn("[{}] [failover-change] failover file changed. dataId={}, group={}, tenant={}, md5={}, content={}",
                agent.getName(), dataId, group, tenant, md5, ContentUtils.truncateContent(content));
        }
    }

    private String null2defaultGroup(String group) {
        return (null == group) ? Constants.DEFAULT_GROUP : group.trim();
    }

    /**
     * 定期会校验配置信息
     */
    public void checkConfigInfo() {
        // 获取当前被监听的配置项  只有被监听的数据才会创建cache
        int listenerSize = cacheMap.get().size();
        int longingTaskCount = (int) Math.ceil(listenerSize / ParamUtil.getPerTaskConfigSize());
        // 每个线程负责轮询一部分的数据  实际上这种模型使用forkJoin 能够最大化提升并行度
        // 注意 只有监听数量发生变化  才会增加新的任务
        if (longingTaskCount > currentLongingTaskCount) {
            for (int i = (int) currentLongingTaskCount; i < longingTaskCount; i++) {
                executorService.execute(new LongPollingRunnable(i));
            }
            currentLongingTaskCount = longingTaskCount;
        }
    }

    /**
     * 该组配置是需要去服务端拉取最新数据 检查是否发生变化的
     * 如果条件允许的情况 会尽可能的 hang 住 直到服务端通知配置发生了变化
     */
    List<String> checkUpdateDataIds(List<CacheData> cacheDatas, List<String> inInitializingCacheList) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (CacheData cacheData : cacheDatas) {
            // 确保不是读取 failover的配置
            if (!cacheData.isUseLocalConfigInfo()) {
                sb.append(cacheData.dataId).append(WORD_SEPARATOR);
                sb.append(cacheData.group).append(WORD_SEPARATOR);
                if (StringUtils.isBlank(cacheData.tenant)) {
                    sb.append(cacheData.getMd5()).append(LINE_SEPARATOR);
                } else {
                    sb.append(cacheData.getMd5()).append(WORD_SEPARATOR);
                    sb.append(cacheData.getTenant()).append(LINE_SEPARATOR);
                }
                // 代表正在初始化中 那么就需要拉取配置
                if (cacheData.isInitializing()) {
                    // cacheData 首次出现在cacheMap中&首次check更新
                    inInitializingCacheList
                        .add(GroupKey.getKeyTenant(cacheData.dataId, cacheData.group, cacheData.tenant));
                }
            }
        }
        boolean isInitializingCacheList = !inInitializingCacheList.isEmpty();
        // 批量从服务器获取配置
        return checkUpdateConfigStr(sb.toString(), isInitializingCacheList);
    }

    /**
     * 以长轮询的方式监听配置的变化
     * @param probeUpdateString
     * @param isInitializingCacheList   如果有某个配置项是刚创建的那么 不应该hang住
     * @return
     * @throws IOException
     */
    List<String> checkUpdateConfigStr(String probeUpdateString, boolean isInitializingCacheList) throws IOException {


        // 设置长轮询需要监听的配置
        List<String> params = new ArrayList<String>(2);
        params.add(Constants.PROBE_MODIFY_REQUEST);
        params.add(probeUpdateString);

        List<String> headers = new ArrayList<String>(2);
        headers.add("Long-Pulling-Timeout");
        headers.add("" + timeout);

        // told server do not hang me up if new initializing cacheData added in
        if (isInitializingCacheList) {
            headers.add("Long-Pulling-Timeout-No-Hangup");
            headers.add("true");
        }

        // 如果没有需要监视的配置 直接返回空列表
        if (StringUtils.isBlank(probeUpdateString)) {
            return Collections.emptyList();
        }

        try {
            // In order to prevent the server from handling the delay of the client's long task,
            // increase the client's read timeout to avoid this problem.
            // 考虑数据传输时间 所以读超时时间比长轮询时间长
            long readTimeoutMs = timeout + (long) Math.round(timeout >> 1);
            // 发起post 请求 这个请求应该会阻塞很久 直到配置发生变化
            HttpResult result = agent.httpPost(Constants.CONFIG_CONTROLLER_PATH + "/listener", headers, params,
                agent.getEncode(), readTimeoutMs);

            if (HttpURLConnection.HTTP_OK == result.code) {
                setHealthServer(true);
                // 返回更新的配置
                return parseUpdateDataIdResponse(result.content);
            } else {
                setHealthServer(false);
                LOGGER.error("[{}] [check-update] get changed dataId error, code: {}", agent.getName(), result.code);
            }
        } catch (IOException e) {
            setHealthServer(false);
            LOGGER.error("[" + agent.getName() + "] [check-update] get changed dataId exception", e);
            throw e;
        }
        return Collections.emptyList();
    }

    /**
     * 这里只返回发生变化的配置  同时配置值也不返回
     */
    private List<String> parseUpdateDataIdResponse(String response) {
        if (StringUtils.isBlank(response)) {
            return Collections.emptyList();
        }

        try {
            response = URLDecoder.decode(response, "UTF-8");
        } catch (Exception e) {
            LOGGER.error("[" + agent.getName() + "] [polling-resp] decode modifiedDataIdsString error", e);
        }

        List<String> updateList = new LinkedList<String>();

        // 该组配置使用 分隔符连接
        for (String dataIdAndGroup : response.split(LINE_SEPARATOR)) {
            if (!StringUtils.isBlank(dataIdAndGroup)) {
                String[] keyArr = dataIdAndGroup.split(WORD_SEPARATOR);
                String dataId = keyArr[0];
                String group = keyArr[1];
                if (keyArr.length == 2) {
                    updateList.add(GroupKey.getKey(dataId, group));
                    LOGGER.info("[{}] [polling-resp] config changed. dataId={}, group={}", agent.getName(), dataId, group);
                } else if (keyArr.length == 3) {
                    String tenant = keyArr[2];
                    updateList.add(GroupKey.getKeyTenant(dataId, group, tenant));
                    LOGGER.info("[{}] [polling-resp] config changed. dataId={}, group={}, tenant={}", agent.getName(),
                        dataId, group, tenant);
                } else {
                    LOGGER.error("[{}] [polling-resp] invalid dataIdAndGroup error {}", agent.getName(), dataIdAndGroup);
                }
            }
        }
        return updateList;
    }

    /**
     * 初始化  clientWorker
     * @param agent
     * @param configFilterChainManager
     * @param properties
     */
    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    public ClientWorker(final HttpAgent agent, final ConfigFilterChainManager configFilterChainManager, final Properties properties) {
        this.agent = agent;
        this.configFilterChainManager = configFilterChainManager;

        // Initialize the timeout parameter
        // 初始化内部属性
        init(properties);

        executor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("com.alibaba.nacos.client.Worker." + agent.getName());
                t.setDaemon(true);
                return t;
            }
        });

        executorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("com.alibaba.nacos.client.Worker.longPolling." + agent.getName());
                t.setDaemon(true);
                return t;
            }
        });

        executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    // 定期检查配置是否发生变化  针对那些设置了监听器的 配置   (单次查询的配置不需要检测)
                    checkConfigInfo();
                } catch (Throwable e) {
                    LOGGER.error("[" + agent.getName() + "] [sub-check] rotate check error", e);
                }
            }
        }, 1L, 10L, TimeUnit.MILLISECONDS);
    }

    private void init(Properties properties) {

        // 长轮询的超时时间
        timeout = Math.max(NumberUtils.toInt(properties.getProperty(PropertyKeyConst.CONFIG_LONG_POLL_TIMEOUT),
            Constants.CONFIG_LONG_POLL_TIMEOUT), Constants.MIN_CONFIG_LONG_POLL_TIMEOUT);

        // 配置重试时长
        taskPenaltyTime = NumberUtils.toInt(properties.getProperty(PropertyKeyConst.CONFIG_RETRY_TIME), Constants.CONFIG_RETRY_TIME);

        // 当需要监听某个配置时是否需要立即拉取数据
        enableRemoteSyncConfig = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.ENABLE_REMOTE_SYNC_CONFIG));
    }

    /**
     * 长轮询任务 用于从 nacos server 拉取最新的配置    这里就对应的监听配置变化的功能
     */
    class LongPollingRunnable implements Runnable {

        /**
         * 每个runnable 都能够 处理某些配置的监听   就是通过taskId 来分组的
         */
        private int taskId;

        public LongPollingRunnable(int taskId) {
            this.taskId = taskId;
        }

        @Override
        public void run() {

            // 本task 需要监听的配置数量
            List<CacheData> cacheDatas = new ArrayList<CacheData>();
            List<String> inInitializingCacheList = new ArrayList<String>();
            try {
                // check failover config
                for (CacheData cacheData : cacheMap.get().values()) {
                    // 如果缓存数据有 taskId 并且属于本 task 对象 那么添加到列表中  之后会批量执行
                    if (cacheData.getTaskId() == taskId) {
                        cacheDatas.add(cacheData);
                        try {
                            // 从本地的 failover文件中读取相关配置 并设置到cacheData中
                            checkLocalConfig(cacheData);
                            // 代表当前正在使用failover的数据
                            if (cacheData.isUseLocalConfigInfo()) {
                                // 如果与之前的配置相比 发生了变化 那么触发用户监听器
                                cacheData.checkListenerMd5();
                            }
                        } catch (Exception e) {
                            LOGGER.error("get local config info error", e);
                        }
                    }
                }

                // check server config   去nacos-config server 判断配置是否发生变化  采用长轮询的方式 同时返回更新的服务列表
                List<String> changedGroupKeys = checkUpdateDataIds(cacheDatas, inInitializingCacheList);
                LOGGER.info("get changedGroupKeys:" + changedGroupKeys);

                for (String groupKey : changedGroupKeys) {
                    // 拆解出 group 和dataId
                    String[] key = GroupKey.parseKey(groupKey);
                    String dataId = key[0];
                    String group = key[1];
                    String tenant = null;
                    if (key.length == 3) {
                        tenant = key[2];
                    }
                    try {
                        // 拉取最新配置
                        String[] ct = getServerConfig(dataId, group, tenant, 3000L);
                        // 更新本次配置
                        CacheData cache = cacheMap.get().get(GroupKey.getKeyTenant(dataId, group, tenant));
                        cache.setContent(ct[0]);
                        if (null != ct[1]) {
                            cache.setType(ct[1]);
                        }
                        LOGGER.info("[{}] [data-received] dataId={}, group={}, tenant={}, md5={}, content={}, type={}",
                            agent.getName(), dataId, group, tenant, cache.getMd5(),
                            ContentUtils.truncateContent(ct[0]), ct[1]);
                    } catch (NacosException ioe) {
                        String message = String.format(
                            "[%s] [get-update] get changed config exception. dataId=%s, group=%s, tenant=%s",
                            agent.getName(), dataId, group, tenant);
                        LOGGER.error(message, ioe);
                    }
                }
                // 配置变更 触发监听器
                for (CacheData cacheData : cacheDatas) {
                    if (!cacheData.isInitializing() || inInitializingCacheList
                        .contains(GroupKey.getKeyTenant(cacheData.dataId, cacheData.group, cacheData.tenant))) {
                        cacheData.checkListenerMd5();  // 触发监听器
                        cacheData.setInitializing(false);
                    }
                }
                inInitializingCacheList.clear();

                // 每次长轮询结束后 开启下一次长轮询
                executorService.execute(this);

            } catch (Throwable e) {

                // If the rotation training task is abnormal, the next execution time of the task will be punished
                LOGGER.error("longPolling error : ", e);
                executorService.schedule(this, taskPenaltyTime, TimeUnit.MILLISECONDS);
            }
        }
    }

    public boolean isHealthServer() {
        return isHealthServer;
    }

    private void setHealthServer(boolean isHealthServer) {
        this.isHealthServer = isHealthServer;
    }

    final ScheduledExecutorService executor;
    final ScheduledExecutorService executorService;

    /**
     * groupKey -> cacheData
     */
    private final AtomicReference<Map<String, CacheData>> cacheMap = new AtomicReference<Map<String, CacheData>>(
        new HashMap<String, CacheData>());

    /**
     * 该对象负责与 server 进行通信
     */
    private final HttpAgent agent;
    /**
     * 该对象负责过滤
     */
    private final ConfigFilterChainManager configFilterChainManager;
    private boolean isHealthServer = true;
    private long timeout;
    private double currentLongingTaskCount = 0;
    private int taskPenaltyTime;
    private boolean enableRemoteSyncConfig = false;
}
