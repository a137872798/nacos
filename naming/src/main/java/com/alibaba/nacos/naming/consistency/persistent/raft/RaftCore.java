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
package com.alibaba.nacos.naming.consistency.persistent.raft;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.nacos.naming.boot.RunningConfig;
import com.alibaba.nacos.naming.consistency.ApplyAction;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.*;
import com.alibaba.nacos.naming.monitor.MetricsMonitor;
import com.alibaba.nacos.naming.pojo.Record;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.javatuples.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

import static com.alibaba.nacos.core.utils.SystemUtils.STANDALONE_MODE;

/**
 * 实现raft的核心逻辑
 * 粗略看下 相比 jraft 瑕疵过多 根本没有考虑脑裂的情况
 * @author nacos
 */
@Component
public class RaftCore {

    // 一些api地址常量
    public static final String API_VOTE = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/vote";

    public static final String API_BEAT = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/beat";

    public static final String API_PUB = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";

    public static final String API_DEL = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";

    public static final String API_GET = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";

    public static final String API_ON_PUB = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum/commit";

    public static final String API_ON_DEL = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum/commit";

    public static final String API_GET_PEER = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/peer";

    private ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);

            t.setDaemon(true);
            t.setName("com.alibaba.nacos.naming.raft.notifier");

            return t;
        }
    });

    public static final Lock OPERATE_LOCK = new ReentrantLock();

    public static final int PUBLISH_TERM_INCREASE_COUNT = 100;

    /**
     * 该对象内部也包含了 监听器
     */
    private volatile Map<String, List<RecordListener>> listeners = new ConcurrentHashMap<>();

    /**
     * key 是service相关的  value 代表该service 下所有实例
     */
    private volatile ConcurrentMap<String, Datum> datums = new ConcurrentHashMap<>();

    // raft相关的一些组件
    @Autowired
    private RaftPeerSet peers;

    @Autowired
    private RaftProxy raftProxy;

    @Autowired
    private RaftStore raftStore;

    @Autowired
    private SwitchDomain switchDomain;

    @Autowired
    private GlobalConfig globalConfig;

    /**
     * 该对象相当于是 监听器 与 task的桥梁 外部基于事件往该对象填充task 该对象异步触发监听器
     */
    public volatile Notifier notifier = new Notifier();

    private boolean initialized = false;

    @PostConstruct
    public void init() throws Exception {

        Loggers.RAFT.info("initializing Raft sub-system");

        executor.submit(notifier);

        long start = System.currentTimeMillis();

        // 当基于CP实现的注册中心重启时 会先从配置文件中加载历史记录  用于避免 raft还没有找到leader 的情况
        raftStore.loadDatums(notifier, datums);

        // 从元数据文件中恢复当前任期信息
        setTerm(NumberUtils.toLong(raftStore.loadMeta().getProperty("term"), 0L));

        Loggers.RAFT.info("cache loaded, datum count: {}, current term: {}", datums.size(), peers.getTerm());

        // 等待监听器的逻辑触发完
        while (true) {
            if (notifier.tasks.size() <= 0) {
                break;
            }
            Thread.sleep(1000L);
        }

        initialized = true;

        Loggers.RAFT.info("finish to load data from disk, cost: {} ms.", (System.currentTimeMillis() - start));

        // 注册心跳及选举任务
        GlobalExecutor.registerMasterElection(new MasterElection());
        GlobalExecutor.registerHeartbeat(new HeartBeat());

        Loggers.RAFT.info("timer started: leader timeout ms: {}, heart-beat timeout ms: {}",
            GlobalExecutor.LEADER_TIMEOUT_MS, GlobalExecutor.HEARTBEAT_INTERVAL_MS);
    }

    public Map<String, List<RecordListener>> getListeners() {
        return listeners;
    }

    /**
     * 参照raft协议 也就是当往raft集群中某一节点写入数据时  需要成功写入半数以上的节点时本次才算是成功写入
     * 回顾一下 jraft 是怎么做的
     * 3种情况
     * 1.leader任期比该节点旧 写入失败 然后leader 降级为follower
     * 2.任期一致直接写入
     * 3.leader任期比该节点新  那么该节点要不断回退 找到偏移量不一致的地方 从那里开始覆盖 直到同步
     * @param key
     * @param value
     * @throws Exception
     */
    public void signalPublish(String key, Record value) throws Exception {

        // 当前不是leader的情况是不具备写入能力的  raft协议要求写入的节点必须是leader节点
        if (!isLeader()) {
            JSONObject params = new JSONObject();
            params.put("key", key);
            params.put("value", value);
            Map<String, String> parameters = new HashMap<>(1);
            parameters.put("key", key);

            // 这里转发到 leader节点 进行写入
            raftProxy.proxyPostLarge(getLeader().ip, API_PUB, params.toJSONString(), parameters);
            return;
        }

        // 如果是leader 节点 直接进行写入
        try {
            OPERATE_LOCK.lock();
            long start = System.currentTimeMillis();
            final Datum datum = new Datum();
            datum.key = key;
            datum.value = value;
            // 这个 timestamp 更像一个版本号
            if (getDatum(key) == null) {
                datum.timestamp.set(1L);
            } else {
                datum.timestamp.set(getDatum(key).timestamp.incrementAndGet());
            }

            JSONObject json = new JSONObject();
            json.put("datum", datum);
            // 将本节点作为leader节点
            json.put("source", peers.local());

            // 首先将数据提交到本地节点
            onPublish(datum, peers.local());

            final String content = JSON.toJSONString(json);

            // 他这个根本不是标准实现首先 写入半数成功就包含了 leader 而下面要求的数量根本没有包含leader 也就是除了leader外 还要成功写入2n + 1的节点数  语义已经曲解了
            final CountDownLatch latch = new CountDownLatch(peers.majorityCount());
            // 开始尝试写入至少半数节点
            for (final String server : peers.allServersIncludeMyself()) {
                if (isLeader(server)) {
                    latch.countDown();
                    continue;
                }
                final String url = buildURL(server, API_ON_PUB);
                HttpClient.asyncHttpPostLarge(url, Arrays.asList("key=" + key), content, new AsyncCompletionHandler<Integer>() {
                    @Override
                    public Integer onCompleted(Response response) throws Exception {
                        if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                            Loggers.RAFT.warn("[RAFT] failed to publish data to peer, datumId={}, peer={}, http code={}",
                                datum.key, server, response.getStatusCode());
                            return 1;
                        }
                        latch.countDown();
                        return 0;
                    }

                    @Override
                    public STATE onContentWriteCompleted() {
                        return STATE.CONTINUE;
                    }
                });

            }

            if (!latch.await(UtilsAndCommons.RAFT_PUBLISH_TIMEOUT, TimeUnit.MILLISECONDS)) {
                // only majority servers return success can we consider this update success
                Loggers.RAFT.error("data publish failed, caused failed to notify majority, key={}", key);
                throw new IllegalStateException("data publish failed, caused failed to notify majority, key=" + key);
            }

            long end = System.currentTimeMillis();
            Loggers.RAFT.info("signalPublish cost {} ms, key: {}", (end - start), key);
        } finally {
            OPERATE_LOCK.unlock();
        }
    }

    /**
     * 向raft集群提交某个删除操作  卧槽这个删除操作都不需要超过半数了  一半节点脑裂没收到请求呢
     * @param key
     * @throws Exception
     */
    public void signalDelete(final String key) throws Exception {

        OPERATE_LOCK.lock();
        try {

            if (!isLeader()) {
                Map<String, String> params = new HashMap<>(1);
                params.put("key", URLEncoder.encode(key, "UTF-8"));
                // 转发到leader节点提交删除请求
                raftProxy.proxy(getLeader().ip, API_DEL, params, HttpMethod.DELETE);
                return;
            }

            JSONObject json = new JSONObject();
            // construct datum:
            Datum datum = new Datum();
            datum.key = key;
            json.put("datum", datum);
            json.put("source", peers.local());

            onDelete(datum.key, peers.local());

            for (final String server : peers.allServersWithoutMySelf()) {
                String url = buildURL(server, API_ON_DEL);
                HttpClient.asyncHttpDeleteLarge(url, null, JSON.toJSONString(json)
                    , new AsyncCompletionHandler<Integer>() {
                        @Override
                        public Integer onCompleted(Response response) throws Exception {
                            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                                Loggers.RAFT.warn("[RAFT] failed to delete data from peer, datumId={}, peer={}, http code={}", key, server, response.getStatusCode());
                                return 1;
                            }

                            RaftPeer local = peers.local();

                            local.resetLeaderDue();

                            return 0;
                        }
                    });
            }
        } finally {
            OPERATE_LOCK.unlock();
        }
    }

    /**
     * 将数据提交到某个节点
     * @param datum
     * @param source  本次收到数据并发起写入请求的leader节点(不一定是有效的 可能任期已经落后 比如脑裂)
     * @throws Exception
     */
    public void onPublish(Datum datum, RaftPeer source) throws Exception {
        RaftPeer local = peers.local();
        if (datum.value == null) {
            Loggers.RAFT.warn("received empty datum");
            throw new IllegalStateException("received empty datum");
        }

        if (!peers.isLeader(source.ip)) {
            Loggers.RAFT.warn("peer {} tried to publish data but wasn't leader, leader: {}",
                JSON.toJSONString(source), JSON.toJSONString(getLeader()));
            throw new IllegalStateException("peer(" + source.ip + ") tried to publish " +
                "data but wasn't leader");
        }

        // 当leader节点的任期小于本地节点 也就是此时leader是无效的 那么写入失败
        if (source.term.get() < local.term.get()) {
            Loggers.RAFT.warn("out of date publish, pub-term: {}, cur-term: {}",
                JSON.toJSONString(source), JSON.toJSONString(local));
            throw new IllegalStateException("out of date publish, pub-term:"
                + source.term.get() + ", cur-term: " + local.term.get());
        }

        // 更新 选举leader的时间戳
        local.resetLeaderDue();

        // if data should be persisted, usually this is true:
        // 确保本次传入的数据是 使用基于持久化的 分布式一致性算法
        if (KeyBuilder.matchPersistentKey(datum.key)) {
            // 将数据写入到文件中  基于AP实现的只需要写入到内存中
            raftStore.write(datum);
        }

        // 在内存中保留一个缓存
        datums.put(datum.key, datum);

        if (isLeader()) {
            local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
        } else {
            // 这行是什么鬼???  2个根本不是一个东西啊
            if (local.term.get() + PUBLISH_TERM_INCREASE_COUNT > source.term.get()) {
                //set leader term:   代表本地缓存的那个leader 已经过期了
                // 应该要确保数据完全同步后才能同步任期啊  否则下次写入发现任期一致偏移量一致 怎么知道之前写入的是不是脏数据 难道每次都要重新对比???
                // 任期既然已经一致了就确保了当前偏移量往前的数据必然是一致的
                // 这里根本就不考虑脏数据的清理问题
                getLeader().term.set(source.term.get());
                local.term.set(getLeader().term.get());
            } else {
                local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
            }
        }
        // 持久化任期信息
        raftStore.updateTerm(local.term.get());

        // 触发连接到本节点的监听器
        notifier.addTask(datum.key, ApplyAction.CHANGE);

        Loggers.RAFT.info("data added/updated, key={}, term={}", datum.key, local.term);
    }

    public void onDelete(String datumKey, RaftPeer source) throws Exception {

        RaftPeer local = peers.local();

        if (!peers.isLeader(source.ip)) {
            Loggers.RAFT.warn("peer {} tried to publish data but wasn't leader, leader: {}",
                JSON.toJSONString(source), JSON.toJSONString(getLeader()));
            throw new IllegalStateException("peer(" + source.ip + ") tried to publish data but wasn't leader");
        }

        if (source.term.get() < local.term.get()) {
            Loggers.RAFT.warn("out of date publish, pub-term: {}, cur-term: {}",
                JSON.toJSONString(source), JSON.toJSONString(local));
            throw new IllegalStateException("out of date publish, pub-term:"
                + source.term + ", cur-term: " + local.term);
        }

        local.resetLeaderDue();

        // do apply
        String key = datumKey;
        deleteDatum(key);

        if (KeyBuilder.matchServiceMetaKey(key)) {

            // 它这里是吧提交数作为一个简易的偏移量 但是这行逻辑还是有问题啊
            if (local.term.get() + PUBLISH_TERM_INCREASE_COUNT > source.term.get()) {
                //set leader term:
                getLeader().term.set(source.term.get());
                local.term.set(getLeader().term.get());
            } else {
                local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
            }

            raftStore.updateTerm(local.term.get());
        }

        Loggers.RAFT.info("data removed, key={}, term={}", datumKey, local.term);

    }

    /**
     * 定期触发重新选举
     */
    public class MasterElection implements Runnable {
        @Override
        public void run() {
            try {

                // 确保此时本地已经获取到集群内所有server
                if (!peers.isReady()) {
                    return;
                }

                RaftPeer local = peers.local();
                // 每次减少一定的量  满足条件后 触发重选举
                local.leaderDueMs -= GlobalExecutor.TICK_PERIOD_MS;

                if (local.leaderDueMs > 0) {
                    return;
                }

                // reset timeout
                // 重置下次的选举时间
                local.resetLeaderDue();
                local.resetHeartbeatDue();

                // 尝试发起一次投票
                sendVote();
            } catch (Exception e) {
                Loggers.RAFT.warn("[RAFT] error while master election {}", e);
            }

        }

        public void sendVote() {

            RaftPeer local = peers.get(NetUtils.localServer());
            Loggers.RAFT.info("leader timeout, start voting,leader: {}, term: {}",
                JSON.toJSONString(getLeader()), local.term);

            // 每个节点应该是不需要维护其他节点本轮的投票对象的 只有发起投票方知道就够了
            peers.reset();

            // 这里是直接将自己判定成候选人 再尝试从其他节点拉票 同时为自己投一票  也就是没有 jraft 的预投票环节
            local.term.incrementAndGet();
            local.voteFor = local.ip;
            local.state = RaftPeer.State.CANDIDATE;

            Map<String, String> params = new HashMap<>(1);
            params.put("vote", JSON.toJSONString(local));
            for (final String server : peers.allServersWithoutMySelf()) {
                final String url = buildURL(server, API_VOTE);
                try {
                    HttpClient.asyncHttpPost(url, null, params, new AsyncCompletionHandler<Integer>() {
                        @Override
                        public Integer onCompleted(Response response) throws Exception {
                            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                                Loggers.RAFT.error("NACOS-RAFT vote failed: {}, url: {}", response.getResponseBody(), url);
                                return 1;
                            }

                            RaftPeer peer = JSON.parseObject(response.getResponseBody(), RaftPeer.class);

                            Loggers.RAFT.info("received approve from peer: {}", JSON.toJSONString(peer));

                            peers.decideLeader(peer);

                            return 0;
                        }
                    });
                } catch (Exception e) {
                    Loggers.RAFT.warn("error while sending vote to server: {}", server);
                }
            }
        }
    }

    /**
     * 接收到一个投票请求
     * @param remote
     * @return
     */
    public synchronized RaftPeer receivedVote(RaftPeer remote) {
        if (!peers.contains(remote)) {
            throw new IllegalStateException("can not find peer: " + remote.ip);
        }

        RaftPeer local = peers.get(NetUtils.localServer());
        // 打回请求就够了呀 为什么自己直接决定好投票人了
        if (remote.term.get() <= local.term.get()) {
            String msg = "received illegitimate vote" +
                ", voter-term:" + remote.term + ", votee-term:" + local.term;

            Loggers.RAFT.info(msg);
            if (StringUtils.isEmpty(local.voteFor)) {
                local.voteFor = local.ip;
            }

            return local;
        }

        local.resetLeaderDue();

        local.state = RaftPeer.State.FOLLOWER;
        local.voteFor = remote.ip;
        local.term.set(remote.term.get());

        Loggers.RAFT.info("vote {} as leader, term: {}", remote.ip, remote.term);

        return local;
    }

    public class HeartBeat implements Runnable {
        @Override
        public void run() {
            try {

                if (!peers.isReady()) {
                    return;
                }

                RaftPeer local = peers.local();
                local.heartbeatDueMs -= GlobalExecutor.TICK_PERIOD_MS;
                if (local.heartbeatDueMs > 0) {
                    return;
                }

                local.resetHeartbeatDue();

                sendBeat();
            } catch (Exception e) {
                Loggers.RAFT.warn("[RAFT] error while sending beat {}", e);
            }

        }

        /**
         * 心跳任务的作用是同步集群内所有节点的状态
         * @throws IOException
         * @throws InterruptedException
         */
        public void sendBeat() throws IOException, InterruptedException {
            RaftPeer local = peers.local();
            // 必须是leader节点
            if (local.state != RaftPeer.State.LEADER && !STANDALONE_MODE) {
                return;
            }

            if (Loggers.RAFT.isDebugEnabled()) {
                Loggers.RAFT.debug("[RAFT] send beat with {} keys.", datums.size());
            }

            local.resetLeaderDue();

            // build data
            JSONObject packet = new JSONObject();
            packet.put("peer", local);

            JSONArray array = new JSONArray();

            if (switchDomain.isSendBeatOnly()) {
                Loggers.RAFT.info("[SEND-BEAT-ONLY] {}", String.valueOf(switchDomain.isSendBeatOnly()));
            }

            // 加工所有数据
            if (!switchDomain.isSendBeatOnly()) {
                for (Datum datum : datums.values()) {

                    JSONObject element = new JSONObject();

                    if (KeyBuilder.matchServiceMetaKey(datum.key)) {
                        element.put("key", KeyBuilder.briefServiceMetaKey(datum.key));
                    } else if (KeyBuilder.matchInstanceListKey(datum.key)) {
                        element.put("key", KeyBuilder.briefInstanceListkey(datum.key));
                    }
                    element.put("timestamp", datum.timestamp);

                    array.add(element);
                }
            }

            packet.put("datums", array);
            // broadcast
            Map<String, String> params = new HashMap<String, String>(1);
            params.put("beat", JSON.toJSONString(packet));

            String content = JSON.toJSONString(params);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            GZIPOutputStream gzip = new GZIPOutputStream(out);
            gzip.write(content.getBytes(StandardCharsets.UTF_8));
            gzip.close();

            byte[] compressedBytes = out.toByteArray();
            String compressedContent = new String(compressedBytes, StandardCharsets.UTF_8);

            if (Loggers.RAFT.isDebugEnabled()) {
                Loggers.RAFT.debug("raw beat data size: {}, size of compressed data: {}",
                    content.length(), compressedContent.length());
            }

            for (final String server : peers.allServersWithoutMySelf()) {
                try {
                    final String url = buildURL(server, API_BEAT);
                    if (Loggers.RAFT.isDebugEnabled()) {
                        Loggers.RAFT.debug("send beat to server " + server);
                    }
                    HttpClient.asyncHttpPostLarge(url, null, compressedBytes, new AsyncCompletionHandler<Integer>() {
                        @Override
                        public Integer onCompleted(Response response) throws Exception {
                            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                                Loggers.RAFT.error("NACOS-RAFT beat failed: {}, peer: {}",
                                    response.getResponseBody(), server);
                                MetricsMonitor.getLeaderSendBeatFailedException().increment();
                                return 1;
                            }

                            peers.update(JSON.parseObject(response.getResponseBody(), RaftPeer.class));
                            if (Loggers.RAFT.isDebugEnabled()) {
                                Loggers.RAFT.debug("receive beat response from: {}", url);
                            }
                            return 0;
                        }

                        @Override
                        public void onThrowable(Throwable t) {
                            Loggers.RAFT.error("NACOS-RAFT error while sending heart-beat to peer: {} {}", server, t);
                            MetricsMonitor.getLeaderSendBeatFailedException().increment();
                        }
                    });
                } catch (Exception e) {
                    Loggers.RAFT.error("error while sending heart-beat to peer: {} {}", server, e);
                    MetricsMonitor.getLeaderSendBeatFailedException().increment();
                }
            }

        }
    }

    /**
     * 接收心跳数据  这里强制性同步元数据  以及leader 节点内写入的所有数据
     * @param beat
     * @return
     * @throws Exception
     */
    public RaftPeer receivedBeat(JSONObject beat) throws Exception {
        final RaftPeer local = peers.local();
        final RaftPeer remote = new RaftPeer();
        remote.ip = beat.getJSONObject("peer").getString("ip");
        remote.state = RaftPeer.State.valueOf(beat.getJSONObject("peer").getString("state"));
        remote.term.set(beat.getJSONObject("peer").getLongValue("term"));
        remote.heartbeatDueMs = beat.getJSONObject("peer").getLongValue("heartbeatDueMs");
        remote.leaderDueMs = beat.getJSONObject("peer").getLongValue("leaderDueMs");
        remote.voteFor = beat.getJSONObject("peer").getString("voteFor");

        if (remote.state != RaftPeer.State.LEADER) {
            Loggers.RAFT.info("[RAFT] invalid state from master, state: {}, remote peer: {}",
                remote.state, JSON.toJSONString(remote));
            throw new IllegalArgumentException("invalid state from master, state: " + remote.state);
        }

        if (local.term.get() > remote.term.get()) {
            Loggers.RAFT.info("[RAFT] out of date beat, beat-from-term: {}, beat-to-term: {}, remote peer: {}, and leaderDueMs: {}"
                , remote.term.get(), local.term.get(), JSON.toJSONString(remote), local.leaderDueMs);
            throw new IllegalArgumentException("out of date beat, beat-from-term: " + remote.term.get()
                + ", beat-to-term: " + local.term.get());
        }

        if (local.state != RaftPeer.State.FOLLOWER) {

            Loggers.RAFT.info("[RAFT] make remote as leader, remote peer: {}", JSON.toJSONString(remote));
            // mk follower
            local.state = RaftPeer.State.FOLLOWER;
            local.voteFor = remote.ip;
        }

        final JSONArray beatDatums = beat.getJSONArray("datums");
        local.resetLeaderDue();
        local.resetHeartbeatDue();

        peers.makeLeader(remote);

        // 强制同步所有数据
        if (!switchDomain.isSendBeatOnly()) {

            Map<String, Integer> receivedKeysMap = new HashMap<>(datums.size());

            for (Map.Entry<String, Datum> entry : datums.entrySet()) {
                receivedKeysMap.put(entry.getKey(), 0);
            }

            // now check datums
            List<String> batch = new ArrayList<>();

            int processedCount = 0;
            if (Loggers.RAFT.isDebugEnabled()) {
                Loggers.RAFT.debug("[RAFT] received beat with {} keys, RaftCore.datums' size is {}, remote server: {}, term: {}, local term: {}",
                    beatDatums.size(), datums.size(), remote.ip, remote.term, local.term);
            }
            for (Object object : beatDatums) {
                processedCount = processedCount + 1;

                JSONObject entry = (JSONObject) object;
                String key = entry.getString("key");
                final String datumKey;

                if (KeyBuilder.matchServiceMetaKey(key)) {
                    datumKey = KeyBuilder.detailServiceMetaKey(key);
                } else if (KeyBuilder.matchInstanceListKey(key)) {
                    datumKey = KeyBuilder.detailInstanceListkey(key);
                } else {
                    // ignore corrupted key:
                    continue;
                }

                long timestamp = entry.getLong("timestamp");

                receivedKeysMap.put(datumKey, 1);

                try {
                    // 这里是什么意思???
                    if (datums.containsKey(datumKey) && datums.get(datumKey).timestamp.get() >= timestamp && processedCount < beatDatums.size()) {
                        continue;
                    }

                    // 只有落后的数据需要写入?? 不需要覆盖脏数据???
                    if (!(datums.containsKey(datumKey) && datums.get(datumKey).timestamp.get() >= timestamp)) {
                        batch.add(datumKey);
                    }

                    if (batch.size() < 50 && processedCount < beatDatums.size()) {
                        continue;
                    }

                    String keys = StringUtils.join(batch, ",");

                    if (batch.size() <= 0) {
                        continue;
                    }

                    Loggers.RAFT.info("get datums from leader: {}, batch size is {}, processedCount is {}, datums' size is {}, RaftCore.datums' size is {}"
                        , getLeader().ip, batch.size(), processedCount, beatDatums.size(), datums.size());

                    // update datum entry
                    // 拉取leader的数据
                    String url = buildURL(remote.ip, API_GET) + "?keys=" + URLEncoder.encode(keys, "UTF-8");
                    HttpClient.asyncHttpGet(url, null, null, new AsyncCompletionHandler<Integer>() {
                        @Override
                        public Integer onCompleted(Response response) throws Exception {
                            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                                return 1;
                            }

                            List<JSONObject> datumList = JSON.parseObject(response.getResponseBody(), new TypeReference<List<JSONObject>>() {
                            });

                            for (JSONObject datumJson : datumList) {
                                OPERATE_LOCK.lock();
                                Datum newDatum = null;
                                try {

                                    Datum oldDatum = getDatum(datumJson.getString("key"));

                                    if (oldDatum != null && datumJson.getLongValue("timestamp") <= oldDatum.timestamp.get()) {
                                        Loggers.RAFT.info("[NACOS-RAFT] timestamp is smaller than that of mine, key: {}, remote: {}, local: {}",
                                            datumJson.getString("key"), datumJson.getLongValue("timestamp"), oldDatum.timestamp);
                                        continue;
                                    }

                                    if (KeyBuilder.matchServiceMetaKey(datumJson.getString("key"))) {
                                        Datum<Service> serviceDatum = new Datum<>();
                                        serviceDatum.key = datumJson.getString("key");
                                        serviceDatum.timestamp.set(datumJson.getLongValue("timestamp"));
                                        serviceDatum.value =
                                            JSON.parseObject(JSON.toJSONString(datumJson.getJSONObject("value")), Service.class);
                                        newDatum = serviceDatum;
                                    }

                                    if (KeyBuilder.matchInstanceListKey(datumJson.getString("key"))) {
                                        Datum<Instances> instancesDatum = new Datum<>();
                                        instancesDatum.key = datumJson.getString("key");
                                        instancesDatum.timestamp.set(datumJson.getLongValue("timestamp"));
                                        instancesDatum.value =
                                            JSON.parseObject(JSON.toJSONString(datumJson.getJSONObject("value")), Instances.class);
                                        newDatum = instancesDatum;
                                    }

                                    if (newDatum == null || newDatum.value == null) {
                                        Loggers.RAFT.error("receive null datum: {}", datumJson);
                                        continue;
                                    }

                                    // 从leader拉取最新数据并覆盖
                                    raftStore.write(newDatum);

                                    datums.put(newDatum.key, newDatum);
                                    notifier.addTask(newDatum.key, ApplyAction.CHANGE);

                                    local.resetLeaderDue();

                                    if (local.term.get() + 100 > remote.term.get()) {
                                        getLeader().term.set(remote.term.get());
                                        local.term.set(getLeader().term.get());
                                    } else {
                                        local.term.addAndGet(100);
                                    }

                                    raftStore.updateTerm(local.term.get());

                                    Loggers.RAFT.info("data updated, key: {}, timestamp: {}, from {}, local term: {}",
                                        newDatum.key, newDatum.timestamp, JSON.toJSONString(remote), local.term);

                                } catch (Throwable e) {
                                    Loggers.RAFT.error("[RAFT-BEAT] failed to sync datum from leader, datum: {}", newDatum, e);
                                } finally {
                                    OPERATE_LOCK.unlock();
                                }
                            }
                            TimeUnit.MILLISECONDS.sleep(200);
                            return 0;
                        }
                    });

                    batch.clear();

                } catch (Exception e) {
                    Loggers.RAFT.error("[NACOS-RAFT] failed to handle beat entry, key: {}", datumKey);
                }

            }

            // 代表leader上已经不包含 该 key了 所以允许删除
            List<String> deadKeys = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : receivedKeysMap.entrySet()) {
                if (entry.getValue() == 0) {
                    deadKeys.add(entry.getKey());
                }
            }

            for (String deadKey : deadKeys) {
                try {
                    deleteDatum(deadKey);
                } catch (Exception e) {
                    Loggers.RAFT.error("[NACOS-RAFT] failed to remove entry, key={} {}", deadKey, e);
                }
            }

        }

        return local;
    }

    public void listen(String key, RecordListener listener) {

        List<RecordListener> listenerList = listeners.get(key);
        if (listenerList != null && listenerList.contains(listener)) {
            return;
        }

        if (listenerList == null) {
            listenerList = new CopyOnWriteArrayList<>();
            listeners.put(key, listenerList);
        }

        Loggers.RAFT.info("add listener: {}", key);

        listenerList.add(listener);

        // if data present, notify immediately
        for (Datum datum : datums.values()) {
            if (!listener.interests(datum.key)) {
                continue;
            }

            try {
                listener.onChange(datum.key, datum.value);
            } catch (Exception e) {
                Loggers.RAFT.error("NACOS-RAFT failed to notify listener", e);
            }
        }
    }

    public void unlisten(String key, RecordListener listener) {

        if (!listeners.containsKey(key)) {
            return;
        }

        for (RecordListener dl : listeners.get(key)) {
            // TODO maybe use equal:
            if (dl == listener) {
                listeners.get(key).remove(listener);
                break;
            }
        }
    }

    public void unlistenAll(String key) {
        listeners.remove(key);
    }

    public void setTerm(long term) {
        peers.setTerm(term);
    }

    public boolean isLeader(String ip) {
        return peers.isLeader(ip);
    }

    public boolean isLeader() {
        return peers.isLeader(NetUtils.localServer());
    }

    /**
     * 构建 api url
     * @param ip
     * @param api
     * @return
     */
    public static String buildURL(String ip, String api) {
        if (!ip.contains(UtilsAndCommons.IP_PORT_SPLITER)) {
            ip = ip + UtilsAndCommons.IP_PORT_SPLITER + RunningConfig.getServerPort();
        }
        return "http://" + ip + RunningConfig.getContextPath() + api;
    }

    public Datum<?> getDatum(String key) {
        return datums.get(key);
    }

    public RaftPeer getLeader() {
        return peers.getLeader();
    }

    public List<RaftPeer> getPeers() {
        return new ArrayList<>(peers.allPeers());
    }

    public RaftPeerSet getPeerSet() {
        return peers;
    }

    public void setPeerSet(RaftPeerSet peerSet) {
        peers = peerSet;
    }

    public int datumSize() {
        return datums.size();
    }

    public void addDatum(Datum datum) {
        datums.put(datum.key, datum);
        notifier.addTask(datum.key, ApplyAction.CHANGE);
    }

    public void loadDatum(String key) {
        try {
            Datum datum = raftStore.load(key);
            if (datum == null) {
                return;
            }
            datums.put(key, datum);
        } catch (Exception e) {
            Loggers.RAFT.error("load datum failed: " + key, e);
        }

    }

    private void deleteDatum(String key) {
        Datum deleted;
        try {
            deleted = datums.remove(URLDecoder.decode(key, "UTF-8"));
            if (deleted != null) {
                raftStore.delete(deleted);
                Loggers.RAFT.info("datum deleted, key: {}", key);
            }
            notifier.addTask(URLDecoder.decode(key, "UTF-8"), ApplyAction.DELETE);
        } catch (UnsupportedEncodingException e) {
            Loggers.RAFT.warn("datum key decode failed: {}", key);
        }
    }

    public boolean isInitialized() {
        return initialized || !globalConfig.isDataWarmup();
    }

    public int getNotifyTaskCount() {
        return notifier.getTaskSize();
    }

    /**
     * 该对象 负责触发监听器的 onChange onDelete 方法
     */
    public class Notifier implements Runnable {

        /**
         * 用于去重
         */
        private ConcurrentHashMap<String, String> services = new ConcurrentHashMap<>(10 * 1024);

        private BlockingQueue<Pair> tasks = new LinkedBlockingQueue<>(1024 * 1024);

        /**
         * 当某个服务发生变化时 将key 和对应的行为存入任务队列
         * @param datumKey
         * @param action
         */
        public void addTask(String datumKey, ApplyAction action) {

            if (services.containsKey(datumKey) && action == ApplyAction.CHANGE) {
                return;
            }
            if (action == ApplyAction.CHANGE) {
                services.put(datumKey, StringUtils.EMPTY);
            }

            Loggers.RAFT.info("add task {}", datumKey);

            tasks.add(Pair.with(datumKey, action));
        }

        public int getTaskSize() {
            return tasks.size();
        }

        /**
         * 定期处理任务队列中数据 触发监听器
         */
        @Override
        public void run() {
            Loggers.RAFT.info("raft notifier started");

            while (true) {
                try {

                    Pair pair = tasks.take();

                    if (pair == null) {
                        continue;
                    }

                    String datumKey = (String) pair.getValue0();
                    ApplyAction action = (ApplyAction) pair.getValue1();

                    services.remove(datumKey);

                    Loggers.RAFT.info("remove task {}", datumKey);

                    int count = 0;

                    // 找到内置的监听器 (也就是 serviceManager)
                    if (listeners.containsKey(KeyBuilder.SERVICE_META_KEY_PREFIX)) {

                        // 代表本次事件是  service
                        if (KeyBuilder.matchServiceMetaKey(datumKey) && !KeyBuilder.matchSwitchKey(datumKey)) {

                            // 触发内置监听器  也就是更新service信息
                            for (RecordListener listener : listeners.get(KeyBuilder.SERVICE_META_KEY_PREFIX)) {
                                try {
                                    if (action == ApplyAction.CHANGE) {
                                        listener.onChange(datumKey, getDatum(datumKey).value);
                                    }

                                    if (action == ApplyAction.DELETE) {
                                        listener.onDelete(datumKey);
                                    }
                                } catch (Throwable e) {
                                    Loggers.RAFT.error("[NACOS-RAFT] error while notifying listener of key: {}", datumKey, e);
                                }
                            }
                        }
                    }

                    if (!listeners.containsKey(datumKey)) {
                        continue;
                    }

                    // 找到  service级别的监听器
                    for (RecordListener listener : listeners.get(datumKey)) {

                        count++;

                        try {
                            if (action == ApplyAction.CHANGE) {
                                listener.onChange(datumKey, getDatum(datumKey).value);
                                continue;
                            }

                            if (action == ApplyAction.DELETE) {
                                listener.onDelete(datumKey);
                                continue;
                            }
                        } catch (Throwable e) {
                            Loggers.RAFT.error("[NACOS-RAFT] error while notifying listener of key: {}", datumKey, e);
                        }
                    }

                    if (Loggers.RAFT.isDebugEnabled()) {
                        Loggers.RAFT.debug("[NACOS-RAFT] datum change notified, key: {}, listener count: {}", datumKey, count);
                    }
                } catch (Throwable e) {
                    Loggers.RAFT.error("[NACOS-RAFT] Error while handling notifying task", e);
                }
            }
        }
    }
}
