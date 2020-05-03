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
import com.alibaba.nacos.core.utils.SystemUtils;
import com.alibaba.nacos.naming.boot.RunningConfig;
import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.cluster.servers.ServerChangeListener;
import com.alibaba.nacos.naming.misc.HttpClient;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.Response;
import org.apache.commons.collections.SortedBag;
import org.apache.commons.collections.bag.TreeBag;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.net.HttpURLConnection;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.nacos.core.utils.SystemUtils.STANDALONE_MODE;

/**
 * @author nacos
 * 该对象维护某个raft集群的节点
 */
@Component
@DependsOn("serverListManager")
public class RaftPeerSet implements ServerChangeListener, ApplicationContextAware {

    /**
     * 内部包含集群下所有的 server
     */
    @Autowired
    private ServerListManager serverListManager;

    private ApplicationContext applicationContext;

    /**
     * 记录本节点的任期信息
     */
    private AtomicLong localTerm = new AtomicLong(0L);

    /**
     * 如果本节点是leader 应该不需要设置
     */
    private RaftPeer leader = null;

    /**
     * 记录其他节点
     */
    private Map<String, RaftPeer> peers = new HashMap<>();

    private Set<String> sites = new HashSet<>();

    private boolean ready = false;

    public RaftPeerSet() {

    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

        this.applicationContext = applicationContext;
    }

    /**
     * 本对象也会监控  集群内 server的变化
     */
    @PostConstruct
    public void init() {
        serverListManager.listen(this);
    }

    /**
     * 获取raft集群下的 leader节点
     * @return
     */
    public RaftPeer getLeader() {
        if (STANDALONE_MODE) {
            return local();
        }
        return leader;
    }

    public Set<String> allSites() {
        return sites;
    }

    /**
     * 应该是代表当前是否属于可用状态吧  当leader 还没有被选举出来时 raft 本身处在不可用状态
     * @return
     */
    public boolean isReady() {
        return ready;
    }

    /**
     * 将某些server 从raft 集群中移除
     * @param servers
     */
    public void remove(List<String> servers) {
        for (String server : servers) {
            peers.remove(server);
        }
    }

    /**
     * 更新某个节点信息
     * @param peer
     * @return
     */
    public RaftPeer update(RaftPeer peer) {
        peers.put(peer.ip, peer);
        return peer;
    }

    public boolean isLeader(String ip) {
        if (STANDALONE_MODE) {
            return true;
        }

        if (leader == null) {
            Loggers.RAFT.warn("[IS LEADER] no leader is available now!");
            return false;
        }

        return StringUtils.equals(leader.ip, ip);
    }

    /**
     * 返回当前raft集群内所有节点
     * @return
     */
    public Set<String> allServersIncludeMyself() {
        return peers.keySet();
    }

    public Set<String> allServersWithoutMySelf() {
        Set<String> servers = new HashSet<String>(peers.keySet());

        // exclude myself
        servers.remove(local().ip);

        return servers;
    }

    public Collection<RaftPeer> allPeers() {
        return peers.values();
    }

    public int size() {
        return peers.size();
    }

    /**
     * 某个候选节点申请成为leader
     * @param candidate
     * @return
     */
    public RaftPeer decideLeader(RaftPeer candidate) {
        peers.put(candidate.ip, candidate);

        // 该对象为每个值设置了一个计数器   很适合用来做投票箱
        SortedBag ips = new TreeBag();
        // 最高票数      必须要超半数才作效的  不然就等下一轮选举  否则如果是集群中某些节点宕机了 或者发生脑裂 那么同一时间就会有多个leader
        int maxApproveCount = 0;
        // 收票最多的节点
        String maxApprovePeer = null;
        for (RaftPeer peer : peers.values()) {
            if (StringUtils.isEmpty(peer.voteFor)) {
                continue;
            }

            // 传入某个节点选择的投票给谁   这个voteFor 应该要确保在同一任期才有意义
            ips.add(peer.voteFor);
            if (ips.getCount(peer.voteFor) > maxApproveCount) {
                maxApproveCount = ips.getCount(peer.voteFor);
                maxApprovePeer = peer.voteFor;
            }
        }

        // 超过半数 同意某节点成为leader
        if (maxApproveCount >= majorityCount()) {
            // 将对应节点标记成leader
            RaftPeer peer = peers.get(maxApprovePeer);
            peer.state = RaftPeer.State.LEADER;

            if (!Objects.equals(leader, peer)) {
                leader = peer;
                // 触发一个leader 替换的事件
                applicationContext.publishEvent(new LeaderElectFinishedEvent(this, leader));
                Loggers.RAFT.info("{} has become the LEADER", leader.ip);
            }
        }

        return leader;
    }

    /**
     * 将 candidate 变成leader 节点
     * @param candidate
     * @return
     */
    public RaftPeer makeLeader(RaftPeer candidate) {
        if (!Objects.equals(leader, candidate)) {
            leader = candidate;
            // 发起一个 强制指定leader 的事件  在jraft 中并没有这种功能
            applicationContext.publishEvent(new MakeLeaderEvent(this, leader));
            Loggers.RAFT.info("{} has become the LEADER, local: {}, leader: {}",
                leader.ip, JSON.toJSONString(local()), JSON.toJSONString(leader));
        }

        for (final RaftPeer peer : peers.values()) {
            Map<String, String> params = new HashMap<>(1);
            // 找到原先的那个leader
            if (!Objects.equals(peer, candidate) && peer.state == RaftPeer.State.LEADER) {
                try {
                    // 生成api 地址
                    String url = RaftCore.buildURL(peer.ip, RaftCore.API_GET_PEER);
                    // 发起异步请求
                    HttpClient.asyncHttpGet(url, null, params, new AsyncCompletionHandler<Integer>() {
                        @Override
                        public Integer onCompleted(Response response) throws Exception {
                            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                                Loggers.RAFT.error("[NACOS-RAFT] get peer failed: {}, peer: {}",
                                    response.getResponseBody(), peer.ip);
                                // 访问api失败时 将该节点标记成 follower
                                peer.state = RaftPeer.State.FOLLOWER;
                                return 1;
                            }

                            // 更新该节点信息
                            update(JSON.parseObject(response.getResponseBody(), RaftPeer.class));

                            return 0;
                        }
                    });
                } catch (Exception e) {
                    peer.state = RaftPeer.State.FOLLOWER;
                    Loggers.RAFT.error("[NACOS-RAFT] error while getting peer from peer: {}", peer.ip);
                }
            }
        }

        // 使用参数更新本地 raftPeer信息
        return update(candidate);
    }

    /**
     * 返回本节点
     * @return
     */
    public RaftPeer local() {
        // 找到本地节点
        RaftPeer peer = peers.get(NetUtils.localServer());
        // 如果没有找到并且属于单机模式 直接构造一个新节点
        if (peer == null && SystemUtils.STANDALONE_MODE) {
            RaftPeer localPeer = new RaftPeer();
            localPeer.ip = NetUtils.localServer();
            localPeer.term.set(localTerm.get());
            peers.put(localPeer.ip, localPeer);
            return localPeer;
        }
        if (peer == null) {
            throw new IllegalStateException("unable to find local peer: " + NetUtils.localServer() + ", all peers: "
                + Arrays.toString(peers.keySet().toArray()));
        }

        return peer;
    }

    public RaftPeer get(String server) {
        return peers.get(server);
    }

    /**
     * 代表需要超过半数节点
     * @return
     */
    public int majorityCount() {
        return peers.size() / 2 + 1;
    }

    /**
     * 处置本节点有关raft集群的所有信息
     */
    public void reset() {

        leader = null;

        for (RaftPeer peer : peers.values()) {
            peer.voteFor = null;
        }
    }

    /**
     * 设置本节点的任期信息
     * @param term
     */
    public void setTerm(long term) {
        localTerm.set(term);
    }

    public long getTerm() {
        return localTerm.get();
    }

    public boolean contains(RaftPeer remote) {
        return peers.containsKey(remote.ip);
    }

    /**
     * 当感知到某个节点下线 or 上线
     * @param latestMembers
     */
    @Override
    public void onChangeServerList(List<Server> latestMembers) {

        Map<String, RaftPeer> tmpPeers = new HashMap<>(8);
        for (Server member : latestMembers) {

            if (peers.containsKey(member.getKey())) {
                tmpPeers.put(member.getKey(), peers.get(member.getKey()));
                continue;
            }

            // 为新增的节点生成 peer 对象
            RaftPeer raftPeer = new RaftPeer();
            raftPeer.ip = member.getKey();

            // first time meet the local server:
            // 先使用本节点任期设置它
            if (NetUtils.localServer().equals(member.getKey())) {
                raftPeer.term.set(localTerm.get());
            }

            tmpPeers.put(member.getKey(), raftPeer);
        }

        // replace raft peer set:  更新当前raft集群内所有节点
        peers = tmpPeers;

        if (RunningConfig.getServerPort() > 0) {
            ready = true;
        }

        Loggers.RAFT.info("raft peers changed: " + latestMembers);
    }

    @Override
    public void onChangeHealthyServerList(List<Server> latestReachableMembers) {

    }
}
