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
package com.alibaba.nacos.client.naming.beat;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.CommonParams;
import com.alibaba.nacos.api.naming.NamingResponseCode;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.net.NamingProxy;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;

import java.util.Map;
import java.util.concurrent.*;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * @author harold
 * 该对象负责 服务注册的续约功能  因为基于AP的实现 无法确保强写入  一旦写入的节点宕机了 那么数据就丢失了
 * 首次注册相当于开启了一个任务  会定期去判断目标节点上是否还存在服务实例信息 不在了就重新注册 否则进行续约
 */
public class BeatReactor {

    /**
     * 定时器
     */
    private ScheduledExecutorService executorService;

    /**
     * 该对象负责与 命名服务通信
     */
    private NamingProxy serverProxy;

    /**
     * 是否开启轻量级心跳
     */
    private boolean lightBeatEnabled = false;

    public final Map<String, BeatInfo> dom2Beat = new ConcurrentHashMap<String, BeatInfo>();

    public BeatReactor(NamingProxy serverProxy) {
        this(serverProxy, UtilAndComs.DEFAULT_CLIENT_BEAT_THREAD_COUNT);
    }

    /**
     * @param serverProxy
     * @param threadCount
     */
    public BeatReactor(NamingProxy serverProxy, int threadCount) {
        this.serverProxy = serverProxy;
        executorService = new ScheduledThreadPoolExecutor(threadCount, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName("com.alibaba.nacos.naming.beat.sender");
                return thread;
            }
        });
    }

    /**
     * 监控某个实例是否还存在与注册中心
     * @param serviceName
     * @param beatInfo
     */
    public void addBeatInfo(String serviceName, BeatInfo beatInfo) {
        NAMING_LOGGER.info("[BEAT] adding beat: {} to beat map.", beatInfo);
        // 通过提供的服务类型  节点信息 生成key
        String key = buildKey(serviceName, beatInfo.getIp(), beatInfo.getPort());
        BeatInfo existBeat = null;
        //fix #1733  先停止之前的心跳
        if ((existBeat = dom2Beat.remove(key)) != null) {
            existBeat.setStopped(true);
        }
        dom2Beat.put(key, beatInfo);
        executorService.schedule(new BeatTask(beatInfo), beatInfo.getPeriod(), TimeUnit.MILLISECONDS);
        MetricsMonitor.getDom2BeatSizeMonitor().set(dom2Beat.size());
    }

    public void removeBeatInfo(String serviceName, String ip, int port) {
        NAMING_LOGGER.info("[BEAT] removing beat: {}:{}:{} from beat map.", serviceName, ip, port);
        BeatInfo beatInfo = dom2Beat.remove(buildKey(serviceName, ip, port));
        if (beatInfo == null) {
            return;
        }
        beatInfo.setStopped(true);
        MetricsMonitor.getDom2BeatSizeMonitor().set(dom2Beat.size());
    }

    private String buildKey(String serviceName, String ip, int port) {
        return serviceName + Constants.NAMING_INSTANCE_ID_SPLITTER
            + ip + Constants.NAMING_INSTANCE_ID_SPLITTER + port;
    }

    /**
     * 确保实例注册到注册中心 要定期进行续约
     */
    class BeatTask implements Runnable {

        BeatInfo beatInfo;

        public BeatTask(BeatInfo beatInfo) {
            this.beatInfo = beatInfo;
        }

        @Override
        public void run() {
            // 代表该任务已经被停止  (或移除 或覆盖)
            if (beatInfo.isStopped()) {
                return;
            }
            long nextTime = beatInfo.getPeriod();
            try {
                // 检测提供者能否正常工作
                JSONObject result = serverProxy.sendBeat(beatInfo, BeatReactor.this.lightBeatEnabled);
                // 推荐的下次心跳时间
                long interval = result.getIntValue("clientBeatInterval");
                boolean lightBeatEnabled = false;
                if (result.containsKey(CommonParams.LIGHT_BEAT_ENABLED)) {
                    lightBeatEnabled = result.getBooleanValue(CommonParams.LIGHT_BEAT_ENABLED);
                }
                BeatReactor.this.lightBeatEnabled = lightBeatEnabled;
                if (interval > 0) {
                    nextTime = interval;
                }
                int code = NamingResponseCode.OK;
                if (result.containsKey(CommonParams.CODE)) {
                    code = result.getIntValue(CommonParams.CODE);
                }
                // 当收到该标识时代表 虽然准备对该实例进行续约 但是注册中心上并不包含该实例信息 所以要重新注册
                if (code == NamingResponseCode.RESOURCE_NOT_FOUND) {
                    Instance instance = new Instance();
                    instance.setPort(beatInfo.getPort());
                    instance.setIp(beatInfo.getIp());
                    instance.setWeight(beatInfo.getWeight());
                    instance.setMetadata(beatInfo.getMetadata());
                    instance.setClusterName(beatInfo.getCluster());
                    instance.setServiceName(beatInfo.getServiceName());
                    instance.setInstanceId(instance.getInstanceId());
                    instance.setEphemeral(true); // 瞬时实例代表通过 client 定期续约来确保服务实例存活  而不是通过持久化的方式
                    try {
                        // 重新发起一次注册请求
                        serverProxy.registerService(beatInfo.getServiceName(),
                            NamingUtils.getGroupName(beatInfo.getServiceName()), instance);
                    } catch (Exception ignore) {
                    }
                }
                // 续约本身失败的情况下  尝试下一轮继续续约
            } catch (NacosException ne) {
                NAMING_LOGGER.error("[CLIENT-BEAT] failed to send beat: {}, code: {}, msg: {}",
                    JSON.toJSONString(beatInfo), ne.getErrCode(), ne.getErrMsg());

            }
            executorService.schedule(new BeatTask(beatInfo), nextTime, TimeUnit.MILLISECONDS);
        }
    }
}
