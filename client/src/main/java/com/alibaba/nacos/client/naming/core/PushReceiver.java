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
package com.alibaba.nacos.client.naming.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.client.utils.StringUtils;
import com.alibaba.nacos.common.utils.IoUtils;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.Charset;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * @author xuanyin
 * 该对象负责接收从 nacos-naming server  发送的数据  更新本地服务信息 并触发相关监听器  以及返回ack信息
 * 相当于是 订阅者的职能  接收注册中心定期发送的数据包 判断某个服务相关的提供者实例是否发生变化
 */
public class PushReceiver implements Runnable {

    private ScheduledExecutorService executorService;

    /**
     * udp缓冲区大小
     */
    private static final int UDP_MSS = 64 * 1024;

    /**
     * 用于接收数据包
     */
    private DatagramSocket udpSocket;

    /**
     * 接收到的服务实例信息存储在该对象中
     */
    private HostReactor hostReactor;

    public PushReceiver(HostReactor hostReactor) {
        try {
            this.hostReactor = hostReactor;
            udpSocket = new DatagramSocket();

            executorService = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r);
                    thread.setDaemon(true);
                    thread.setName("com.alibaba.nacos.naming.push.receiver");
                    return thread;
                }
            });

            executorService.execute(this);
        } catch (Exception e) {
            NAMING_LOGGER.error("[NA] init udp socket failed", e);
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                // byte[] is initialized with 0 full filled by default
                // 设置udp的缓冲区
                byte[] buffer = new byte[UDP_MSS];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                // 等待nacos-naming server 发来的数据包
                udpSocket.receive(packet);

                // 可能数据是被压缩过的 这里要进行还原
                String json = new String(IoUtils.tryDecompress(packet.getData()), "UTF-8").trim();
                NAMING_LOGGER.info("received push data: " + json + " from " + packet.getAddress().toString());

                PushPacket pushPacket = JSON.parseObject(json, PushPacket.class);
                String ack;
                // 接收到文档 或者 service 数据实体
                if ("dom".equals(pushPacket.type) || "service".equals(pushPacket.type)) {
                    // 更新本地缓存 及将数据写入到文件  以及触发监听器
                    hostReactor.processServiceJSON(pushPacket.data);

                    // send ack to server  这里是生成ack信息
                    ack = "{\"type\": \"push-ack\""
                        + ", \"lastRefTime\":\"" + pushPacket.lastRefTime
                        + "\", \"data\":" + "\"\"}";
                // 将当前所有服务实例信息返还到服务端
                } else if ("dump".equals(pushPacket.type)) {
                    // dump data to server
                    ack = "{\"type\": \"dump-ack\""
                        + ", \"lastRefTime\": \"" + pushPacket.lastRefTime
                        + "\", \"data\":" + "\""
                        + StringUtils.escapeJavaScript(JSON.toJSONString(hostReactor.getServiceInfoMap()))
                        + "\"}";
                // 其余情况不做处理 但还是要返回ack 信息
                } else {
                    // do nothing send ack only
                    ack = "{\"type\": \"unknown-ack\""
                        + ", \"lastRefTime\":\"" + pushPacket.lastRefTime
                        + "\", \"data\":" + "\"\"}";
                }

                // 通过udp返回
                udpSocket.send(new DatagramPacket(ack.getBytes(Charset.forName("UTF-8")),
                    ack.getBytes(Charset.forName("UTF-8")).length, packet.getSocketAddress()));
            } catch (Exception e) {
                NAMING_LOGGER.error("[NA] error while receiving push data", e);
            }
        }
    }

    /**
     * 代表从 nacos server 推送过来的数据
     */
    public static class PushPacket {
        public String type;
        public long lastRefTime;
        /**
         * 数据采用json格式
         */
        public String data;
    }

    /**
     * 获取本地 udp 监听的端口
     * @return
     */
    public int getUDPPort() {
        return udpSocket.getLocalPort();
    }
}
