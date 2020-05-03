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
package com.alibaba.nacos.naming.healthcheck;

import com.alibaba.nacos.naming.core.Cluster;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.monitor.MetricsMonitor;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;

import static com.alibaba.nacos.naming.misc.Loggers.SRV_LOG;

/**
 * TCP health check processor
 *
 * @author nacos
 * 服务端负责维护服务提供者的心跳检测
 */
@Component
public class TcpSuperSenseProcessor implements HealthCheckProcessor, Runnable {

    public static final String TYPE = "TCP";

    /**
     * 该对象内部包含一些检测服务实例的方法
     */
    @Autowired
    private HealthCheckCommon healthCheckCommon;

    @Autowired
    private SwitchDomain switchDomain;

    public static final int CONNECT_TIMEOUT_MS = 500;

    /**
     * 每个服务实例对应的 selectionKey  (beatKey 实际上是 selectionKey的包装对象)
     */
    private Map<String, BeatKey> keyMap = new ConcurrentHashMap<>();

    private BlockingQueue<Beat> taskQueue = new LinkedBlockingQueue<Beat>();

    /**
     * this value has been carefully tuned, do not modify unless you're confident
     */
    private static final int NIO_THREAD_COUNT = Runtime.getRuntime().availableProcessors() <= 1 ?
        1 : Runtime.getRuntime().availableProcessors() / 2;

    /**
     * because some hosts doesn't support keep-alive connections, disabled temporarily
     * 连接最大存活时间
     */
    private static final long TCP_KEEP_ALIVE_MILLIS = 0;

    private static ScheduledExecutorService TCP_CHECK_EXECUTOR
        = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("nacos.naming.tcp.check.worker");
            t.setDaemon(true);
            return t;
        }
    });

    private static ScheduledExecutorService NIO_EXECUTOR
        = Executors.newScheduledThreadPool(NIO_THREAD_COUNT,
        new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName("nacos.supersense.checker");
                return thread;
            }
        }
    );

    /**
     * 该对象负责与各个 服务实例建立长连接
     */
    private Selector selector;

    /**
     * 该对象启动的时候会开启selector 对象 并运行自身
     */
    public TcpSuperSenseProcessor() {
        try {
            selector = Selector.open();

            TCP_CHECK_EXECUTOR.submit(this);

        } catch (Exception e) {
            throw new IllegalStateException("Error while initializing SuperSense(TM).");
        }
    }

    /**
     * 开始处理心跳任务
     * @param task check task
     */
    @Override
    public void process(HealthCheckTask task) {
        // 获取集群下所有实例对象
        List<Instance> ips = task.getCluster().allIPs(false);

        if (CollectionUtils.isEmpty(ips)) {
            return;
        }

        for (Instance ip : ips) {

            // 被标记过的 实例不需要检测
            if (ip.isMarked()) {
                if (SRV_LOG.isDebugEnabled()) {
                    SRV_LOG.debug("tcp check, ip is marked as to skip health check, ip:" + ip.getIp());
                }
                continue;
            }

            // 代表正在执行心跳中
            if (!ip.markChecking()) {
                SRV_LOG.warn("tcp check started before last one finished, service: "
                    + task.getCluster().getService().getName() + ":"
                    + task.getCluster().getName() + ":"
                    + ip.getIp() + ":"
                    + ip.getPort());

                healthCheckCommon.reEvaluateCheckRT(task.getCheckRTNormalized() * 2, task, switchDomain.getTcpHealthParams());
                continue;
            }

            // 创建心跳对象 添加到 队列中
            Beat beat = new Beat(ip, task);
            // 这里没有直接处理任务 而是添加到一个队列中  之后会异步 创建 socketChannel 并发送心跳包
            taskQueue.add(beat);
            MetricsMonitor.getTcpHealthCheckMonitor().incrementAndGet();
        }
    }

    /**
     * 开始从任务队列中取出任务并执行
     * @throws Exception
     */
    private void processTask() throws Exception {
        Collection<Callable<Void>> tasks = new LinkedList<>();
        do {
            // 这里存放了所有待执行的心跳
            Beat beat = taskQueue.poll(CONNECT_TIMEOUT_MS / 2, TimeUnit.MILLISECONDS);
            if (beat == null) {
                return;
            }

            tasks.add(new TaskProcessor(beat));
        } while (taskQueue.size() > 0 && tasks.size() < NIO_THREAD_COUNT * 64);

        // 通过线程池并行提高效率 实际上有点多余 里面并没有什么耗时操作  如果这个线程池只是做这件事 并且 心跳数不多的话 感觉有点浪费资源
        for (Future<?> f : NIO_EXECUTOR.invokeAll(tasks)) {
            f.get();
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                // 为新添加的心跳任务创建TCP 连接
                processTask();

                // 开始读取新连接  这不用 select() ??? 这样自旋不消耗性能吗
                int readyCount = selector.selectNow();
                if (readyCount <= 0) {
                    continue;
                }

                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove();

                    // 执行准备好的key
                    NIO_EXECUTOR.execute(new PostProcessor(key));
                }
            } catch (Throwable e) {
                SRV_LOG.error("[HEALTH-CHECK] error while processing NIO task", e);
            }
        }
    }

    /**
     * 处理选择器上准备好的key
     */
    public class PostProcessor implements Runnable {
        SelectionKey key;

        public PostProcessor(SelectionKey key) {
            this.key = key;
        }

        @Override
        public void run() {
            // 得知该key对应的是哪个连接  以及哪个心跳任务
            Beat beat = (Beat) key.attachment();
            SocketChannel channel = (SocketChannel) key.channel();
            try {
                // 收到回复的时间间隔太长 关闭本连接
                if (!beat.isHealthy()) {
                    //invalid beat means this server is no longer responsible for the current service
                    key.cancel();
                    key.channel().close();

                    beat.finishCheck();
                    return;
                }

                // 感知到的是连接事件
                if (key.isValid() && key.isConnectable()) {
                    // 此时调用该方法如果连接没有成功建立 会抛出异常
                    channel.finishConnect();
                    // 因为成功建立连接 所以认定该节点有效
                    beat.finishCheck(true, false, System.currentTimeMillis() - beat.getTask().getStartTime(), "tcp:ok+");
                }

                if (key.isValid() && key.isReadable()) {
                    //disconnected
                    ByteBuffer buffer = ByteBuffer.allocate(128);
                    // 没有读取到任何数据时 关闭连接
                    if (channel.read(buffer) == -1) {
                        key.cancel();
                        key.channel().close();
                    } else {
                        // not terminate request, ignore
                    }
                }
                // 对应 finishConnect() 出现的异常  也就是虽然准备好了 连接事件 但实际上连接失败了
            } catch (ConnectException e) {
                // unable to connect, possibly port not opened
                beat.finishCheck(false, true, switchDomain.getTcpHealthParams().getMax(), "tcp:unable2connect:" + e.getMessage());
            } catch (Exception e) {
                // 其余异常顺便关闭连接
                beat.finishCheck(false, false, switchDomain.getTcpHealthParams().getMax(), "tcp:error:" + e.getMessage());

                try {
                    key.cancel();
                    key.channel().close();
                } catch (Exception ignore) {
                }
            }
        }
    }

    /**
     * 心跳对象
     */
    private class Beat {
        Instance ip;

        HealthCheckTask task;

        long startTime = System.currentTimeMillis();

        Beat(Instance ip, HealthCheckTask task) {
            this.ip = ip;
            this.task = task;
        }

        public void setStartTime(long time) {
            startTime = time;
        }

        public long getStartTime() {
            return startTime;
        }

        public Instance getIp() {
            return ip;
        }

        public HealthCheckTask getTask() {
            return task;
        }

        public boolean isHealthy() {
            return System.currentTimeMillis() - startTime < TimeUnit.SECONDS.toMillis(30L);
        }

        /**
         * finish check only, no ip state will be changed
         */
        public void finishCheck() {
            ip.setBeingChecked(false);
        }

        /**
         * 代表完成了一次检查
         * @param success 本次心跳是否成功
         * @param now
         * @param rt  消耗时间
         * @param msg
         */
        public void finishCheck(boolean success, boolean now, long rt, String msg) {
            ip.setCheckRT(System.currentTimeMillis() - startTime);

            if (success) {
                healthCheckCommon.checkOK(ip, task, msg);
            } else {
                if (now) {
                    // 内部将 instance 标记成失活 且 触发一个事件
                    healthCheckCommon.checkFailNow(ip, task, msg);
                } else {
                    healthCheckCommon.checkFail(ip, task, msg);
                }

                keyMap.remove(task.toString());
            }

            healthCheckCommon.reEvaluateCheckRT(rt, task, switchDomain.getTcpHealthParams());
        }

        @Override
        public String toString() {
            return task.getCluster().getService().getName() + ":"
                + task.getCluster().getName() + ":"
                + ip.getIp() + ":"
                + ip.getPort();
        }

        @Override
        public int hashCode() {
            return Objects.hash(ip.toJSON());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof Beat)) {
                return false;
            }

            return this.toString().equals(obj.toString());
        }
    }

    /**
     * 看来该节点负责与其他节点建立长连接 所以一个beatKey 对应一个 selectorKey
     */
    private static class BeatKey {
        public SelectionKey key;
        public long birthTime;

        public BeatKey(SelectionKey key) {
            this.key = key;
            this.birthTime = System.currentTimeMillis();
        }
    }

    /**
     * 在一定时间后如果没有连接到 对端 那么关闭掉key
     */
    private static class TimeOutTask implements Runnable {
        SelectionKey key;

        public TimeOutTask(SelectionKey key) {
            this.key = key;
        }

        @Override
        public void run() {
            if (key != null && key.isValid()) {
                SocketChannel channel = (SocketChannel) key.channel();
                Beat beat = (Beat) key.attachment();

                if (channel.isConnected()) {
                    return;
                }

                try {
                    channel.finishConnect();
                } catch (Exception ignore) {
                }

                try {
                    // 因为在指定时间内没有成功创建连接 所以关闭套接字
                    beat.finishCheck(false, false, beat.getTask().getCheckRTNormalized() * 2, "tcp:timeout");
                    key.cancel();
                    key.channel().close();
                } catch (Exception ignore) {
                }
            }
        }
    }

    /**
     * 心跳对象会被包裹成待执行对象
     */
    private class TaskProcessor implements Callable<Void> {

        private static final int MAX_WAIT_TIME_MILLISECONDS = 500;
        Beat beat;

        public TaskProcessor(Beat beat) {
            this.beat = beat;
        }

        @Override
        public Void call() {
            long waited = System.currentTimeMillis() - beat.getStartTime();
            if (waited > MAX_WAIT_TIME_MILLISECONDS) {
                Loggers.SRV_LOG.warn("beat task waited too long: " + waited + "ms");
            }

            SocketChannel channel = null;
            try {
                // 获取本次检测心跳的目标节点信息
                Instance instance = beat.getIp();
                Cluster cluster = beat.getTask().getCluster();

                // 根据 ip:port 找到 对应的selectionKey
                BeatKey beatKey = keyMap.get(beat.toString());

                if (beatKey != null && beatKey.key.isValid()) {
                    // 超过应该存活时间 直接返回null
                    if (System.currentTimeMillis() - beatKey.birthTime < TCP_KEEP_ALIVE_MILLIS) {
                        instance.setBeingChecked(false);
                        return null;
                    }

                    // 关闭之前的key
                    beatKey.key.cancel();
                    beatKey.key.channel().close();
                }

                // 开启一个新的通道 连接到各个实例
                channel = SocketChannel.open();
                // 配置成非阻塞模式 才可以使用选择器
                channel.configureBlocking(false);
                // only by setting this can we make the socket close event asynchronous
                channel.socket().setSoLinger(false, -1);
                channel.socket().setReuseAddress(true);
                channel.socket().setKeepAlive(true);
                channel.socket().setTcpNoDelay(true);

                int port = cluster.isUseIPPort4Check() ? instance.getPort() : cluster.getDefCkport();
                channel.connect(new InetSocketAddress(instance.getIp(), port));

                SelectionKey key
                    = channel.register(selector, SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
                key.attach(beat);
                keyMap.put(beat.toString(), new BeatKey(key));

                beat.setStartTime(System.currentTimeMillis());

                // 提交一个超时任务  当指定时间后还没有完成连接就关闭
                NIO_EXECUTOR.schedule(new TimeOutTask(key),
                    CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                beat.finishCheck(false, false, switchDomain.getTcpHealthParams().getMax(), "tcp:error:" + e.getMessage());

                if (channel != null) {
                    try {
                        channel.close();
                    } catch (Exception ignore) {
                    }
                }
            }

            return null;
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
