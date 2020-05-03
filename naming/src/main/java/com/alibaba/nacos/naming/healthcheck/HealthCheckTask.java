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

import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.nacos.naming.boot.SpringContext;
import com.alibaba.nacos.naming.core.Cluster;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import org.apache.commons.lang3.RandomUtils;

/**
 * 心跳检测任务对象   该对象是针对集群创建的
 * @author nacos
 */
public class HealthCheckTask implements Runnable {

    /**
     * 本次心跳任务关于哪个集群
     */
    private Cluster cluster;

    private long checkRTNormalized = -1;
    private long checkRTBest = -1;
    private long checkRTWorst = -1;

    private long checkRTLast = -1;
    private long checkRTLastLast = -1;

    private long startTime;

    private volatile boolean cancelled = false;

    @JSONField(serialize = false)
    private DistroMapper distroMapper;

    @JSONField(serialize = false)
    private SwitchDomain switchDomain;

    /**
     * 该对象负责执行心跳任务   内含多种方式 (TCP,HTTP,MYSQL 等)   默认采用TCP
     */
    @JSONField(serialize = false)
    private HealthCheckProcessor healthCheckProcessor;

    public HealthCheckTask(Cluster cluster) {
        this.cluster = cluster;
        distroMapper = SpringContext.getAppContext().getBean(DistroMapper.class);
        switchDomain = SpringContext.getAppContext().getBean(SwitchDomain.class);
        healthCheckProcessor = SpringContext.getAppContext().getBean(HealthCheckProcessorDelegate.class);
        initCheckRT();
    }

    public void initCheckRT() {
        // first check time delay
        checkRTNormalized = 2000 + RandomUtils.nextInt(0, RandomUtils.nextInt(0, switchDomain.getTcpHealthParams().getMax()));
        checkRTBest = Long.MAX_VALUE;
        checkRTWorst = 0L;
    }

    // 定期触发该方法
    @Override
    public void run() {

        try {
            if (distroMapper.responsible(cluster.getService().getName()) &&
                switchDomain.isHealthCheckEnabled(cluster.getService().getName())) {
                // 交由处理器执行本任务
                healthCheckProcessor.process(this);
                if (Loggers.EVT_LOG.isDebugEnabled()) {
                    Loggers.EVT_LOG.debug("[HEALTH-CHECK] schedule health check task: {}", cluster.getService().getName());
                }
            }
        } catch (Throwable e) {
            Loggers.SRV_LOG.error("[HEALTH-CHECK] error while process health check for {}:{}",
                cluster.getService().getName(), cluster.getName(), e);
        } finally {
            // 执行下次任务
            if (!cancelled) {
                HealthCheckReactor.scheduleCheck(this);

                // worst == 0 means never checked
                if (this.getCheckRTWorst() > 0
                    && switchDomain.isHealthCheckEnabled(cluster.getService().getName())
                    && distroMapper.responsible(cluster.getService().getName())) {
                    // TLog doesn't support float so we must convert it into long
                    long diff = ((this.getCheckRTLast() - this.getCheckRTLastLast()) * 10000)
                        / this.getCheckRTLastLast();

                    this.setCheckRTLastLast(this.getCheckRTLast());

                    Cluster cluster = this.getCluster();

                    if (Loggers.CHECK_RT.isDebugEnabled()) {
                        Loggers.CHECK_RT.debug("{}:{}@{}->normalized: {}, worst: {}, best: {}, last: {}, diff: {}",
                            cluster.getService().getName(), cluster.getName(), cluster.getHealthChecker().getType(),
                            this.getCheckRTNormalized(), this.getCheckRTWorst(), this.getCheckRTBest(),
                            this.getCheckRTLast(), diff);
                    }
                }
            }
        }
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public long getCheckRTNormalized() {
        return checkRTNormalized;
    }

    public long getCheckRTBest() {
        return checkRTBest;
    }

    public long getCheckRTWorst() {
        return checkRTWorst;
    }

    public void setCheckRTWorst(long checkRTWorst) {
        this.checkRTWorst = checkRTWorst;
    }

    public void setCheckRTBest(long checkRTBest) {
        this.checkRTBest = checkRTBest;
    }

    public void setCheckRTNormalized(long checkRTNormalized) {
        this.checkRTNormalized = checkRTNormalized;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getCheckRTLast() {
        return checkRTLast;
    }

    public void setCheckRTLast(long checkRTLast) {
        this.checkRTLast = checkRTLast;
    }

    public long getCheckRTLastLast() {
        return checkRTLastLast;
    }

    public void setCheckRTLastLast(long checkRTLastLast) {
        this.checkRTLastLast = checkRTLastLast;
    }
}
