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
package com.alibaba.nacos.config.server.service.dump;

import com.alibaba.nacos.common.utils.IoUtils;
import com.alibaba.nacos.config.server.constant.Constants;
import com.alibaba.nacos.config.server.manager.TaskManager;
import com.alibaba.nacos.config.server.model.ConfigInfo;
import com.alibaba.nacos.config.server.model.ConfigInfoAggr;
import com.alibaba.nacos.config.server.model.ConfigInfoChanged;
import com.alibaba.nacos.config.server.model.Page;
import com.alibaba.nacos.config.server.service.*;
import com.alibaba.nacos.config.server.service.PersistService.ConfigInfoWrapper;
import com.alibaba.nacos.config.server.service.merge.MergeTaskProcessor;
import com.alibaba.nacos.config.server.utils.*;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.nacos.core.utils.SystemUtils.LOCAL_IP;
import static com.alibaba.nacos.core.utils.SystemUtils.STANDALONE_MODE;
import static com.alibaba.nacos.config.server.utils.LogUtil.fatalLog;

/**
 * Dump data service
 *
 * @author Nacos
 * 为了避免某些节点没有被通知到 配置发生变更 这里需要主动的去同步数据库与本地的配置
 */
@Service
public class DumpService {

    /**
     * 包含了spring配置文件信息
     */
    @Autowired
    private Environment env;

    /**
     * 用于与持久层交互的对象
     */
    @Autowired
    PersistService persistService;

    /**
     * 该对象的职责应该是首次创建时 根据数据库的数据初始化本地文件
     */
    @PostConstruct
    public void init() {
        LogUtil.defaultLog.warn("DumpService start");
        // 多个刷盘处理器
        DumpProcessor processor = new DumpProcessor(this);
        DumpAllProcessor dumpAllProcessor = new DumpAllProcessor(this);
        DumpAllBetaProcessor dumpAllBetaProcessor = new DumpAllBetaProcessor(this);
        DumpAllTagProcessor dumpAllTagProcessor = new DumpAllTagProcessor(this);

        // 生成对应的 manager
        dumpTaskMgr = new TaskManager("com.alibaba.nacos.server.DumpTaskManager");
        dumpTaskMgr.setDefaultTaskProcessor(processor);

        dumpAllTaskMgr = new TaskManager("com.alibaba.nacos.server.DumpAllTaskManager");
        dumpAllTaskMgr.setDefaultTaskProcessor(dumpAllProcessor);

        // 添加2个刷盘相关的任务
        Runnable dumpAll = () -> dumpAllTaskMgr.addTask(DumpAllTask.TASK_ID, new DumpAllTask());
        Runnable dumpAllBeta = () -> dumpAllTaskMgr.addTask(DumpAllBetaTask.TASK_ID, new DumpAllBetaTask());

        // 定期清理配置历史
        Runnable clearConfigHistory = () -> {
            log.warn("clearConfigHistory start");
            // 避免并发删除 集群中只有第一个节点具备删除功能  就像 skywalking 数据都是集中发往集群某个节点
            if (ServerListService.isFirstIp()) {
                try {
                    // 计算某个之前的时间戳
                    Timestamp startTime = getBeforeStamp(TimeUtils.getCurrentTime(), 24 * getRetentionDays());
                    // 找到配置历史
                    int totalCount = persistService.findConfigHistoryCountByTime(startTime);
                    if (totalCount > 0) {
                        int pageSize = 1000;
                        int removeTime = (totalCount + pageSize - 1) / pageSize;
                        log.warn("clearConfigHistory, getBeforeStamp:{}, totalCount:{}, pageSize:{}, removeTime:{}",
                            new Object[] {startTime, totalCount, pageSize, removeTime});
                        while (removeTime > 0) {
                            // 分页删除，以免批量太大报错
                            // 分批删除历史记录
                            persistService.removeConfigHistory(startTime, pageSize);
                            removeTime--;
                        }
                    }
                } catch (Throwable e) {
                    log.error("clearConfigHistory error", e);
                }
            }
        };

        try {
            // 将数据库的数据 加载到本地文件以及一级缓存中
            dumpConfigInfo(dumpAllProcessor);

            // 更新beta缓存
            LogUtil.defaultLog.info("start clear all config-info-beta.");
            DiskUtil.clearAllBeta();
            if (persistService.isExistTable(BETA_TABLE_NAME)) {
                dumpAllBetaProcessor.process(DumpAllBetaTask.TASK_ID, new DumpAllBetaTask());
            }
            // 更新Tag缓存
            LogUtil.defaultLog.info("start clear all config-info-tag.");
            DiskUtil.clearAllTag();
            if (persistService.isExistTable(TAG_TABLE_NAME)) {
                dumpAllTagProcessor.process(DumpAllTagTask.TASK_ID, new DumpAllTagTask());
            }

            // add to dump aggr   配置有一个聚合组的概念
            List<ConfigInfoChanged> configList = persistService.findAllAggrGroup();
            if (configList != null && !configList.isEmpty()) {
                total = configList.size();
                // 按线程数分组 并行执行任务
                List<List<ConfigInfoChanged>> splitList = splitList(configList, INIT_THREAD_COUNT);
                for (List<ConfigInfoChanged> list : splitList) {
                    MergeAllDataWorker work = new MergeAllDataWorker(list);
                    work.start();
                }
                log.info("server start, schedule merge end.");
            }
        } catch (Exception e) {
            LogUtil.fatalLog.error(
                "Nacos Server did not start because dumpservice bean construction failure :\n" + e.getMessage(),
                e.getCause());
            throw new RuntimeException(
                "Nacos Server did not start because dumpservice bean construction failure :\n" + e.getMessage());
        }
        // 非单机模式下 需要开启心跳任务
        if (!STANDALONE_MODE) {
            Runnable heartbeat = () -> {
                String heartBeatTime = TimeUtils.getCurrentTime().toString();
                // write disk
                try {
                    DiskUtil.saveHeartBeatToDisk(heartBeatTime);
                } catch (IOException e) {
                    LogUtil.fatalLog.error("save heartbeat fail" + e.getMessage());
                }
            };

            // 设置一个更新时间戳的文件 (当本次从宕机中恢复时 通过该文件能判断自己离线多久)
            TimerTaskService.scheduleWithFixedDelay(heartbeat, 0, 10, TimeUnit.SECONDS);

            long initialDelay = new Random().nextInt(INITIAL_DELAY_IN_MINUTE) + 10;
            LogUtil.defaultLog.warn("initialDelay:{}", initialDelay);

            // 定期执行从数据库同步数据的任务
            TimerTaskService.scheduleWithFixedDelay(dumpAll, initialDelay, DUMP_ALL_INTERVAL_IN_MINUTE,
                TimeUnit.MINUTES);

            TimerTaskService.scheduleWithFixedDelay(dumpAllBeta, initialDelay, DUMP_ALL_INTERVAL_IN_MINUTE,
                TimeUnit.MINUTES);
        }

        // 定期清理操作记录 避免占用过多磁盘
        TimerTaskService.scheduleWithFixedDelay(clearConfigHistory, 10, 10, TimeUnit.MINUTES);

    }

    /**
     * 进行落盘
     * @param dumpAllProcessor
     * @throws IOException
     */
    private void dumpConfigInfo(DumpAllProcessor dumpAllProcessor)
        throws IOException {
        int timeStep = 6;
        Boolean isAllDump = true;
        // initial dump all
        FileInputStream fis = null;
        Timestamp heartheatLastStamp = null;
        try {
            // 如果是快速启动
            if (isQuickStart()) {
                // 找到本机的心跳文件
                File heartbeatFile = DiskUtil.heartBeatFile();
                if (heartbeatFile.exists()) {
                    fis = new FileInputStream(heartbeatFile);
                    String heartheatTempLast = IoUtils.toString(fis, Constants.ENCODE);
                    heartheatLastStamp = Timestamp.valueOf(heartheatTempLast);
                    // 还没到下次检测的时间 不进行落盘
                    if (TimeUtils.getCurrentTime().getTime()
                        // 这里 timeStep * 60 * 60 * 1000 就是6小时
                        - heartheatLastStamp.getTime() < timeStep * 60 * 60 * 1000) {
                        isAllDump = false;
                    }
                }
            }
            // 执行一个全落盘任务  从数据库读取配置并设置到二级缓存和一级缓存(配置文件 和 内存)
            if (isAllDump) {
                LogUtil.defaultLog.info("start clear all config-info.");
                // 先清理旧文件
                DiskUtil.clearAll();
                dumpAllProcessor.process(DumpAllTask.TASK_ID, new DumpAllTask());
            } else {
                Timestamp beforeTimeStamp = getBeforeStamp(heartheatLastStamp,
                    timeStep);
                // 根据 近6小时的时间生成任务对象    此时认为本地配置都还是有效的
                DumpChangeProcessor dumpChangeProcessor = new DumpChangeProcessor(
                    this, beforeTimeStamp, TimeUtils.getCurrentTime());
                dumpChangeProcessor.process(DumpChangeTask.TASK_ID, new DumpChangeTask());
                Runnable checkMd5Task = () -> {
                    LogUtil.defaultLog.error("start checkMd5Task");
                    List<String> diffList = ConfigService.checkMd5();
                    for (String groupKey : diffList) {
                        String[] dg = GroupKey.parseKey(groupKey);
                        String dataId = dg[0];
                        String group = dg[1];
                        String tenant = dg[2];
                        // 使用数据库的数据同步本地数据
                        ConfigInfoWrapper configInfo = persistService.queryConfigInfo(dataId, group, tenant);
                        ConfigService.dumpChange(dataId, group, tenant, configInfo.getContent(),
                            configInfo.getLastModified());
                    }
                    LogUtil.defaultLog.error("end checkMd5Task");
                };
                // 按一定的间隔时间执行
                TimerTaskService.scheduleWithFixedDelay(checkMd5Task, 0, 12,
                    TimeUnit.HOURS);
            }
        } catch (IOException e) {
            LogUtil.fatalLog.error("dump config fail" + e.getMessage());
            throw e;
        } finally {
            if (null != fis) {
                try {
                    fis.close();
                } catch (IOException e) {
                    LogUtil.defaultLog.warn("close file failed");
                }
            }
        }
    }

    private Timestamp getBeforeStamp(Timestamp date, int step) {
        Calendar cal = Calendar.getInstance();
        /**
         *  date 换成已经已知的Date对象
         */
        cal.setTime(date);
        /**
         *  before 6 hour
         */
        cal.add(Calendar.HOUR_OF_DAY, -step);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return Timestamp.valueOf(format.format(cal.getTime()));
    }

    /**
     * 判断是否是快速启动
     * @return
     */
    private Boolean isQuickStart() {
        try {
            String val = null;
            val = env.getProperty("isQuickStart");
            if (val != null && TRUE_STR.equals(val)) {
                isQuickStart = true;
            }
            fatalLog.warn("isQuickStart:{}", isQuickStart);
        } catch (Exception e) {
            fatalLog.error("read application.properties wrong", e);
        }
        return isQuickStart;
    }

    private int getRetentionDays() {
        String val = env.getProperty("nacos.config.retention.days");
        if (null == val) {
            return retentionDays;
        }

        int tmp = 0;
        try {
            tmp = Integer.parseInt(val);
            if (tmp > 0) {
                retentionDays = tmp;
            }
        } catch (NumberFormatException nfe) {
            fatalLog.error("read nacos.config.retention.days wrong", nfe);
        }

        return retentionDays;
    }

    public void dump(String dataId, String group, String tenant, String tag, long lastModified, String handleIp) {
        dump(dataId, group, tenant, tag, lastModified, handleIp, false);
    }

    public void dump(String dataId, String group, String tenant, long lastModified, String handleIp) {
        dump(dataId, group, tenant, lastModified, handleIp, false);
    }

    public void dump(String dataId, String group, String tenant, long lastModified, String handleIp, boolean isBeta) {
        String groupKey = GroupKey2.getKey(dataId, group, tenant);
        dumpTaskMgr.addTask(groupKey, new DumpTask(groupKey, lastModified, handleIp, isBeta));
    }

    public void dump(String dataId, String group, String tenant, String tag, long lastModified, String handleIp,
                     boolean isBeta) {
        String groupKey = GroupKey2.getKey(dataId, group, tenant);
        dumpTaskMgr.addTask(groupKey, new DumpTask(groupKey, tag, lastModified, handleIp, isBeta));
    }

    public void dumpAll() {
        dumpAllTaskMgr.addTask(DumpAllTask.TASK_ID, new DumpAllTask());
    }

    static List<List<ConfigInfoChanged>> splitList(List<ConfigInfoChanged> list, int count) {
        List<List<ConfigInfoChanged>> result = new ArrayList<List<ConfigInfoChanged>>(count);
        for (int i = 0; i < count; i++) {
            result.add(new ArrayList<ConfigInfoChanged>());
        }
        for (int i = 0; i < list.size(); i++) {
            ConfigInfoChanged config = list.get(i);
            result.get(i % count).add(config);
        }
        return result;
    }

    /**
     * 该线程负责将配置进行聚合
     */
    class MergeAllDataWorker extends Thread {
        static final int PAGE_SIZE = 10000;

        /**
         * 某组需要做聚合的数据
         */
        private List<ConfigInfoChanged> configInfoList;

        public MergeAllDataWorker(List<ConfigInfoChanged> configInfoList) {
            super("MergeAllDataWorker");
            this.configInfoList = configInfoList;
        }

        @Override
        public void run() {
            for (ConfigInfoChanged configInfo : configInfoList) {
                String dataId = configInfo.getDataId();
                String group = configInfo.getGroup();
                String tenant = configInfo.getTenant();
                try {
                    List<ConfigInfoAggr> datumList = new ArrayList<ConfigInfoAggr>();
                    // 去详情表 查询待聚合的数据
                    int rowCount = persistService.aggrConfigInfoCount(dataId, group, tenant);
                    int pageCount = (int)Math.ceil(rowCount * 1.0 / PAGE_SIZE);
                    // 分页处理 避免 OOM
                    for (int pageNo = 1; pageNo <= pageCount; pageNo++) {
                        Page<ConfigInfoAggr> page = persistService.findConfigInfoAggrByPage(dataId, group, tenant,
                            pageNo, PAGE_SIZE);
                        if (page != null) {
                            datumList.addAll(page.getPageItems());
                            log.info("[merge-query] {}, {}, size/total={}/{}", dataId, group, datumList.size(),
                                rowCount);
                        }
                    }

                    final Timestamp time = TimeUtils.getCurrentTime();
                    // 聚合
                    if (datumList.size() > 0) {
                        // 将数据进行聚合 实际上就是将Content 拼接
                        ConfigInfo cf = MergeTaskProcessor.merge(dataId, group, tenant, datumList);
                        String aggrContent = cf.getContent();
                        // 找到本地的md5
                        String localContentMD5 = ConfigService.getContentMd5(GroupKey.getKey(dataId, group));
                        String aggrConetentMD5 = MD5.getInstance().getMD5String(aggrContent);
                        if (!StringUtils.equals(localContentMD5, aggrConetentMD5)) {
                            // 将聚合结果回填到数据库
                            persistService.insertOrUpdate(null, null, cf, time, null, false);
                            log.info("[merge-ok] {}, {}, size={}, length={}, md5={}, content={}", dataId, group,
                                datumList.size(), cf.getContent().length(), cf.getMd5(),
                                ContentUtils.truncateContent(cf.getContent()));
                        }
                    }
                    // 删除
                    else {
                        persistService.removeConfigInfo(dataId, group, tenant, LOCAL_IP, null);
                        log.warn("[merge-delete] delete config info because no datum. dataId=" + dataId + ", groupId="
                            + group);
                    }

                } catch (Throwable e) {
                    log.info("[merge-error] " + dataId + ", " + group + ", " + e.toString(), e);
                }
                FINISHED.incrementAndGet();
                if (FINISHED.get() % 100 == 0) {
                    log.info("[all-merge-dump] {} / {}", FINISHED.get(), total);
                }
            }
            log.info("[all-merge-dump] {} / {}", FINISHED.get(), total);
        }
    }

    /**
     * 全量dump间隔
     */
    static final int DUMP_ALL_INTERVAL_IN_MINUTE = 6 * 60;
    /**
     * 全量dump间隔
     */
    static final int INITIAL_DELAY_IN_MINUTE = 6 * 60;

    private TaskManager dumpTaskMgr;
    private TaskManager dumpAllTaskMgr;

    private static final Logger log = LoggerFactory.getLogger(DumpService.class);

    static final AtomicInteger FINISHED = new AtomicInteger();

    static final int INIT_THREAD_COUNT = 10;
    int total = 0;
    private final static String TRUE_STR = "true";
    private final static String BETA_TABLE_NAME = "config_info_beta";
    private final static String TAG_TABLE_NAME = "config_info_tag";

    Boolean isQuickStart = false;

    private int retentionDays = 30;
}
