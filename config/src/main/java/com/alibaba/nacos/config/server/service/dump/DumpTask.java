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

import com.alibaba.nacos.config.server.manager.AbstractTask;
import com.alibaba.nacos.config.server.manager.TaskProcessor;
import com.alibaba.nacos.config.server.model.ConfigInfo;
import com.alibaba.nacos.config.server.model.ConfigInfo4Beta;
import com.alibaba.nacos.config.server.model.ConfigInfo4Tag;
import com.alibaba.nacos.config.server.model.Page;
import com.alibaba.nacos.config.server.service.*;
import com.alibaba.nacos.config.server.service.PersistService.ConfigInfoBetaWrapper;
import com.alibaba.nacos.config.server.service.PersistService.ConfigInfoTagWrapper;
import com.alibaba.nacos.config.server.service.PersistService.ConfigInfoWrapper;
import com.alibaba.nacos.config.server.service.trace.ConfigTraceService;
import com.alibaba.nacos.config.server.utils.GroupKey2;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.alibaba.nacos.config.server.utils.MD5;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.util.List;

import static com.alibaba.nacos.config.server.utils.LogUtil.defaultLog;

/**
 * Dump data task
 *
 * @author Nacos
 * 刷盘相关的任务
 */
public class DumpTask extends AbstractTask {

    public DumpTask(String groupKey, long lastModified, String handleIp) {
        this.groupKey = groupKey;
        this.lastModified = lastModified;
        this.handleIp = handleIp;
        this.isBeta = false;
        this.tag = null;
        /**
         * retry interval: 1s
         * 设置任务间隔时间
         */
        setTaskInterval(1000L);
    }

    public DumpTask(String groupKey, long lastModified, String handleIp, boolean isBeta) {
        this.groupKey = groupKey;
        this.lastModified = lastModified;
        this.handleIp = handleIp;
        this.isBeta = isBeta;
        this.tag = null;
        /**
         *  retry interval: 1s
         */
        setTaskInterval(1000L);
    }

    public DumpTask(String groupKey, String tag, long lastModified, String handleIp, boolean isBeta) {
        this.groupKey = groupKey;
        this.lastModified = lastModified;
        // 原始的处理nacos-config client请求的节点
        this.handleIp = handleIp;
        this.isBeta = isBeta;
        this.tag = tag;
        /**
         * retry interval: 1s
         */
        setTaskInterval(1000L);
    }

    /**
     * 默认情况下就是新任务顶替旧任务  不需要整合
     * @param task task
     */
    @Override
    public void merge(AbstractTask task) {
    }

    final String groupKey;
    final long lastModified;
    final String handleIp;
    final boolean isBeta;
    final String tag;
}

class DumpAllTask extends AbstractTask {
    @Override
    public void merge(AbstractTask task) {
    }

    static final String TASK_ID = "dumpAllConfigTask";
}

class DumpAllBetaTask extends AbstractTask {
    @Override
    public void merge(AbstractTask task) {
    }

    static final String TASK_ID = "dumpAllBetaConfigTask";
}

class DumpAllTagTask extends AbstractTask {
    @Override
    public void merge(AbstractTask task) {
    }

    static final String TASK_ID = "dumpAllTagConfigTask";
}

class DumpChangeTask extends AbstractTask {
    @Override
    public void merge(AbstractTask task) {
    }

    static final String TASK_ID = "dumpChangeConfigTask";
}

/**
 * 刷盘相关的处理器
 */
class DumpProcessor implements TaskProcessor {

    DumpProcessor(DumpService dumpService) {
        this.dumpService = dumpService;
    }

    @Override
    public boolean process(String taskType, AbstractTask task) {
        DumpTask dumpTask = (DumpTask)task;
        // 本次任务针对的是哪个配置
        String[] pair = GroupKey2.parseKey(dumpTask.groupKey);
        String dataId = pair[0];
        String group = pair[1];
        String tenant = pair[2];
        long lastModified = dumpTask.lastModified;
        String handleIp = dumpTask.handleIp;
        boolean isBeta = dumpTask.isBeta;
        String tag = dumpTask.tag;
        if (isBeta) {
            // beta发布，则dump数据，更新beta缓存
            // 从数据库中找到对应数据
            ConfigInfo4Beta cf = dumpService.persistService.findConfigInfo4Beta(dataId, group, tenant);
            boolean result;
            if (null != cf) {
                // 将本地文件与数据库做同步  同时更新缓存
                result = ConfigService.dumpBeta(dataId, group, tenant, cf.getContent(), lastModified, cf.getBetaIps());
                if (result) {
                    ConfigTraceService.logDumpEvent(dataId, group, tenant, null, lastModified, handleIp,
                        ConfigTraceService.DUMP_EVENT_OK, System.currentTimeMillis() - lastModified,
                        cf.getContent().length());
                }
            } else {
                // 如果数据库已经移除了该配置 同时移除本地文件以及缓存
                result = ConfigService.removeBeta(dataId, group, tenant);
                if (result) {
                    ConfigTraceService.logDumpEvent(dataId, group, tenant, null, lastModified, handleIp,
                        ConfigTraceService.DUMP_EVENT_REMOVE_OK, System.currentTimeMillis() - lastModified, 0);
                }
            }
            return result;
        } else {
            // 处理非beta配置
            if (StringUtils.isBlank(tag)) {
                ConfigInfo cf = dumpService.persistService.findConfigInfo(dataId, group, tenant);
                if (dataId.equals(AggrWhitelist.AGGRIDS_METADATA)) {
                    if (null != cf) {
                        AggrWhitelist.load(cf.getContent());
                    } else {
                        AggrWhitelist.load(null);
                    }
                }

                if (dataId.equals(ClientIpWhiteList.CLIENT_IP_WHITELIST_METADATA)) {
                    if (null != cf) {
                        ClientIpWhiteList.load(cf.getContent());
                    } else {
                        ClientIpWhiteList.load(null);
                    }
                }

                if (dataId.equals(SwitchService.SWITCH_META_DATAID)) {
                    if (null != cf) {
                        SwitchService.load(cf.getContent());
                    } else {
                        SwitchService.load(null);
                    }
                }

                boolean result;
                if (null != cf) {
                    result = ConfigService.dump(dataId, group, tenant, cf.getContent(), lastModified, cf.getType());

                    if (result) {
                        ConfigTraceService.logDumpEvent(dataId, group, tenant, null, lastModified, handleIp,
                            ConfigTraceService.DUMP_EVENT_OK, System.currentTimeMillis() - lastModified,
                            cf.getContent().length());
                    }
                } else {
                    result = ConfigService.remove(dataId, group, tenant);

                    if (result) {
                        ConfigTraceService.logDumpEvent(dataId, group, tenant, null, lastModified, handleIp,
                            ConfigTraceService.DUMP_EVENT_REMOVE_OK, System.currentTimeMillis() - lastModified, 0);
                    }
                }
                return result;
            } else {
                // 如果携带了 tag 信息 使用tag 查询数据
                ConfigInfo4Tag cf = dumpService.persistService.findConfigInfo4Tag(dataId, group, tenant, tag);
                //
                boolean result;
                if (null != cf) {
                    result = ConfigService.dumpTag(dataId, group, tenant, tag, cf.getContent(), lastModified);
                    if (result) {
                        ConfigTraceService.logDumpEvent(dataId, group, tenant, null, lastModified, handleIp,
                            ConfigTraceService.DUMP_EVENT_OK, System.currentTimeMillis() - lastModified,
                            cf.getContent().length());
                    }
                } else {
                    result = ConfigService.removeTag(dataId, group, tenant, tag);
                    if (result) {
                        ConfigTraceService.logDumpEvent(dataId, group, tenant, null, lastModified, handleIp,
                            ConfigTraceService.DUMP_EVENT_REMOVE_OK, System.currentTimeMillis() - lastModified, 0);
                    }
                }
                return result;
            }
        }

    }

    final DumpService dumpService;
}

/**
 * 该处理器用于处理 刷盘任务
 */
class DumpAllProcessor implements TaskProcessor {

    DumpAllProcessor(DumpService dumpService) {
        this.dumpService = dumpService;
        this.persistService = dumpService.persistService;
    }

    @Override
    public boolean process(String taskType, AbstractTask task) {
        long currentMaxId = persistService.findConfigMaxId();
        long lastMaxId = 0;
        while (lastMaxId < currentMaxId) {
            // 按照固定大小读取分页信息
            Page<PersistService.ConfigInfoWrapper> page = persistService.findAllConfigInfoFragment(lastMaxId,
                PAGE_SIZE);
            if (page != null && page.getPageItems() != null && !page.getPageItems().isEmpty()) {
                for (PersistService.ConfigInfoWrapper cf : page.getPageItems()) {
                    long id = cf.getId();
                    // 每次处理的时候 同时更新id
                    lastMaxId = id > lastMaxId ? id : lastMaxId;
                    if (cf.getDataId().equals(AggrWhitelist.AGGRIDS_METADATA)) {
                        AggrWhitelist.load(cf.getContent());
                    }

                    // 如果是某种配置就更新本地数据  同时新配置 会更新旧配置
                    if (cf.getDataId().equals(ClientIpWhiteList.CLIENT_IP_WHITELIST_METADATA)) {
                        ClientIpWhiteList.load(cf.getContent());
                    }

                    if (cf.getDataId().equals(SwitchService.SWITCH_META_DATAID)) {
                        SwitchService.load(cf.getContent());
                    }

                    // 将数据库的数据 转移到本地文件中
                    boolean result = ConfigService.dump(cf.getDataId(), cf.getGroup(), cf.getTenant(), cf.getContent(),
                        cf.getLastModified(), cf.getType());

                    final String content = cf.getContent();
                    final String md5 = MD5.getInstance().getMD5String(content);
                    LogUtil.dumpLog.info("[dump-all-ok] {}, {}, length={}, md5={}",
                        GroupKey2.getKey(cf.getDataId(), cf.getGroup()), cf.getLastModified(), content.length(), md5);
                }
                defaultLog.info("[all-dump] {} / {}", lastMaxId, currentMaxId);
            } else {
                lastMaxId += PAGE_SIZE;
            }
        }
        return true;
    }

    static final int PAGE_SIZE = 1000;

    final DumpService dumpService;
    final PersistService persistService;
}

/**
 * 处理所有 beta  数据  (beta数据也就是代表只有部分受众的 有点像灰度发布)
 */
class DumpAllBetaProcessor implements TaskProcessor {

    DumpAllBetaProcessor(DumpService dumpService) {
        this.dumpService = dumpService;
        this.persistService = dumpService.persistService;
    }

    /**
     * 基本与上面类似 数据直接落盘
     * @param taskType task type    任务类型
     * @param task     task         待处理的任务
     * @return
     */
    @Override
    public boolean process(String taskType, AbstractTask task) {
        int rowCount = persistService.configInfoBetaCount();
        int pageCount = (int)Math.ceil(rowCount * 1.0 / PAGE_SIZE);

        int actualRowCount = 0;
        for (int pageNo = 1; pageNo <= pageCount; pageNo++) {
            Page<ConfigInfoBetaWrapper> page = persistService.findAllConfigInfoBetaForDumpAll(pageNo, PAGE_SIZE);
            if (page != null) {
                for (ConfigInfoBetaWrapper cf : page.getPageItems()) {
                    boolean result = ConfigService.dumpBeta(cf.getDataId(), cf.getGroup(), cf.getTenant(),
                        cf.getContent(), cf.getLastModified(), cf.getBetaIps());
                    LogUtil.dumpLog.info("[dump-all-beta-ok] result={}, {}, {}, length={}, md5={}", result,
                        GroupKey2.getKey(cf.getDataId(), cf.getGroup()), cf.getLastModified(), cf.getContent()
                            .length(), cf.getMd5());
                }

                actualRowCount += page.getPageItems().size();
                defaultLog.info("[all-dump-beta] {} / {}", actualRowCount, rowCount);
            }
        }
        return true;
    }

    static final int PAGE_SIZE = 1000;

    final DumpService dumpService;
    final PersistService persistService;
}

/**
 * 也就是一致性是通过数据库来保证的 每个节点定期从数据库拉取最新数据 并更新本地文件  以及刷新二级缓存(内存缓存)
 */
class DumpAllTagProcessor implements TaskProcessor {

    DumpAllTagProcessor(DumpService dumpService) {
        this.dumpService = dumpService;
        this.persistService = dumpService.persistService;
    }

    @Override
    public boolean process(String taskType, AbstractTask task) {
        int rowCount = persistService.configInfoTagCount();
        int pageCount = (int)Math.ceil(rowCount * 1.0 / PAGE_SIZE);

        int actualRowCount = 0;
        for (int pageNo = 1; pageNo <= pageCount; pageNo++) {
            Page<ConfigInfoTagWrapper> page = persistService.findAllConfigInfoTagForDumpAll(pageNo, PAGE_SIZE);
            if (page != null) {
                for (ConfigInfoTagWrapper cf : page.getPageItems()) {
                    boolean result = ConfigService.dumpTag(cf.getDataId(), cf.getGroup(), cf.getTenant(), cf.getTag(),
                        cf.getContent(), cf.getLastModified());
                    LogUtil.dumpLog.info("[dump-all-Tag-ok] result={}, {}, {}, length={}, md5={}", result,
                        GroupKey2.getKey(cf.getDataId(), cf.getGroup()), cf.getLastModified(), cf.getContent()
                            .length(), cf.getMd5());
                }

                actualRowCount += page.getPageItems().size();
                defaultLog.info("[all-dump-tag] {} / {}", actualRowCount, rowCount);
            }
        }
        return true;
    }

    static final int PAGE_SIZE = 1000;

    final DumpService dumpService;
    final PersistService persistService;
}

/**
 * 针对数据发生变更时触发
 */
class DumpChangeProcessor implements TaskProcessor {

    DumpChangeProcessor(DumpService dumpService, Timestamp startTime,
                        Timestamp endTime) {
        this.dumpService = dumpService;
        this.persistService = dumpService.persistService;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public boolean process(String taskType, AbstractTask task) {
        LogUtil.defaultLog.warn("quick start; startTime:{},endTime:{}",
            startTime, endTime);
        LogUtil.defaultLog.warn("updateMd5 start");
        long startUpdateMd5 = System.currentTimeMillis();
        // 这里读取所有配置  不怕发生OOM吗
        List<ConfigInfoWrapper> updateMd5List = persistService
            .listAllGroupKeyMd5();
        LogUtil.defaultLog.warn("updateMd5 count:{}", updateMd5List.size());
        for (ConfigInfoWrapper config : updateMd5List) {
            final String groupKey = GroupKey2.getKey(config.getDataId(),
                config.getGroup());
            // 更新缓存  同时触发一个本地配置变更的事件 监听器感知到后 会通知所有订阅该配置的client
            // 注意这里没有 dump  因为调用的场景是 快速启动 也就是继续使用之前创建的本地配置文件
            ConfigService.updateMd5(groupKey, config.getMd5(),
                config.getLastModified());
        }
        long endUpdateMd5 = System.currentTimeMillis();
        LogUtil.defaultLog.warn("updateMd5 done,cost:{}", endUpdateMd5
            - startUpdateMd5);

        LogUtil.defaultLog.warn("deletedConfig start");
        long startDeletedConfigTime = System.currentTimeMillis();
        // 找到这段时间被删除的配置
        List<ConfigInfo> configDeleted = persistService.findDeletedConfig(
            startTime, endTime);
        LogUtil.defaultLog.warn("deletedConfig count:{}", configDeleted.size());
        for (ConfigInfo configInfo : configDeleted) {
            if (persistService.findConfigInfo(configInfo.getDataId(), configInfo.getGroup(),
                configInfo.getTenant()) == null) {
                // 删除本地配置文件中相关的数据 以及缓存中的数据  每个 dataId对应一个文件
                ConfigService.remove(configInfo.getDataId(), configInfo.getGroup(), configInfo.getTenant());
            }
        }
        long endDeletedConfigTime = System.currentTimeMillis();
        LogUtil.defaultLog.warn("deletedConfig done,cost:{}",
            endDeletedConfigTime - startDeletedConfigTime);

        LogUtil.defaultLog.warn("changeConfig start");
        long startChangeConfigTime = System.currentTimeMillis();
        // 找到时间段内发生变化的配置
        List<PersistService.ConfigInfoWrapper> changeConfigs = persistService
            .findChangeConfig(startTime, endTime);
        LogUtil.defaultLog.warn("changeConfig count:{}", changeConfigs.size());
        for (PersistService.ConfigInfoWrapper cf : changeConfigs) {
            boolean result = ConfigService.dumpChange(cf.getDataId(), cf.getGroup(), cf.getTenant(),
                cf.getContent(), cf.getLastModified());
            final String content = cf.getContent();
            final String md5 = MD5.getInstance().getMD5String(content);
            LogUtil.defaultLog.info(
                "[dump-change-ok] {}, {}, length={}, md5={}",
                new Object[] {
                    GroupKey2.getKey(cf.getDataId(), cf.getGroup()),
                    cf.getLastModified(), content.length(), md5});
        }
        // 重新加载配置
        ConfigService.reloadConfig();
        long endChangeConfigTime = System.currentTimeMillis();
        LogUtil.defaultLog.warn("changeConfig done,cost:{}",
            endChangeConfigTime - startChangeConfigTime);
        return true;
    }

    // =====================

    final DumpService dumpService;
    final PersistService persistService;
    final Timestamp startTime;
    final Timestamp endTime;
}
