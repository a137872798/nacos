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

import com.alibaba.nacos.config.server.constant.Constants;
import com.alibaba.nacos.config.server.model.CacheItem;
import com.alibaba.nacos.config.server.model.ConfigInfoBase;
import com.alibaba.nacos.config.server.utils.GroupKey;
import com.alibaba.nacos.config.server.utils.GroupKey2;
import com.alibaba.nacos.config.server.utils.MD5;
import com.alibaba.nacos.config.server.utils.PropertyUtil;
import com.alibaba.nacos.config.server.utils.event.EventDispatcher;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.nacos.core.utils.SystemUtils.STANDALONE_MODE;
import static com.alibaba.nacos.config.server.utils.LogUtil.*;

/**
 * config service
 *
 * @author Nacos
 * 核心的配置服务
 * 这里还会将配置做持久化操作
 */
public class ConfigService {

    /**
     * 该对象负责与持久层交互
     */
    @Autowired
    private static PersistService persistService;

    static public int groupCount() {
        return CACHE.size();
    }

    /**
     * 当前是否有某个 group 相关的配置项
     * @param groupKey
     * @return
     */
    static public boolean hasGroupKey(String groupKey) {
        return CACHE.containsKey(groupKey);
    }

    /**
     * 保存配置文件，并缓存md5.
     */
    static public boolean dump(String dataId, String group, String tenant, String content, long lastModifiedTs, String type) {
        // 根据这些能确定唯一用户的信息 生成 groupKey
        String groupKey = GroupKey2.getKey(dataId, group, tenant);
        // 先确保缓存项已经被创建 (此时该对象不一定有数据)
        CacheItem ci = makeSure(groupKey);
        // 设置类型信息
        ci.setType(type);
        final int lockResult = tryWriteLock(groupKey);
        assert (lockResult != 0);

        // 代表尝试获取锁失败 无法进行存储
        if (lockResult < 0) {
            dumpLog.warn("[dump-error] write lock failed. {}", groupKey);
            return false;
        }

        try {
            // 根据当前配置内容生成md5  这样只需要校验md5是否发生变化就能直到 配置项是否变化
            final String md5 = MD5.getInstance().getMD5String(content);
            // 配置相同的情况 不需要写入磁盘
            if (md5.equals(ConfigService.getContentMd5(groupKey))) {
                dumpLog.warn(
                    "[dump-ignore] ignore to save cache file. groupKey={}, md5={}, lastModifiedOld={}, "
                        + "lastModifiedNew={}",
                    groupKey, md5, ConfigService.getLastModifiedTs(groupKey), lastModifiedTs);
            } else if (!STANDALONE_MODE || PropertyUtil.isStandaloneUseMysql()) {
                // 满足写入磁盘条件   写入到磁盘后 更新缓存中的数据
                DiskUtil.saveToDisk(dataId, group, tenant, content);
            }
            // 这里只是更新缓存
            updateMd5(groupKey, md5, lastModifiedTs);
            return true;
        } catch (IOException ioe) {
            dumpLog.error("[dump-exception] save disk error. " + groupKey + ", " + ioe.toString(), ioe);
            if (ioe.getMessage() != null) {
                String errMsg = ioe.getMessage();
                if (NO_SPACE_CN.equals(errMsg) || NO_SPACE_EN.equals(errMsg) || errMsg.contains(DISK_QUATA_CN)
                    || errMsg.contains(DISK_QUATA_EN)) {
                    // 磁盘写满保护代码
                    fatalLog.error("磁盘满自杀退出", ioe);
                    System.exit(0);
                }
            }
            return false;
        } finally {
            releaseWriteLock(groupKey);
        }
    }

    /**
     * 保存配置文件，并缓存md5.
     */
    static public boolean dumpBeta(String dataId, String group, String tenant, String content, long lastModifiedTs,
                                   String betaIps) {
        final String groupKey = GroupKey2.getKey(dataId, group, tenant);

        makeSure(groupKey);
        final int lockResult = tryWriteLock(groupKey);
        assert (lockResult != 0);

        if (lockResult < 0) {
            dumpLog.warn("[dump-beta-error] write lock failed. {}", groupKey);
            return false;
        }

        try {
            final String md5 = MD5.getInstance().getMD5String(content);
            if (md5.equals(ConfigService.getContentBetaMd5(groupKey))) {
                dumpLog.warn(
                    "[dump-beta-ignore] ignore to save cache file. groupKey={}, md5={}, lastModifiedOld={}, "
                        + "lastModifiedNew={}",
                    groupKey, md5, ConfigService.getLastModifiedTs(groupKey), lastModifiedTs);
            } else if (!STANDALONE_MODE || PropertyUtil.isStandaloneUseMysql()) {
                DiskUtil.saveBetaToDisk(dataId, group, tenant, content);
            }
            String[] betaIpsArr = betaIps.split(",");

            updateBetaMd5(groupKey, md5, Arrays.asList(betaIpsArr), lastModifiedTs);
            return true;
        } catch (IOException ioe) {
            dumpLog.error("[dump-beta-exception] save disk error. " + groupKey + ", " + ioe.toString(),
                ioe);
            return false;
        } finally {
            releaseWriteLock(groupKey);
        }
    }

    /**
     * 保存配置文件，并缓存md5.
     */
    static public boolean dumpTag(String dataId, String group, String tenant, String tag, String content,
                                  long lastModifiedTs) {
        final String groupKey = GroupKey2.getKey(dataId, group, tenant);

        makeSure(groupKey);
        final int lockResult = tryWriteLock(groupKey);
        assert (lockResult != 0);

        if (lockResult < 0) {
            dumpLog.warn("[dump-tag-error] write lock failed. {}", groupKey);
            return false;
        }

        try {
            final String md5 = MD5.getInstance().getMD5String(content);
            if (md5.equals(ConfigService.getContentTagMd5(groupKey, tag))) {
                dumpLog.warn(
                    "[dump-tag-ignore] ignore to save cache file. groupKey={}, md5={}, lastModifiedOld={}, "
                        + "lastModifiedNew={}",
                    groupKey, md5, ConfigService.getLastModifiedTs(groupKey), lastModifiedTs);
            } else if (!STANDALONE_MODE || PropertyUtil.isStandaloneUseMysql()) {
                DiskUtil.saveTagToDisk(dataId, group, tenant, tag, content);
            }

            updateTagMd5(groupKey, tag, md5, lastModifiedTs);
            return true;
        } catch (IOException ioe) {
            dumpLog.error("[dump-tag-exception] save disk error. " + groupKey + ", " + ioe.toString(),
                ioe);
            return false;
        } finally {
            releaseWriteLock(groupKey);
        }
    }

    // 上面的3个方法都是一个套路

    /**
     * 保存配置文件 刷新缓存 发出一个配置变更事件 用于通知client
     */
    static public boolean dumpChange(String dataId, String group, String tenant, String content, long lastModifiedTs) {
        final String groupKey = GroupKey2.getKey(dataId, group, tenant);

        makeSure(groupKey);
        final int lockResult = tryWriteLock(groupKey);
        assert (lockResult != 0);

        if (lockResult < 0) {
            dumpLog.warn("[dump-error] write lock failed. {}", groupKey);
            return false;
        }

        try {
            final String md5 = MD5.getInstance().getMD5String(content);
            if (!STANDALONE_MODE || PropertyUtil.isStandaloneUseMysql()) {
                String loacalMd5 = DiskUtil.getLocalConfigMd5(dataId, group, tenant);
                if (md5.equals(loacalMd5)) {
                    dumpLog.warn(
                        "[dump-ignore] ignore to save cache file. groupKey={}, md5={}, lastModifiedOld={}, "
                            + "lastModifiedNew={}",
                        groupKey, md5, ConfigService.getLastModifiedTs(groupKey), lastModifiedTs);
                } else {
                    DiskUtil.saveToDisk(dataId, group, tenant, content);
                }
            }
            updateMd5(groupKey, md5, lastModifiedTs);
            return true;
        } catch (IOException ioe) {
            dumpLog.error("[dump-exception] save disk error. " + groupKey + ", " + ioe.toString(),
                ioe);
            return false;
        } finally {
            releaseWriteLock(groupKey);
        }
    }

    /**
     * 重新加载配置
     */
    static public void reloadConfig() {
        String aggreds = null;
        try {
            // 这里有一个优先级问题  每个节点都会定期同数据库的数据同步 之后写入到本地文件 一般情况下面对client的请求都是通过 基于配置文件同步的缓存数据
            // 这样就减小了对数据库的读压力

            // 单机模式下   单机模式可能隐式代表着处于非正式环境 或者说访问量必然小 那么直接让数据库承担读压力就好
            if (STANDALONE_MODE && !PropertyUtil.isStandaloneUseMysql()) {
                // 查找默认数据
                ConfigInfoBase config = persistService.findConfigInfoBase(AggrWhitelist.AGGRIDS_METADATA,
                    "DEFAULT_GROUP");
                if (config != null) {
                    aggreds = config.getContent();
                }
            } else {
                // 基于 二级缓存(本地文件) 读取数据
                aggreds = DiskUtil.getConfig(AggrWhitelist.AGGRIDS_METADATA,
                    "DEFAULT_GROUP", StringUtils.EMPTY);
            }
            if (aggreds != null) {
                // 将数据加载到  一级缓存（内存缓存）
                AggrWhitelist.load(aggreds);
            }
        } catch (IOException e) {
            dumpLog.error("reload fail:" + AggrWhitelist.AGGRIDS_METADATA, e);
        }

        /**
         * 从数据库读取clientip 相关的白名单填充到容器中
         */
        String clientIpWhitelist = null;
        try {
            if (STANDALONE_MODE && !PropertyUtil.isStandaloneUseMysql()) {
                ConfigInfoBase config = persistService.findConfigInfoBase(
                    ClientIpWhiteList.CLIENT_IP_WHITELIST_METADATA, "DEFAULT_GROUP");
                if (config != null) {
                    clientIpWhitelist = config.getContent();
                }
            } else {
                clientIpWhitelist = DiskUtil.getConfig(ClientIpWhiteList.CLIENT_IP_WHITELIST_METADATA, "DEFAULT_GROUP",
                    StringUtils.EMPTY);
            }
            if (clientIpWhitelist != null) {
                ClientIpWhiteList.load(clientIpWhitelist);
            }
        } catch (IOException e) {
            dumpLog.error("reload fail:"
                + ClientIpWhiteList.CLIENT_IP_WHITELIST_METADATA, e);
        }

        /**
         * 填充开关信息
         */
        String switchContent = null;
        try {
            if (STANDALONE_MODE && !PropertyUtil.isStandaloneUseMysql()) {
                ConfigInfoBase config = persistService.findConfigInfoBase(SwitchService.SWITCH_META_DATAID,
                    "DEFAULT_GROUP");
                if (config != null) {
                    switchContent = config.getContent();
                }
            } else {
                switchContent = DiskUtil.getConfig(
                    SwitchService.SWITCH_META_DATAID, "DEFAULT_GROUP", StringUtils.EMPTY);
            }
            if (switchContent != null) {
                SwitchService.load(switchContent);
            }
        } catch (IOException e) {
            dumpLog.error("reload fail:" + SwitchService.SWITCH_META_DATAID, e);
        }

    }

    /**
     * 检验缓存中的数据是否与磁盘的一致
     * @return
     */
    static public List<String> checkMd5() {
        List<String> diffList = new ArrayList<String>();
        long startTime = System.currentTimeMillis();
        for (Entry<String/* groupKey */, CacheItem> entry : CACHE.entrySet()) {
            String groupKey = entry.getKey();
            String[] dg = GroupKey.parseKey(groupKey);
            String dataId = dg[0];
            String group = dg[1];
            String tenant = dg[2];
            try {
                String loacalMd5 = DiskUtil.getLocalConfigMd5(dataId, group, tenant);
                if (!entry.getValue().md5.equals(loacalMd5)) {
                    defaultLog.warn("[md5-different] dataId:{},group:{}",
                        dataId, group);
                    diffList.add(groupKey);
                }
            } catch (IOException e) {
                defaultLog.error("getLocalConfigMd5 fail,dataId:{},group:{}",
                    dataId, group);
            }
        }
        long endTime = System.currentTimeMillis();
        defaultLog.warn("checkMd5 cost:{}; diffCount:{}", endTime - startTime,
            diffList.size());
        return diffList;
    }

    /**
     * 删除配置文件，删除缓存。
     */
    static public boolean remove(String dataId, String group, String tenant) {
        final String groupKey = GroupKey2.getKey(dataId, group, tenant);
        final int lockResult = tryWriteLock(groupKey);
        /**
         *  数据不存在
         */
        if (0 == lockResult) {
            dumpLog.info("[remove-ok] {} not exist.", groupKey);
            return true;
        }
        /**
         * 加锁失败
         */
        if (lockResult < 0) {
            dumpLog.warn("[remove-error] write lock failed. {}", groupKey);
            return false;
        }

        try {
            // 先删除文件再删除缓存
            if (!STANDALONE_MODE || PropertyUtil.isStandaloneUseMysql()) {
                DiskUtil.removeConfigInfo(dataId, group, tenant);
            }
            CACHE.remove(groupKey);
            EventDispatcher.fireEvent(new LocalDataChangeEvent(groupKey));

            return true;
        } finally {
            releaseWriteLock(groupKey);
        }
    }

    /**
     * 删除配置文件，删除缓存。
     */
    static public boolean removeBeta(String dataId, String group, String tenant) {
        final String groupKey = GroupKey2.getKey(dataId, group, tenant);
        final int lockResult = tryWriteLock(groupKey);
        /**
         *  数据不存在
         */
        if (0 == lockResult) {
            dumpLog.info("[remove-ok] {} not exist.", groupKey);
            return true;
        }
        /**
         *  加锁失败
         */
        if (lockResult < 0) {
            dumpLog.warn("[remove-error] write lock failed. {}", groupKey);
            return false;
        }

        try {
            // 集群模式下 才使用本地文件
            if (!STANDALONE_MODE || PropertyUtil.isStandaloneUseMysql()) {
                DiskUtil.removeConfigInfo4Beta(dataId, group, tenant);
            }
            EventDispatcher.fireEvent(new LocalDataChangeEvent(groupKey, true, CACHE.get(groupKey).getIps4Beta()));
            CACHE.get(groupKey).setBeta(false);
            CACHE.get(groupKey).setIps4Beta(null);
            CACHE.get(groupKey).setMd54Beta(Constants.NULL);
            return true;
        } finally {
            releaseWriteLock(groupKey);
        }
    }

    /**
     * 删除配置文件，删除缓存。
     */
    static public boolean removeTag(String dataId, String group, String tenant, String tag) {
        final String groupKey = GroupKey2.getKey(dataId, group, tenant);
        final int lockResult = tryWriteLock(groupKey);
        /**
         *  数据不存在
         */
        if (0 == lockResult) {
            dumpLog.info("[remove-ok] {} not exist.", groupKey);
            return true;
        }
        /**
         *  加锁失败
         */
        if (lockResult < 0) {
            dumpLog.warn("[remove-error] write lock failed. {}", groupKey);
            return false;
        }

        try {
            if (!STANDALONE_MODE || PropertyUtil.isStandaloneUseMysql()) {
                DiskUtil.removeConfigInfo4Tag(dataId, group, tenant, tag);
            }

            CacheItem ci = CACHE.get(groupKey);
            ci.tagMd5.remove(tag);
            ci.tagLastModifiedTs.remove(tag);
            EventDispatcher.fireEvent(new LocalDataChangeEvent(groupKey, false, null, tag));
            return true;
        } finally {
            releaseWriteLock(groupKey);
        }
    }

    /**
     * 发布一个需要更新的事件   同时更新缓存
     * @param groupKey
     * @param md5
     * @param lastModifiedTs
     */
    public static void updateMd5(String groupKey, String md5, long lastModifiedTs) {
        CacheItem cache = makeSure(groupKey);
        if (cache.md5 == null || !cache.md5.equals(md5)) {
            cache.md5 = md5;
            cache.lastModifiedTs = lastModifiedTs;
            EventDispatcher.fireEvent(new LocalDataChangeEvent(groupKey));
        }
    }

    public static void updateBetaMd5(String groupKey, String md5, List<String> ips4Beta, long lastModifiedTs) {
        CacheItem cache = makeSure(groupKey);
        if (cache.md54Beta == null || !cache.md54Beta.equals(md5)) {
            cache.isBeta = true;
            cache.md54Beta = md5;
            cache.lastModifiedTs4Beta = lastModifiedTs;
            cache.ips4Beta = ips4Beta;
            EventDispatcher.fireEvent(new LocalDataChangeEvent(groupKey, true, ips4Beta));
        }
    }

    public static void updateTagMd5(String groupKey, String tag, String md5, long lastModifiedTs) {
        CacheItem cache = makeSure(groupKey);
        if (cache.tagMd5 == null) {
            Map<String, String> tagMd5Tmp = new HashMap<String, String>(1);
            tagMd5Tmp.put(tag, md5);
            cache.tagMd5 = tagMd5Tmp;
            if (cache.tagLastModifiedTs == null) {
                Map<String, Long> tagLastModifiedTsTmp = new HashMap<String, Long>(1);
                tagLastModifiedTsTmp.put(tag, lastModifiedTs);
                cache.tagLastModifiedTs = tagLastModifiedTsTmp;
            } else {
                cache.tagLastModifiedTs.put(tag, lastModifiedTs);
            }
            EventDispatcher.fireEvent(new LocalDataChangeEvent(groupKey, false, null, tag));
            return;
        }
        if (cache.tagMd5.get(tag) == null || !cache.tagMd5.get(tag).equals(md5)) {
            cache.tagMd5.put(tag, md5);
            cache.tagLastModifiedTs.put(tag, lastModifiedTs);
            EventDispatcher.fireEvent(new LocalDataChangeEvent(groupKey, false, null, tag));
        }
    }

    /**
     * 返回cache的md5。零长度字符串表示没有该数据。
     */
    static public String getContentMd5(String groupKey) {
        CacheItem item = CACHE.get(groupKey);
        return (null != item) ? item.md5 : Constants.NULL;
    }

    /**
     * 返回cache的md5。零长度字符串表示没有该数据。
     */
    static public String getContentBetaMd5(String groupKey) {
        CacheItem item = CACHE.get(groupKey);
        return (null != item) ? item.md54Beta : Constants.NULL;
    }

    /**
     * 返回cache的md5。零长度字符串表示没有该数据。
     */
    static public String getContentTagMd5(String groupKey, String tag) {
        CacheItem item = CACHE.get(groupKey);
        if (item == null) {
            return Constants.NULL;
        }
        if (item.tagMd5 == null) {
            return Constants.NULL;
        }
        return item.tagMd5.get(tag);
    }

    /**
     * 返回beta Ip列表
     */
    static public List<String> getBetaIps(String groupKey) {
        CacheItem item = CACHE.get(groupKey);
        return (null != item) ? item.getIps4Beta() : Collections.<String>emptyList();
    }

    /**
     * 返回cache。
     */
    static public CacheItem getContentCache(String groupKey) {
        return CACHE.get(groupKey);
    }

    /**
     * 获取本地配置
     * @param groupKey
     * @param ip
     * @param tag
     * @return
     */
    static public String getContentMd5(String groupKey, String ip, String tag) {
        // 首先尝试从缓存中获取
        CacheItem item = CACHE.get(groupKey);
        // 如果是 beta版本 并且 本次尝试订阅的client 刚好属于beta的节点 那么返回当前配置md5处理后的值
        if (item != null && item.isBeta) {
            if (item.ips4Beta.contains(ip)) {
                return item.md54Beta;
            }
        }
        // 返回 tag版本 的md5数据
        if (item != null && item.tagMd5 != null && item.tagMd5.size() > 0) {
            if (StringUtils.isNotBlank(tag) && item.tagMd5.containsKey(tag)) {
                return item.tagMd5.get(tag);
            }
        }
        return (null != item) ? item.md5 : Constants.NULL;
    }

    static public long getLastModifiedTs(String groupKey) {
        CacheItem item = CACHE.get(groupKey);
        return (null != item) ? item.lastModifiedTs : 0L;
    }

    static public boolean isUptodate(String groupKey, String md5) {
        String serverMd5 = ConfigService.getContentMd5(groupKey);
        return StringUtils.equals(md5, serverMd5);
    }

    /**
     * 根据groupKey 等 唯一信息 从本机中找到当前配置项 并与传入的md5进行比较
     * @param groupKey
     * @param md5
     * @param ip
     * @param tag
     * @return
     */
    static public boolean isUptodate(String groupKey, String md5, String ip, String tag) {
        String serverMd5 = ConfigService.getContentMd5(groupKey, ip, tag);
        return StringUtils.equals(md5, serverMd5);
    }

    /**
     * 给数据加读锁。如果成功，后面必须调用{@link #releaseReadLock(String)}，失败则不需要。
     *
     * @param groupKey
     * @return 零表示没有数据，失败。正数表示成功，负数表示有写锁导致加锁失败。
     */
    static public int tryReadLock(String groupKey) {
        CacheItem groupItem = CACHE.get(groupKey);
        int result = (null == groupItem) ? 0 : (groupItem.rwLock.tryReadLock() ? 1 : -1);
        if (result < 0) {
            defaultLog.warn("[read-lock] failed, {}, {}", result, groupKey);
        }
        return result;
    }

    static public void releaseReadLock(String groupKey) {
        CacheItem item = CACHE.get(groupKey);
        if (null != item) {
            item.rwLock.releaseReadLock();
        }
    }

    /**
     * 给数据加写锁。如果成功，后面必须调用{@link #releaseWriteLock(String)}，失败则不需要。
     *
     * @param groupKey
     * @return 零表示没有数据，失败。正数表示成功，负数表示加锁失败。
     */
    static int tryWriteLock(String groupKey) {
        CacheItem groupItem = CACHE.get(groupKey);
        // 这里每个缓存项对应一个锁对象
        int result = (null == groupItem) ? 0 : (groupItem.rwLock.tryWriteLock() ? 1 : -1);
        if (result < 0) {
            defaultLog.warn("[write-lock] failed, {}, {}", result, groupKey);
        }
        return result;
    }

    static void releaseWriteLock(String groupKey) {
        CacheItem groupItem = CACHE.get(groupKey);
        if (null != groupItem) {
            groupItem.rwLock.releaseWriteLock();
        }
    }

    static CacheItem makeSure(final String groupKey) {
        CacheItem item = CACHE.get(groupKey);
        if (null != item) {
            return item;
        }
        CacheItem tmp = new CacheItem(groupKey);
        item = CACHE.putIfAbsent(groupKey, tmp);
        return (null == item) ? tmp : item;
    }

    private final static String NO_SPACE_CN = "设备上没有空间";
    private final static String NO_SPACE_EN = "No space left on device";
    private final static String DISK_QUATA_CN = "超出磁盘限额";
    private final static String DISK_QUATA_EN = "Disk quota exceeded";
    static final Logger log = LoggerFactory.getLogger(ConfigService.class);
    /**
     * groupKey -> cacheItem
     */
    static private final ConcurrentHashMap<String, CacheItem> CACHE =
        new ConcurrentHashMap<String, CacheItem>();
}

