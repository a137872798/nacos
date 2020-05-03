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

import com.alibaba.nacos.config.server.monitor.MetricsMonitor;
import com.alibaba.nacos.config.server.utils.PropertyUtil;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.alibaba.nacos.config.server.service.PersistService.CONFIG_INFO4BETA_ROW_MAPPER;
import static com.alibaba.nacos.config.server.utils.LogUtil.defaultLog;
import static com.alibaba.nacos.config.server.utils.LogUtil.fatalLog;
import static com.alibaba.nacos.core.utils.SystemUtils.STANDALONE_MODE;

/**
 * Base data source
 *
 * @author Nacos
 * 主从数据源
 */
@Service("basicDataSourceService")
public class BasicDataSourceServiceImpl implements DataSourceService {

    private static final Logger log = LoggerFactory.getLogger(BasicDataSourceServiceImpl.class);
    // 分别是 旧版和 新版的 jdbc引擎
    private static final String DEFAULT_MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static final String MYSQL_HIGH_LEVEL_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static String JDBC_DRIVER_NAME;

    /**
     * JDBC执行超时时间, 单位秒
     */
    private int queryTimeout = 3;

    private static final int TRANSACTION_QUERY_TIMEOUT = 5;

    private static final String DB_LOAD_ERROR_MSG = "[db-load-error]load jdbc.properties error";

    // 内部存放了一组数据源  该数据源是由apache提供的
    private List<BasicDataSource> dataSourceList = new ArrayList<BasicDataSource>();

    // 这3个属性对应master
    private JdbcTemplate jt;
    private DataSourceTransactionManager tm;
    private TransactionTemplate tjt;

    private JdbcTemplate testMasterJT;
    private JdbcTemplate testMasterWritableJT;

    /**
     * 有多少db节点就有多少  jt 对象
     */
    volatile private List<JdbcTemplate> testJTList;
    /**
     * 对应每个 jt 当前状态
     */
    volatile private List<Boolean> isHealthList;
    /**
     * 主节点的下标
     */
    private volatile int masterIndex;
    private static Pattern ipPattern = Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");


    @Autowired
    private Environment env;


    static {
        try {
            Class.forName(MYSQL_HIGH_LEVEL_DRIVER);
            JDBC_DRIVER_NAME = MYSQL_HIGH_LEVEL_DRIVER;
            log.info("Use Mysql 8 as the driver");
        } catch (ClassNotFoundException e) {
            log.info("Use Mysql as the driver");
            JDBC_DRIVER_NAME = DEFAULT_MYSQL_DRIVER;
        }
    }

    @PostConstruct
    public void init() {
        // 根据系统变量 更新查询超时时间
        queryTimeout = NumberUtils.toInt(System.getProperty("QUERYTIMEOUT"), 3);
        // 设置jdbc 模板对象
        jt = new JdbcTemplate();
        /**
         *  设置最大记录数，防止内存膨胀
         */
        jt.setMaxRows(50000);
        jt.setQueryTimeout(queryTimeout);

        // 创建一个查询master的sql
        testMasterJT = new JdbcTemplate();
        testMasterJT.setQueryTimeout(queryTimeout);

        testMasterWritableJT = new JdbcTemplate();
        /**
         * 防止login接口因为主库不可用而rt太长
         */
        testMasterWritableJT.setQueryTimeout(1);
        /**
         * 数据库健康检测
         */
        testJTList = new ArrayList<JdbcTemplate>();
        isHealthList = new ArrayList<Boolean>();

        tm = new DataSourceTransactionManager();
        // 事务模板对象
        tjt = new TransactionTemplate(tm);
        /**
         *  事务的超时时间需要与普通操作区分开
         */
        tjt.setTimeout(TRANSACTION_QUERY_TIMEOUT);
        // 如果是基于 mysql 或者 非单机模式 加载某些数据
        if (!STANDALONE_MODE || PropertyUtil.isStandaloneUseMysql()) {
            try {
                reload();
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(DB_LOAD_ERROR_MSG);
            }

            // 按照一定的间隔时间 持续执行任务
            TimerTaskService.scheduleWithFixedDelay(new SelectMasterTask(), 10, 10,
                TimeUnit.SECONDS);
            TimerTaskService.scheduleWithFixedDelay(new CheckDBHealthTask(), 10, 10,
                TimeUnit.SECONDS);
        }
    }

    @Override
    public synchronized void reload() throws IOException {
        List<BasicDataSource> dblist = new ArrayList<BasicDataSource>();
        try {
            String val = null;
            // 获取db节点数量
            val = env.getProperty("db.num");
            if (null == val) {
                throw new IllegalArgumentException("db.num is null");
            }
            int dbNum = Integer.parseInt(val.trim());

            // 根据节点数创建对应的数据源  根据下标使用不同的配置
            for (int i = 0; i < dbNum; i++) {
                BasicDataSource ds = new BasicDataSource();
                ds.setDriverClassName(JDBC_DRIVER_NAME);

                val = env.getProperty("db.url." + i);
                if (null == val) {
                    fatalLog.error("db.url." + i + " is null");
                    throw new IllegalArgumentException();
                }
                ds.setUrl(val.trim());

                val = env.getProperty("db.user." + i, env.getProperty("db.user"));
                if (null == val) {
                    fatalLog.error("db.user." + i + " is null");
                    throw new IllegalArgumentException();
                }
                ds.setUsername(val.trim());

                val = env.getProperty("db.password." + i, env.getProperty("db.password"));
                if (null == val) {
                    fatalLog.error("db.password." + i + " is null");
                    throw new IllegalArgumentException();
                }
                ds.setPassword(val.trim());

                val = env.getProperty("db.initialSize." + i, env.getProperty("db.initialSize"));
                ds.setInitialSize(Integer.parseInt(defaultIfNull(val, "10")));

                val = env.getProperty("db.maxActive." + i, env.getProperty("db.maxActive"));
                ds.setMaxActive(Integer.parseInt(defaultIfNull(val, "20")));

                val = env.getProperty("db.maxIdle." + i, env.getProperty("db.maxIdle"));
                ds.setMaxIdle(Integer.parseInt(defaultIfNull(val, "50")));

                ds.setMaxWait(3000L);
                ds.setPoolPreparedStatements(true);

                // 每10分钟检查一遍连接池
                ds.setTimeBetweenEvictionRunsMillis(TimeUnit.MINUTES
                    .toMillis(10L));
                // 开启在空闲时 测试连接
                ds.setTestWhileIdle(true);
                ds.setValidationQuery("SELECT 1 FROM dual");

                dblist.add(ds);

                JdbcTemplate jdbcTemplate = new JdbcTemplate();
                jdbcTemplate.setQueryTimeout(queryTimeout);
                jdbcTemplate.setDataSource(ds);

                testJTList.add(jdbcTemplate);
                isHealthList.add(Boolean.TRUE);
            }

            if (dblist == null || dblist.size() == 0) {
                throw new RuntimeException("no datasource available");
            }

            dataSourceList = dblist;
            // 初始化完成时 执行任务   此时启动了所有db 后还没有确定哪个才是 master节点
            new SelectMasterTask().run();
            new CheckDBHealthTask().run();
        } catch (RuntimeException e) {
            fatalLog.error(DB_LOAD_ERROR_MSG, e);
            throw new IOException(e);
        } finally {
        }
    }

    /**
     * 检查当前主节点是否可写入
     * @return
     */
    @Override
    public boolean checkMasterWritable() {

        // 获取 master节点
        testMasterWritableJT.setDataSource(jt.getDataSource());
        /**
         *  防止login接口因为主库不可用而rt太长
         */
        testMasterWritableJT.setQueryTimeout(1);
        String sql = " SELECT @@read_only ";

        try {
            // 检查该节点是否允许写入数据
            Integer result = testMasterWritableJT.queryForObject(sql, Integer.class);
            if (result == null) {
                return false;
            } else {
                return result == 0;
            }
        } catch (CannotGetJdbcConnectionException e) {
            fatalLog.error("[db-error] " + e.toString(), e);
            return false;
        }

    }

    @Override
    public JdbcTemplate getJdbcTemplate() {
        return this.jt;
    }

    @Override
    public TransactionTemplate getTransactionTemplate() {
        return this.tjt;
    }

    @Override
    public String getCurrentDBUrl() {
        DataSource ds = this.jt.getDataSource();
        if (ds == null) {
            return StringUtils.EMPTY;
        }
        BasicDataSource bds = (BasicDataSource) ds;
        return bds.getUrl();
    }

    @Override
    public String getHealth() {
        for (int i = 0; i < isHealthList.size(); i++) {
            if (!isHealthList.get(i)) {
                if (i == masterIndex) {
                    /**
                     * 主库不健康
                     */
                    return "DOWN:" + getIpFromUrl(dataSourceList.get(i).getUrl());
                } else {
                    /**
                     * 从库不健康
                     */
                    return "WARN:" + getIpFromUrl(dataSourceList.get(i).getUrl());
                }
            }
        }

        return "UP";
    }

    private String getIpFromUrl(String url) {

        Matcher m = ipPattern.matcher(url);
        if (m.find()) {
            return m.group();
        }

        return "";
    }

    static String defaultIfNull(String value, String defaultValue) {
        return null == value ? defaultValue : value;
    }

    /**
     * 从当前所有可用db中选择一个作为master
     */
    class SelectMasterTask implements Runnable {

        @Override
        public void run() {
            if (defaultLog.isDebugEnabled()) {
                defaultLog.debug("check master db.");
            }
            boolean isFound = false;

            int index = -1;
            // 只有master节点能够正常执行该操作
            for (BasicDataSource ds : dataSourceList) {
                index++;
                // 该模板对象专门用于寻找master
                testMasterJT.setDataSource(ds);
                testMasterJT.setQueryTimeout(queryTimeout);
                try {
                    testMasterJT
                        .update(
                            "DELETE FROM config_info WHERE data_id='com.alibaba.nacos.testMasterDB'");
                    if (jt.getDataSource() != ds) {
                        fatalLog.warn("[master-db] {}", ds.getUrl());
                    }
                    jt.setDataSource(ds);
                    tm.setDataSource(ds);
                    isFound = true;
                    masterIndex = index;
                    break;
                } catch (DataAccessException e) { // read only
                    e.printStackTrace(); // TODO remove
                }
            }

            if (!isFound) {
                fatalLog.error("[master-db] master db not found.");
                MetricsMonitor.getDbException().increment();
            }
        }
    }

    /**
     * 该对象负责确保db处于可用状态
     */
    @SuppressWarnings("PMD.ClassNamingShouldBeCamelRule")
    class CheckDBHealthTask implements Runnable {

        @Override
        public void run() {
            if (defaultLog.isDebugEnabled()) {
                defaultLog.debug("check db health.");
            }
            String sql = "SELECT * FROM config_info_beta WHERE id = 1";

            // 专门使用测试模板去执行语句
            for (int i = 0; i < testJTList.size(); i++) {
                JdbcTemplate jdbcTemplate = testJTList.get(i);
                try {
                    // CONFIG_INFO4BETA_ROW_MAPPER 该对象用于将 数据字段转换成 实体对象
                    jdbcTemplate.query(sql, CONFIG_INFO4BETA_ROW_MAPPER);
                    isHealthList.set(i, Boolean.TRUE);
                } catch (DataAccessException e) {
                    if (i == masterIndex) {
                        fatalLog.error("[db-error] master db {} down.",
                            getIpFromUrl(dataSourceList.get(i).getUrl()));
                    } else {
                        fatalLog.error("[db-error] slave db {} down.",
                            getIpFromUrl(dataSourceList.get(i).getUrl()));
                    }
                    isHealthList.set(i, Boolean.FALSE);

                    MetricsMonitor.getDbException().increment();
                }
            }
        }
    }
}
