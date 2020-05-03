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

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.IOException;

/**
 * datasource interface
 *
 * @author Nacos
 * 数据源服务接口   配置中心需要一个能够提供配置的数据源
 */
public interface DataSourceService {
    /**
     * reload
     *
     * @throws IOException exception
     * 重新加载数据(配置)
     */
    void reload() throws IOException;

    /**
     * check master db
     *
     * @return is master
     * 检查 db当前是否处于可用状态
     */
    boolean checkMasterWritable();

    /**
     * get jdbc template
     *
     * @return JdbcTemplate
     * 获取jdbc模板
     */
    JdbcTemplate getJdbcTemplate();

    /**
     * get transaction template
     *
     * @return TransactionTemplate
     * 获取事务模板对象
     */
    TransactionTemplate getTransactionTemplate();

    /**
     * get current db url
     * 获取当前 db的url
     *
     * @return
     */
    String getCurrentDBUrl();

    /**
     * get heath
     *
     * @return heath info
     */
    String getHealth();
}
