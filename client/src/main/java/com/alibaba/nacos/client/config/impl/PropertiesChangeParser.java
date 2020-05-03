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
package com.alibaba.nacos.client.config.impl;

import com.alibaba.nacos.api.config.ConfigChangeItem;
import com.alibaba.nacos.client.utils.StringUtils;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;
import java.util.Properties;

/**
 * PropertiesChangeParser
 *
 * @author rushsky518
 */
public class PropertiesChangeParser extends AbstractConfigChangeParser {
    public PropertiesChangeParser() {
        super("properties");
    }

    /**
     * 分别读取old 和 new 配置 并填充到map中
     * @param oldContent   看来配置是一个字符流啊 而不是一个文件
     * @param newContent
     * @param type
     * @return
     * @throws IOException
     */
    @Override
    public Map<String, ConfigChangeItem> doParse(String oldContent, String newContent, String type) throws IOException {
        Properties oldProps = new Properties();
        Properties newProps = new Properties();

        if (StringUtils.isNotBlank(oldContent)) {
            oldProps.load(new StringReader(oldContent));
        }
        if (StringUtils.isNotBlank(newContent)) {
            newProps.load(new StringReader(newContent));
        }

        return filterChangeData(oldProps, newProps);
    }
}
