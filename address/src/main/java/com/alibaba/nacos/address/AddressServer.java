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
package com.alibaba.nacos.address;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * support address server.
 *
 * @author nacos
 * @since 1.1.0
 * 以spring boot 的方式启动一个简单的地址服务 用于提供获取加工地址的基本功能
 * 避免以硬编码的方式设置集群的地址
 */
@SpringBootApplication(scanBasePackages = "com.alibaba.nacos")
public class AddressServer {
    public static void main(String[] args) {

        SpringApplication.run(AddressServer.class, args);
    }
}
