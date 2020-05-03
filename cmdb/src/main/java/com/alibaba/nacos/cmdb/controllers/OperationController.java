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
package com.alibaba.nacos.cmdb.controllers;

import com.alibaba.nacos.cmdb.memory.CmdbProvider;
import com.alibaba.nacos.cmdb.utils.UtilsAndCommons;
import com.alibaba.nacos.core.utils.WebUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

/**
 * @author nkorange
 * @since 0.7.0
 */
@RestController
@RequestMapping(UtilsAndCommons.NACOS_CMDB_CONTEXT + "/ops")
public class OperationController {

    @Autowired
    private CmdbProvider cmdbProvider;

    @RequestMapping(value = "/label", method = RequestMethod.GET)
    public String queryLabel(HttpServletRequest request) throws Exception {
        // 从请求体中获取参数
        String entry = WebUtils.required(request, "entry");
        String label = WebUtils.required(request, "label");
        // 通过entry 和 label 查询数据
        return cmdbProvider.queryLabel(entry, "ip", label);
    }
}
