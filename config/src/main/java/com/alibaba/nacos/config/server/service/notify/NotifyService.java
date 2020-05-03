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
package com.alibaba.nacos.config.server.service.notify;

import com.alibaba.nacos.common.utils.IoUtils;
import com.alibaba.nacos.config.server.manager.TaskManager;
import com.alibaba.nacos.config.server.service.ServerListService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Iterator;
import java.util.List;

/**
 * 该对象只是负责与其他服务器通信 (基于http请求)
 * @author jiuRen
 * 这个类应该是被弃用了 没有使用 @Service 注解 同时 AsyncNotifyService 已经具备通知功能了
 */
public class NotifyService {

    @Autowired
    public NotifyService(ServerListService serverListService) {
        // 该对象维护了所有待执行的任务  以及对应的任务处理器
        notifyTaskManager = new TaskManager("com.alibaba.nacos.NotifyTaskManager");
        notifyTaskManager.setDefaultTaskProcessor(new NotifyTaskProcessor(serverListService));
    }

    protected NotifyService() {
    }

    /**
     * 為了方便系统beta，不改变notify.do接口，新增lastModifed参数通过Http header传递
     */
    static public final String NOTIFY_HEADER_LAST_MODIFIED = "lastModified";
    static public final String NOTIFY_HEADER_OP_HANDLE_IP = "opHandleIp";

    /**
     * 发起http请求并处理结果
     * @param url
     * @param headers
     * @param encoding
     * @return
     * @throws IOException
     */
    static public HttpResult invokeURL(String url, List<String> headers, String encoding) throws IOException {
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection)new URL(url).openConnection();

            conn.setConnectTimeout(TIMEOUT);
            conn.setReadTimeout(TIMEOUT);
            conn.setRequestMethod("GET");

            if (null != headers && !StringUtils.isEmpty(encoding)) {
                for (Iterator<String> iter = headers.iterator(); iter.hasNext(); ) {
                    conn.addRequestProperty(iter.next(), iter.next());
                }
            }
            conn.addRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=" + encoding);

            conn.connect();
            int respCode = conn.getResponseCode();
            String resp = null;

            if (HttpServletResponse.SC_OK == respCode) {
                resp = IoUtils.toString(conn.getInputStream(),encoding);
            } else {
                resp = IoUtils.toString(conn.getErrorStream(),encoding);
            }
            return new HttpResult(respCode, resp);
        } finally {
            IoUtils.closeQuietly(conn);
        }
    }

    static public class HttpResult {
        final public int code;
        final public String content;

        public HttpResult(int code, String content) {
            this.code = code;
            this.content = content;
        }
    }

    /**
     * 和其他server的连接超时和socket超时
     */
    static final int TIMEOUT = 500;

    private TaskManager notifyTaskManager;

}
