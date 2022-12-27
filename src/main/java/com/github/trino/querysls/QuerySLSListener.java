/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.trino.querysls;

import com.aliyun.openservices.aliyun.log.producer.Producer;
import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import com.aliyun.openservices.log.common.LogItem;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class QuerySLSListener implements EventListener {
    private final String queryCreateName;
    private final String queryCompletedName;
    private final String splitCompletedName;
    private final Producer producer;
    private final String slsProject;


    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicInteger successCount = new AtomicInteger(0);

    public static final String QUERY_CREATE_NAME = "event-listener.query.sls.queryCreatedName";
    public static final String QUERY_COMPLETED_NAME = "event-listener.query.sls.queryCompletedName";
    public static final String SPLIT_COMPLETED_NAME = "event-listener.query.sls.splitCompletedName";

    public static final String SLS_PROJECT = "event-listener.query.sls.project";
    public static final String SLS_ENDPOINT =  "event-listener.query.sls.endpoint";
    public static final String SLS_ACCESSKEY_ID = "event-listener.query.sls.accessKeyId";
    public static final String SLS_ACCESSKEY_SECRET = "event-listener.query.sls.accessKeySecret";

    public QuerySLSListener(Map<String, String> config) {

        String project = config.get(SLS_PROJECT);
        String endpoint =  config.get(SLS_ENDPOINT);
        String accessKeyId = config.get(SLS_ACCESSKEY_ID);
        String accessKeySecret = config.get(SLS_ACCESSKEY_SECRET);

        queryCreateName = config.getOrDefault(QUERY_CREATE_NAME, "false");
        queryCompletedName = config.getOrDefault(QUERY_COMPLETED_NAME, "false");
        splitCompletedName = config.getOrDefault(SPLIT_COMPLETED_NAME, "false");

        producer = Utils.createProducer(project, endpoint, accessKeyId, accessKeySecret);

        slsProject = project;

        objectMapper.registerModule(new Jdk8Module());
    }

    public void close() throws ProducerException, InterruptedException {
        producer.close();
    }

    private LogItem createLogItem(Object obj) {
        LogItem logitem = new LogItem();

        try {
            Field[] fields = obj.getClass().getDeclaredFields();

            for (Field field : fields) {
                field.setAccessible(true);

                String name = field.getName();
                Object value = field.get(obj);
                String data = objectMapper.writeValueAsString(value);

                logitem.PushBack(name, data);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return logitem;
    }

    private void sendEvent( String slsLogStore, LogItem logitem) throws ProducerException, InterruptedException {
        producer.send(
                slsProject,
                slsLogStore,
                logitem,
                result -> {
                    if (result.isSuccessful()) {
                        successCount.incrementAndGet();
                    } else {
                        System.out.println(result);
                    }
                }
        );
    }

    @Override
    public void queryCreated(final QueryCreatedEvent queryCreatedEvent) {
        boolean result = Utils.parseBoolean(queryCreateName);
        if (result) {
            try {
                LogItem logitem = createLogItem(queryCreatedEvent);
                sendEvent(queryCreateName, logitem);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void queryCompleted(final QueryCompletedEvent queryCompletedEvent) {
        boolean result = Utils.parseBoolean(queryCompletedName);
        if (result) {
            try {
                LogItem logitem = createLogItem(queryCompletedEvent);
                sendEvent(queryCompletedName, logitem);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void splitCompleted(final SplitCompletedEvent splitCompletedEvent) {
        boolean result = Utils.parseBoolean(splitCompletedName);
        if (result) {
            try {
                LogItem logitem = createLogItem(splitCompletedEvent);
                sendEvent(splitCompletedName, logitem);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
