/*
 * Copyright 2023 Ververica Inc.
 *
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

package com.ververica.cdc.connectors.doris.sink;

//import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.cdc.common.data.TimestampData;
import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;
//import org.apache.commons.codec.binary.Base64;
import org.apache.doris.flink.exception.DorisSchemaChangeException;
import org.apache.doris.flink.rest.RestService;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A test for {@link DorisRowConverter} .
 */
public class SchemaChangeManagerTest {


    @Test
    public void testDorisExec() throws Exception {
//        ObjectMapper objectMapper = new ObjectMapper();
//        String ddl = "CREATE TABLE IF NOT EXISTS `yangpu_6`.`t_yp_525`(`id` BIGINT COMMENT '企业ID',`name_code_varchar_yp_5_1` VARCHAR(8) COMMENT '企业杨普' ) UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS AUTO  PROPERTIES ('replication_num'='1','light_schema_change'='true')";
//        System.out.println(ddl);
////        Map<String, String> param = new HashMap<>();
////        param.put("stmt", ddl);
////        System.out.println(objectMapper.writeValueAsString(param));;
//
//        String CHECK_SCHEMA_CHANGE_API = "http://%s/api/enable_light_schema_change/%s/%s";
//        String SCHEMA_CHANGE_API = "http://%s/api/query/default_cluster/%s";
//
//        Map<String, String> param = new HashMap<>();
//        param.put("stmt", ddl);
//        String requestUrl = String.format(SCHEMA_CHANGE_API, "192.168.58.4:8030", "yangpu_6");
//        HttpPost httpPost = new HttpPost(requestUrl);
//        httpPost.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + new String(Base64.encodeBase64("root:".getBytes(StandardCharsets.UTF_8))));
//        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
//        httpPost.setEntity(new StringEntity(objectMapper.writeValueAsString(param), StandardCharsets.UTF_8));
////        httpPost.setEntity(new StringEntity(objectMapper.writeValueAsString(param)));
//
//        CloseableHttpClient httpclient = HttpClients.createDefault();
//        CloseableHttpResponse response = httpclient.execute(httpPost);
//        final int statusCode = response.getStatusLine().getStatusCode();
//        final String reasonPhrase = response.getStatusLine().getReasonPhrase();
//        if (statusCode == 200 && response.getEntity() != null) {
//            String loadResult = EntityUtils.toString(response.getEntity());
//            Map<String, Object> responseMap = objectMapper.readValue(loadResult, Map.class);
//            String code = responseMap.getOrDefault("code", "-1").toString();
//            if (code.equals("0")) {
//                System.out.println(code);
//            } else {
//                throw new DorisSchemaChangeException("Failed to schemaChange, response: " + loadResult);
//            }
//        } else {
//            throw new DorisSchemaChangeException("Failed to schemaChange, status: " + statusCode + ", reason: " + reasonPhrase);
//        }


    }
}
