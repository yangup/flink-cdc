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

package com.ververica.cdc.connectors.mysql.source;

import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.connectors.mysql.factory.MySqlDataSourceFactory;
import com.ververica.cdc.connectors.mysql.utils.MySqlTypeUtils;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.ververica.cdc.connectors.mysql.source.MySqlDataSourceOptions.*;
import static com.ververica.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_PASSWORD;
import static com.ververica.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link MySqlDataSourceFactory}.
 */
public class MySqlPipelineRecordEmitterTest {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlPipelineRecordEmitterTest.class);

    @Test
    public void testParseDDL() {
        String ddlStatement = "CREATE TABLE `t_yp_5` (\n" +
                "  `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '企业ID',\n" +
                "  `name_code_varchar_yp_5_1` varchar(2) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '企业杨普',\n" +
                "  PRIMARY KEY (`id`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='商户企业信息'";
        TableId tableId = new TableId("yangpu_6", "", "t_yp_5");
        LOG.info("parseDDL, ddlStatement: {}, tableId: {} ", ddlStatement, tableId);
        // 包含注释
        MySqlAntlrDdlParser mySqlAntlrDdlParser = new MySqlAntlrDdlParser(
                true,
                false,
                true,
                null,
                Tables.TableFilter.includeAll());
        mySqlAntlrDdlParser.setCurrentDatabase(tableId.catalog());
        Tables tables = new Tables();
        mySqlAntlrDdlParser.parse(ddlStatement, tables);
        Table table = tables.forTable(tableId);
//        Table table = parseDdl(ddlStatement, tableId);

        List<Column> columns = table.columns();
        Schema.Builder tableBuilder = Schema.newBuilder();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);

            String colName = column.name();
            DataType dataType = MySqlTypeUtils.fromDbzColumn(column);
            // todo : 日志, 在 task manager 中

            LOG.info("parseDDL, column: {}, result: {} ", column, dataType);
            if (!column.isOptional()) {
                dataType = dataType.notNull();
            }
            tableBuilder.physicalColumn(colName, dataType, column.comment());
        }
    }

}
