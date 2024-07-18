/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.common.schema;

import org.apache.flink.cdc.common.event.TableId;

import org.apache.flink.cdc.common.utils.Predicates;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link org.apache.flink.cdc.common.schema.Selectors}.
 */
class SelectorsTest {


    @Test
    public void testTableSelectorCustom() {
//        String tableInclusions = "*.advance_config,\n" +
//                "*.af_equipment_callback,\n" +
//                "*.analyze_report,\n" +
//                "*.analyze_report_credit,\n" +
//                "*.analyze_report_loan,\n" +
//                "*.wa_source_log";
//        String tableInclusions = "mx_risk.(advance_config|af_equipment_callback|analyze_report)";
        String tableInclusions = "(eagle_wallet_android|eagle_wallet_ios|pera_bag_android|pera_bag_ios|perajet_ios|ph_dc|ph_dc_android_04|ph_dc_android_05|ph_dc_android_06|ph_dc_ios_05|ph_dc_ios_06|ph_fenqi|propeso_ios).(bank_card_info|entry_order|repay_plan|user_info),\n" +
                "    (eagle_wallet_android|eagle_wallet_ios|pera_bag_android|pera_bag_ios|perajet_ios|ph_dc|ph_dc_android_04|ph_dc_android_05|ph_dc_android_06|ph_dc_ios_05|ph_dc_ios_06|propeso_ios).(af_equipment_callback|af_equipment_market_report|app_buried_click_log|cash_merchant_product|cash_order|certify_click_log|certify_click_submit_log|certify_finished_log|daily_activity|page_event_info|register_point_log|user_frozen|user_login_log|user_login_otp_log|user_preprocess_log|user_send_message_log|user_verification),\n" +
                "    (ph_callcenter).(collection_repay|loan_collection_user_calllog|loan_collection_user_sms|tb_auto_call_collection_call_log|tb_collection_product_dispatch_log|tb_collection_reduce_list|tb_loan_collection|tb_loan_collection_order|tb_loan_collection_order_dispatch_log|tb_loan_collection_user_company),\n" +
                "    (ph_callcenter|ph_risk).(tb_admin_user),\n" +
                "    (ph_fenqi).(loan_order|loan_order_repay_log|pay_remit_result|pay_repay_result|product|rise_amount|risk_result),\n" +
                "    (ph_global).(app_list_oss_log|black_user_list|calendar_list_oss_log|collection_old_order_level|collection_order_level|customer_qa_record|customer_service_qa|dc_apply_intercept_log|dc_buried_point_history|dc_buried_point_log|dc_user_channel_tag|debit_bind_request_log|delay_click_log|delay_payment_log|delay_payment_log_java|face_feedback_log|global_common_debt_log|global_common_debt_log_java|global_credit_product_amount|global_credit_product_amount_bak|global_credit_product_amount_java|global_hong_bao|global_jiang_pay_code|global_low_rate_user|global_main_product_amount|global_user_certify_time|google_score|loan_order_package|loan_speed_limit_log|lottery_draw_log|lundu_merchant_info|market_conversion_report|market_transfer_report_platform|merchant_product_bank_list|message_setting|natural_user_register_monitor|package|pay_setting|platform_product_amount_ctrl|product_conversion_report|product_conversion_report_info|product_group_disable_log|product_package|rc_api_pre_process_log|repay_after_loan_app_link|repay_after_loan_link|repay_after_loan_product_link|repay_bank|repay_method_setting|repay_method_setting_new|repay_remit_flow|request_log|short_message_oss_log|sms_list_oss_log|sms_write_oss_log|statistics_dc_api_daily|summary_market_transfer_report_platform|tb_advance_api_request_log|tb_api_order_credit|tb_app_crash_log|tb_apply_google_channel_log|tb_customer_call_log|tb_customer_record_log|tb_google_channel_log|tb_register_google_channel_log|tb_statistics_dc|tb_telemarketing_call_log|telemarketing|telemarketing_compare_log|user_api_amount_log|user_block_all|user_channel_change_log|user_click_log|user_contacts_oss_log|user_credit_amount_log|user_device_info_oss|user_id_check|user_id_check_log|user_id_high_risk_log|user_info_channel_global|user_info_hash|user_main_amount_log|user_order_found|user_problem_info|user_type_config|user_type_package_config|user_type_package_config_log),\n" +
                "    (ph_global|ph_risk).(auto_call_log|dc_buried_point|dc_price|exchange_rate|merchant_account|merchant_account_balance|merchant_account_bill|merchant_incharge|merchant_update_log|setting|sms_log|tb_ext_white_list|tb_statistics|user_id_risk_level|user_id_risk_level_log|user_id_unpaid_limit|user_risk_tag|user_risk_tag_log),\n" +
                "    (ph_risk).(advance_config|analyze_report|analyze_report_credit|analyze_report_loan|analyze_report_loan_0709_wjl|analyze_report_repay|api_nano_log|api_request_log|app_class_type_list|app_detail_info|calendar_log|decision_tree|decision_tree_version|feature_tree|flink_check_delay|global_setting|google_play_packages|merchant_price|order_dispatch_log|order_risk_callback_log|order_risk_credit|order_risk_log|order_user_tag|package_not_exist|rc_app_code_table|rc_black_list|rc_black_list_category|rc_black_list_device|rc_black_list_device_base_adv|rc_black_list_device_ios|rc_black_list_hit_adv|rc_sms_code_table|rc_white_list|review_rollback|reviewer|reviewer_group|reviewer_order|runtime_feature_log|runtime_feature_log_bak|score_card|score_card_version|tb_admin_captcha_log|tb_admin_login_log|tb_admin_user_role|tb_all_order_sms_model_data|tb_api_request_log|tb_auth_items|tb_auth_org_data|tb_auth_source|tb_danger_code|tb_exception_code|tb_exception_code1|tb_forbid|tb_front_user|tb_manual_check|tb_model|tb_model_log|tb_order|tb_order_app_list|tb_order_delay_payment_log|tb_order_extend_log|tb_order_gps_log|tb_order_natural_count_log|tb_order_postback|tb_order_repayment|tb_order_sms_list|tb_order_sms_log|tb_order_status_log|tb_org_prod_phase_data|tb_orgs|tb_phase|tb_product|tb_product_sms|tb_risk_credit_log|tb_rule|tb_statistics_apply|tb_statistics_apply_user|tb_statistics_audit_performance|tb_statistics_loan|tb_statistics_machine_result_layout|tb_statistics_overdue_proportion|tb_statistics_process|tb_statistics_repay|tb_task_sai_order|tb_user|tb_user_captcha|tb_user_contacts|tb_user_contacts_new|tb_user_device|tb_user_device_ios|tb_user_emergent|tb_user_equipment|tb_user_info|tb_user_org|tb_user_phone|td_reject_reason_layout|user_amount|user_amount_log|wa_source_log),\n" +
                "    (propeso_ios).(user_feedback_cancel_log)";
        // error
//        String tableInclusions = "mx_risk.(advance_config\n" +
//                "|af_equipment_callback\n" +
//                "|analyze_report\n" +
//                ")";
//        Set<String> tableSplitSet =
//                Predicates.setOf(
//                        tableInclusions, Predicates.RegExSplitterByComma::split, (str) -> str);
//        for (String tableSplit : tableSplitSet) {
//            List<String> tableIdList =
//                    Predicates.listOf(
//                            tableSplit, Predicates.RegExSplitterByDot::split, (str) -> str);
//            Iterator<String> iterator = tableIdList.iterator();
//            System.out.println(tableIdList);
////            new Selectors.Selector(null, null, iterator.next())
//        }

        Selectors selectors =
                new Selectors.SelectorsBuilder()
                        .includeTables(tableInclusions)
                        .build();
//        assertAllowed(selectors, null, "mx_risk", "analyze_report");
//        assertAllowed(selectors, null, "mx_risk", "analyze_report");
//        assertAllowed(selectors, null, "mx_dc", "cash_order");
        assertAllowed(selectors, null, "ph_callcenter", "tb_admin_user");
//        assertAllowed(selectors, null, "ph_callcenter", "tb_admin_user");
//        assertAllowed(selectors, null, "mx_risk", "af_equipment_callback");

    }


    @Test
    public void testTableSelector() {

        // nameSpace, schemaName, tableName
        Selectors selectors =
                new Selectors.SelectorsBuilder()
                        .includeTables("db.sc1.A[0-9]+,db.sc2.B[0-1]+,db.sc1.sc1")
                        .build();

        assertAllowed(selectors, "db", "sc1", "sc1");
        assertAllowed(selectors, "db", "sc1", "A1");
        assertAllowed(selectors, "db", "sc1", "A2");
        assertAllowed(selectors, "db", "sc2", "B0");
        assertAllowed(selectors, "db", "sc2", "B1");
        assertNotAllowed(selectors, "db", "sc1", "A");
        assertNotAllowed(selectors, "db", "sc1a", "B");
        assertNotAllowed(selectors, "db", "sc1", "AA");
        assertNotAllowed(selectors, "db", "sc2", "B2");
        assertNotAllowed(selectors, "db2", "sc1", "A1");
        assertNotAllowed(selectors, "db2", "sc1", "A2");
        assertNotAllowed(selectors, "db", "sc11", "A1");
        assertNotAllowed(selectors, "db", "sc1A", "A1");

        selectors =
                new Selectors.SelectorsBuilder()
                        .includeTables("db\\..sc1.A[0-9]+,db.sc2.B[0-1]+,db\\..sc1.sc1,db.sc1.sc1")
                        .build();

        assertAllowed(selectors, "db", "sc1", "sc1");
        assertAllowed(selectors, "db1", "sc1", "sc1");
        assertAllowed(selectors, "dba", "sc1", "sc1");
        assertAllowed(selectors, "db1", "sc1", "A1");
        assertAllowed(selectors, "dba", "sc1", "A2");
        assertAllowed(selectors, "db", "sc2", "B0");
        assertAllowed(selectors, "db", "sc2", "B1");
        assertNotAllowed(selectors, "db", "sc1", "A");
        assertNotAllowed(selectors, "db", "sc1a", "B");
        assertNotAllowed(selectors, "db", "sc1", "AA");
        assertNotAllowed(selectors, "db", "sc2", "B2");
        assertNotAllowed(selectors, "dba1", "sc1", "A1");
        assertNotAllowed(selectors, "dba2", "sc1", "A2");
        assertNotAllowed(selectors, "db", "sc11", "A1");
        assertNotAllowed(selectors, "db", "sc1A", "A1");

        // schemaName, tableName
        selectors =
                new Selectors.SelectorsBuilder()
                        .includeTables("sc1.A[0-9]+,sc2.B[0-1]+,sc1.sc1")
                        .build();

        assertAllowed(selectors, null, "sc1", "sc1");
        assertAllowed(selectors, null, "sc1", "A1");
        assertAllowed(selectors, null, "sc1", "A2");
        assertAllowed(selectors, null, "sc2", "B0");
        assertAllowed(selectors, null, "sc2", "B1");
        assertNotAllowed(selectors, "db", "sc1", "A1");
        assertNotAllowed(selectors, null, "sc1", "A");
        assertNotAllowed(selectors, null, "sc2", "B");
        assertNotAllowed(selectors, null, "sc1", "AA");
        assertNotAllowed(selectors, null, "sc11", "A1");
        assertNotAllowed(selectors, null, "sc1A", "A1");

        // tableName
        selectors =
                new Selectors.SelectorsBuilder().includeTables("\\.A[0-9]+,B[0-1]+,sc1").build();

        assertAllowed(selectors, null, null, "sc1");
        assertNotAllowed(selectors, "db", "sc1", "sc1");
        assertNotAllowed(selectors, null, "sc1", "sc1");
        assertAllowed(selectors, null, null, "1A1");
        assertAllowed(selectors, null, null, "AA2");
        assertAllowed(selectors, null, null, "B0");
        assertAllowed(selectors, null, null, "B1");
        assertNotAllowed(selectors, "db", "sc1", "A1");
        assertNotAllowed(selectors, null, null, "A");
        assertNotAllowed(selectors, null, null, "B");
        assertNotAllowed(selectors, null, null, "2B");

        selectors =
                new Selectors.SelectorsBuilder()
                        .includeTables("sc1.A[0-9]+,sc2.B[0-1]+,sc1.sc1")
                        .build();

        assertAllowed(selectors, null, "sc1", "sc1");
        assertAllowed(selectors, null, "sc1", "A1");
        assertAllowed(selectors, null, "sc1", "A2");
        assertAllowed(selectors, null, "sc1", "A2");
        assertAllowed(selectors, null, "sc2", "B0");
        assertNotAllowed(selectors, "db", "sc1", "A1");
        assertNotAllowed(selectors, null, "sc1", "A");
        assertNotAllowed(selectors, null, "sc1", "AA");
        assertNotAllowed(selectors, null, "sc2", "B");
        assertNotAllowed(selectors, null, "sc2", "B2");
        assertNotAllowed(selectors, null, "sc11", "A1");
        assertNotAllowed(selectors, null, "sc1A", "A1");

        selectors = new Selectors.SelectorsBuilder().includeTables("sc1.sc1").build();
        assertAllowed(selectors, null, "sc1", "sc1");

        selectors = new Selectors.SelectorsBuilder().includeTables("sc1.sc[0-9]+").build();
        assertAllowed(selectors, null, "sc1", "sc1");

        selectors = new Selectors.SelectorsBuilder().includeTables("sc1.\\.*").build();
        assertAllowed(selectors, null, "sc1", "sc1");
    }

    protected void assertAllowed(
            Selectors filter, String nameSpace, String schemaName, String tableName) {

        TableId id = getTableId(nameSpace, schemaName, tableName);

        assertThat(filter.isMatch(id)).isTrue();
    }

    protected void assertNotAllowed(
            Selectors filter, String nameSpace, String schemaName, String tableName) {

        TableId id = getTableId(nameSpace, schemaName, tableName);

        assertThat(filter.isMatch(id)).isFalse();
    }

    private static TableId getTableId(String nameSpace, String schemaName, String tableName) {
        TableId id;
        if (nameSpace == null && schemaName == null) {
            id = TableId.tableId(tableName);
        } else if (nameSpace == null) {
            id = TableId.tableId(schemaName, tableName);
        } else {
            id = TableId.tableId(nameSpace, schemaName, tableName);
        }
        return id;
    }
}
