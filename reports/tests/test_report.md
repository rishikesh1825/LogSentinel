# LogSentinel - Test Report

Generated: 2026-11-11 00:53

| Total | Passed | Failed | Errors | Skipped | Pass rate (executed) | Duration |
|---|---|---|---|---|---|---|
| 44 | 44 | 0 | 0 | 0 | 100.0% | 15.9 s |

## Test cases

| # | Module | Test | Result | Time (s) |
|---|---|---|---|---|
| 1 | `tests.e2e.test_stack` | `test_ui_served_over_https` | PASS | 0.05 |
| 2 | `tests.e2e.test_stack` | `test_backend_ready` | PASS | 0.03 |
| 3 | `tests.e2e.test_stack` | `test_predict_through_ui_proxy` | PASS | 0.05 |
| 4 | `tests.e2e.test_stack` | `test_prometheus_targets_up` | PASS | 0.01 |
| 5 | `tests.e2e.test_stack` | `test_grafana_dashboard_provisioned` | PASS | 0.08 |
| 6 | `tests.integration.test_pyfunc_model` | `test_model_scores_sample_lines` | PASS | 0.49 |
| 7 | `tests.integration.test_pyfunc_model` | `test_model_handles_garbage_input` | PASS | 0.00 |
| 8 | `tests.integration.test_pyfunc_model` | `test_model_flags_unseen_events` | PASS | 0.00 |
| 9 | `tests.integration.test_spark_stages` | `test_parse_explodes_multi_block_lines` | PASS | 9.94 |
| 10 | `tests.integration.test_spark_stages` | `test_broadcast_matching_reproduces_mined_templates` | PASS | 4.32 |
| 11 | `tests.unit.test_backend` | `test_health` | PASS | 0.01 |
| 12 | `tests.unit.test_backend` | `test_predict_requires_api_key` | PASS | 0.01 |
| 13 | `tests.unit.test_backend` | `test_predict_rejects_empty_input` | PASS | 0.01 |
| 14 | `tests.unit.test_backend` | `test_predict_rejects_non_hdfs_text` | PASS | 0.01 |
| 15 | `tests.unit.test_backend` | `test_predict_success_sorted_by_score` | PASS | 0.01 |
| 16 | `tests.unit.test_backend` | `test_predict_file_upload` | PASS | 0.01 |
| 17 | `tests.unit.test_backend` | `test_model_down_returns_503` | PASS | 0.01 |
| 18 | `tests.unit.test_backend` | `test_too_many_lines_returns_413` | PASS | 0.01 |
| 19 | `tests.unit.test_backend` | `test_metrics_endpoint_exposes_counters` | PASS | 0.01 |
| 20 | `tests.unit.test_backend` | `test_feedback_is_saved` | PASS | 0.01 |
| 21 | `tests.unit.test_backend` | `test_alert_webhook_queues_retraining` | PASS | 0.01 |
| 22 | `tests.unit.test_drain_config` | `test_block_and_ip_are_masked` | PASS | 0.00 |
| 23 | `tests.unit.test_drain_config` | `test_block_list_becomes_one_token` | PASS | 0.00 |
| 24 | `tests.unit.test_drain_config` | `test_paths_are_masked` | PASS | 0.00 |
| 25 | `tests.unit.test_drain_config` | `test_terminating_and_interrupted_stay_separate` | PASS | 0.00 |
| 26 | `tests.unit.test_model_helpers` | `test_parse_content_returns_message` | PASS | 0.00 |
| 27 | `tests.unit.test_model_helpers` | `test_extract_blocks_deduplicates` | PASS | 0.00 |
| 28 | `tests.unit.test_model_helpers` | `test_sessionize_groups_by_block_and_skips_bad_lines` | PASS | 0.00 |
| 29 | `tests.unit.test_model_helpers` | `test_sessionize_keeps_unseen_marker` | PASS | 0.00 |
| 30 | `tests.unit.test_split` | `test_split_sizes_and_no_overlap` | PASS | 0.02 |
| 31 | `tests.unit.test_split` | `test_split_is_stratified` | PASS | 0.01 |
| 32 | `tests.unit.test_split` | `test_split_is_reproducible` | PASS | 0.01 |
| 33 | `tests.unit.test_split` | `test_split_rejects_bad_fractions` | PASS | 0.00 |
| 34 | `tests.unit.test_validate` | `test_line_regex_accepts_hdfs_line` | PASS | 0.00 |
| 35 | `tests.unit.test_validate` | `test_line_regex_rejects_garbage` | PASS | 0.00 |
| 36 | `tests.unit.test_validate` | `test_block_regex_handles_negative_ids` | PASS | 0.00 |
| 37 | `tests.unit.test_validate` | `test_scan_logs_counts_malformed_and_blockless` | PASS | 0.00 |
| 38 | `tests.unit.test_validate` | `test_scan_labels_detects_invalid_and_duplicates` | PASS | 0.00 |
| 39 | `tests.unit.test_validate` | `test_build_checks_fails_on_bad_data` | PASS | 0.00 |
| 40 | `tests.unit.test_vectorizer` | `test_fit_learns_sorted_vocabulary` | PASS | 0.00 |
| 41 | `tests.unit.test_vectorizer` | `test_unknown_templates_go_to_unseen_column` | PASS | 0.00 |
| 42 | `tests.unit.test_vectorizer` | `test_min_block_freq_filters_rare_templates` | PASS | 0.00 |
| 43 | `tests.unit.test_vectorizer` | `test_save_and_load_roundtrip` | PASS | 0.00 |
| 44 | `tests.unit.test_vectorizer` | `test_transform_before_fit_raises` | PASS | 0.79 |
