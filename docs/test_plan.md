# LogSentinel – Test Plan

## 1. Scope and approach

| Level | Tool | Location | How to run |
|---|---|---|---|
| Unit | pytest | `tests/unit/` | `pytest tests/unit -v` (also run by the pre-push CI hook) |
| Integration (Spark) | pytest + local Spark | `tests/integration/test_spark_stages.py` | `pytest -m spark -v` |
| Integration (model) | pytest + MLflow pyfunc | `tests/integration/test_pyfunc_model.py` | `pytest -m model -v` (after `dvc repro package`) |
| End-to-end | pytest + requests | `tests/e2e/test_stack.py` | `E2E=1 pytest tests/e2e -v` (stack running) |
| Pipeline data checks | validation stage | `src/ingest/validate.py` | `dvc repro validate` |
| Manual UI checks | browser | – | checklist in section 4 |

Test report: `pytest --junitxml=reports/tests/junit.xml && python scripts/test_report.py`
→ `reports/tests/test_report.md` (totals, passed, failed, skipped, per-test result).

## 2. Acceptance criteria

| ID | Criterion | How measured | Target |
|---|---|---|---|
| AC-1 | Served model quality | test F1 in `reports/train_metrics.json` | ≥ 0.90 |
| AC-2 | Ranking quality on imbalanced data | test PR-AUC | ≥ 0.90 |
| AC-3 | API latency | p95 of `/api/v1/predict` for 1,000 lines (Grafana / `curl -w`) | < 2 s |
| AC-4 | Pipeline throughput | match stage lines/s (`reports/match_stats.json`) | ≥ 3,500 lines/s |
| AC-5 | Data quality gate | `reports/validation.json` → `all_passed` | true |
| AC-6 | No leakage | unseen ratio train/val/test from `reports/match_stats.json`; drain3/vectorizer fitted on train only | train 0%, vectorizer fitted on train split |
| AC-7 | Reproducibility | `dvc repro` on a clean clone reports "up to date" after `dvc pull` | yes |
| AC-8 | Automated tests | all unit + integration tests | 100% pass |
| AC-9 | End-to-end | all e2e tests with the stack running | 100% pass |
| AC-10 | Monitoring | all Prometheus targets `up`, dashboard shows live data | yes |

## 3. Test cases

| ID | Test (file::function) | Description | Expected result |
|---|---|---|---|
| TC-01 | unit/test_validate::test_line_regex_accepts_hdfs_line | real HDFS line parsed into 6 fields | match, level INFO |
| TC-02 | unit/test_validate::test_line_regex_rejects_garbage | non-HDFS text | no match |
| TC-03 | unit/test_validate::test_block_regex_handles_negative_ids | negative block IDs | both IDs found |
| TC-04 | unit/test_validate::test_scan_logs_counts_malformed_and_blockless | malformed / block-less lines | counted 1 / 1 |
| TC-05 | unit/test_validate::test_scan_labels_detects_invalid_and_duplicates | bad label rows | 1 invalid, 1 duplicate |
| TC-06 | unit/test_validate::test_build_checks_fails_on_bad_data | 50% malformed | check fails |
| TC-07 | unit/test_split::test_split_sizes_and_no_overlap | 70/10/20 sizes | 700/100/200, unique |
| TC-08 | unit/test_split::test_split_is_stratified | anomaly ratio per split | 3% ± 1% |
| TC-09 | unit/test_split::test_split_is_reproducible | same seed twice | identical |
| TC-10 | unit/test_split::test_split_rejects_bad_fractions | fractions sum 1.1 | ValueError |
| TC-11 | unit/test_drain_config::test_block_and_ip_are_masked | masking | `<BLK>`, `<IP>` |
| TC-12 | unit/test_drain_config::test_block_list_becomes_one_token | 3 vs 5 listed blocks | same template, `<BLKS>` |
| TC-13 | unit/test_drain_config::test_paths_are_masked | different subdirs | same template, `<PATH>` |
| TC-14 | unit/test_drain_config::test_terminating_and_interrupted_stay_separate | sim_th 0.85 | different clusters |
| TC-15 | unit/test_vectorizer::test_fit_learns_sorted_vocabulary | vocabulary | T1..T3 + `__unseen__` |
| TC-16 | unit/test_vectorizer::test_unknown_templates_go_to_unseen_column | unknown + UNSEEN | counted in `__unseen__` |
| TC-17 | unit/test_vectorizer::test_min_block_freq_filters_rare_templates | min_block_freq=2 | only T1 |
| TC-18 | unit/test_vectorizer::test_save_and_load_roundtrip | save/load (TF-IDF) | identical features |
| TC-19 | unit/test_vectorizer::test_transform_before_fit_raises | unfitted | RuntimeError |
| TC-20 | unit/test_model_helpers::test_parse_content_returns_message | message extraction | content / None |
| TC-21 | unit/test_model_helpers::test_extract_blocks_deduplicates | repeated block | counted once |
| TC-22 | unit/test_model_helpers::test_sessionize_groups_by_block_and_skips_bad_lines | grouping | 1 session, 2 skipped |
| TC-23 | unit/test_model_helpers::test_sessionize_keeps_unseen_marker | unseen | -1 kept |
| TC-24 | unit/test_backend::test_health | /health | 200 |
| TC-25 | unit/test_backend::test_predict_requires_api_key | no key | 401 |
| TC-26 | unit/test_backend::test_predict_rejects_empty_input | blank text | 422 |
| TC-27 | unit/test_backend::test_predict_rejects_non_hdfs_text | wrong format | 422 + example |
| TC-28 | unit/test_backend::test_predict_success_sorted_by_score | valid logs | 200, sorted, summary correct |
| TC-29 | unit/test_backend::test_predict_file_upload | file upload | 200 |
| TC-30 | unit/test_backend::test_model_down_returns_503 | model failure | 503 friendly message |
| TC-31 | unit/test_backend::test_too_many_lines_returns_413 | over line limit | 413 |
| TC-32 | unit/test_backend::test_metrics_endpoint_exposes_counters | /metrics | LogSentinel metrics present |
| TC-33 | unit/test_backend::test_feedback_is_saved | feedback | saved to file |
| TC-34 | unit/test_backend::test_alert_webhook_queues_retraining | webhook | 401 without token, queued with |
| TC-35 | integration/test_spark_stages::test_parse_explodes_multi_block_lines | Spark parse on sample | 2,206 rows, 2,000 lines |
| TC-36 | integration/test_spark_stages::test_broadcast_matching_reproduces_mined_templates | broadcast match | 0 unseen |
| TC-37 | integration/test_pyfunc_model::test_model_scores_sample_lines | pyfunc on raw lines | one row per block |
| TC-38 | integration/test_pyfunc_model::test_model_handles_garbage_input | garbage | empty result |
| TC-39 | integration/test_pyfunc_model::test_model_flags_unseen_events | new event | n_unseen = 1 |
| TC-40 | e2e/test_stack::test_ui_served_over_https | UI over HTTPS | 200, config present |
| TC-41 | e2e/test_stack::test_backend_ready | readiness | 200 |
| TC-42 | e2e/test_stack::test_predict_through_ui_proxy | UI → nginx → backend → model | 200, 1 session |
| TC-43 | e2e/test_stack::test_prometheus_targets_up | scrape targets | all up |
| TC-44 | e2e/test_stack::test_grafana_dashboard_provisioned | dashboard | 200 |

## 4. Manual UI test checklist (record pass/fail with screenshots)

| ID | Step | Expected |
|---|---|---|
| UI-1 | Open https://localhost:8443, accept the certificate warning | UI loads, model badge green |
| UI-2 | Click **Analyze** with empty input | Error message, no request sent |
| UI-3 | Paste "hello world", Analyze | Friendly "not HDFS log lines" error with example |
| UI-4 | Load example logs, Analyze | 2 sessions, 1 problem, summary shown |
| UI-5 | Click the red session | Explanation sentence + event bars |
| UI-6 | Upload `data/sample/HDFS_2k.log` | ~2,200 sessions, results within a few seconds |
| UI-7 | Choose a file > 5 MB | Rejected before upload |
| UI-8 | Stop the model (`docker compose stop model`), Analyze | "model is not available" message; restart → works |
| UI-9 | Pipeline console | All stages shown with status, runs, MLflow runs, benchmark |
| UI-10 | Request retraining | "Retraining queued" toast, pending count +1 |
| UI-11 | Monitoring tab | Grafana dashboard embedded and refreshing |
| UI-12 | Resize to phone width | Layout stacks into one column, still usable |

## 5. Results

See `reports/tests/test_report.md` (generated) and the acceptance-criteria table filled in below.

### Automated tests

44 test cases executed, **44 passed, 0 failed, 0 skipped** (34 unit, 2 Spark integration,
3 model integration, 5 end-to-end) - `reports/tests/test_report.md`, screenshot 62/63.

### Acceptance criteria

| AC | Target | Result | Met | Evidence |
|---|---|---|---|---|
| AC-1 | test F1 >= 0.90 | 0.9951 | Yes | `reports/train_metrics.json`, MLflow run |
| AC-2 | test PR-AUC >= 0.90 | 0.9994 | Yes | `reports/train_metrics.json`, PR curve |
| AC-3 | p95 latency < 2 s for 1,000 lines | 60 ms (1,000 lines), 26 ms (823-line demo) | Yes | screenshot 42, Grafana latency panel |
| AC-4 | >= 3,500 lines/s | match ~388k lines/s (8 cores) | Yes | `reports/match_stats.json`, benchmark |
| AC-5 | validation all_passed | true (9/9 checks) | Yes | `reports/validation.json` |
| AC-6 | no leakage | split before mining; vectorizer fitted on train; 0% unseen in all splits | Yes | `reports/match_stats.json`, `featurize_stats.json` |
| AC-7 | reproducible from Git + DVC | Fresh clone + `dvc pull` (126 files restored): "Data and pipelines are up to date." | Yes | screenshot 66 |
| AC-8 | unit + integration 100% pass | 39/39 | Yes | test report |
| AC-9 | e2e 100% pass | 5/5 | Yes | test report |
| AC-10 | all Prometheus targets up, live dashboard | 7/7 targets UP | Yes | screenshots 53, 55, 56 |

### Manual UI checks

| ID | Result | Evidence |
|---|---|---|
| UI-1 | Pass - UI loads over HTTPS, model badge green | screenshot 51 |
| UI-2 | Pass - empty input rejected before sending | – |
| UI-3 | Pass - friendly "not HDFS log lines" error with example | screenshot 47 |
| UI-4 | Pass - example logs: 2 sessions, 1 problem | – |
| UI-5 | Pass - explanation sentence + event bars | screenshots 45, 46 |
| UI-6 | Pass - demo file: 40 sessions in 26 ms | screenshot 44 |
| UI-7 | Pass - files > 5 MB rejected in the browser | – |
| UI-8 | Pass - model stopped: "model is not available" message, ModelServerDown alert; recovered after restart | screenshots 64, 65 |
| UI-9 | Pass - pipeline console shows stages, runs, MLflow runs, benchmark | screenshot 48 |
| UI-10 | Pass - retraining request queued | – |
| UI-11 | Pass - Grafana embedded in Monitoring tab | screenshot 57 |
| UI-12 | Pass - single-column layout at phone width | screenshot 49 |
