# LogSentinel - Design notes (decision log)

Running log of design decisions: **decision -> alternatives -> evidence**. Add an entry
whenever you change something non-obvious. The HLD is assembled from these notes.

## Data

**D1. Dataset: Loghub HDFS_v1** - 11,175,629 lines, 1.47 GB, 575,061 labelled blocks, 2.93%
anomalous. Labels are per block; the session key is the block ID.

**D2. Removed Loghub's pre-computed features** (`Event_traces.csv`, `Event_occurrence_matrix.csv`,
`HDFS.npz`). Kept only `HDFS.log`, `anomaly_label.csv` and the reference `HDFS.log_templates.csv`
(29 templates, used only for comparison). Our pipeline builds every feature itself.

**D3. Validation is plain Python, not Spark.** One sequential regex scan (~970k lines/s, 11.5 s for
1.5 GB); Spark start-up (~20 s) would be slower. 8 checks; the stage exits non-zero on failure so
`dvc repro` stops before bad data reaches training. Result: 0 malformed lines, 0 lines without a
block ID, every block labelled.

**D4. HDFS_2k sample anomaly.** The 2k sample contains 3 `BLOCK* ask <ip> to delete blk_... blk_...`
lines listing many blocks (206 extra rows after explode); the full HDFS_v1 log has **0** lines with
2+ distinct blocks. Consequences: the sample is a unit-test fixture for the explode path, and that
template is *unseen* for the production model (drift demo).

## EDA findings that drove the design (notebooks/01_eda.ipynb)

- Normal blocks: >= 13 lines, median 19, std 4.8; anomalies: 2-284 lines, std 12.4 ->
  missing/extra events matter -> event-count features.
- WARN in 48% of anomalies but also 22.6% of normal blocks (~126k) -> keyword rule fails ->
  template-based ML.
- Duration does not separate the classes -> not a feature.
- 4 components produce ~99% of lines; rare components relate to failures -> keep rare templates.
- Traffic: mean ~80 lines/s, burst peak ~3,500 lines/s -> throughput target >= 3,500 lines/s.

## Pipeline

**P1. Split blocks before template mining (70/10/20, stratified, seed 42).** drain3, the vectorizer
and the models only see training blocks -> no leakage into val/test. Random stratified split is the
standard HDFS benchmark protocol; a chronological split is a harder alternative (future work).
Result: anomaly ratio 2.928% / 2.928% / 2.927% in train / val / test.

**P2. drain3 masks, in order:** `BLKS` (run of 2+ block IDs) -> `BLK` -> `IP` -> `PATH` -> `NUM`.
`BLKS`: Drain groups by token count, so a block list would otherwise give one template per list
length. `PATH`: stops file-path lines fragmenting once the similarity threshold is high.

**P3. `sim_th` raised 0.4 -> 0.85 (evidence).** At 0.4, drain merged `PacketResponder ... terminating`
(1,706,679 lines, blocks 1.9% anomalous) with `PacketResponder ... Interrupted.` (49 lines, 46 blocks,
**100% anomalous**) into one `<*>` template, destroying a perfect signal. One word differing in a
6-token message is 0.83 similar, so only a threshold > 0.83 separates them.
Result: 45 -> 52 templates, events kept separate, no fragmentation. Guarded by test TC-14.

**P4. Mine once (sequential), match in parallel (broadcast).** Drain is online and order-dependent,
so mining runs on one core (7.8M training lines in ~110 s, ~69k lines/s). The frozen state (a few
KB) is broadcast; each Spark partition rebuilds a read-only TemplateMiner and matches with
`mapInPandas` (Arrow batches). Matching is strict (all non-wildcard tokens must match).
Result: 388k lines/s on 8 cores; **0 unseen lines in train, val and test** -> drift baseline is
exactly 0%. Guarded by test TC-36.

**P5. Sessions** keep the template-count map (features), the ordered sequence (explanations,
future sequence models), session length and unseen count.

**P6. Throughput benchmark (1-16 cores, Spark `noop` sink, fresh JVM per run).**
Parse: 151k -> 838k lines/s (5.6x). Match: 116k -> 551k lines/s at 12 cores (4.7x), slightly worse
at 16. Ceiling: (a) 12 Parquet files = at most 12 match tasks, (b) Intel Core Ultra 7 155H has only
6 P-cores (+8 E-cores, 2 LP E-cores, hyper-threads), (c) turbo boost on 1 core, (d) Amdahl (JVM <->
Python transfer, scheduling). Pipeline uses `local[8]` (~75% of peak, leaves cores for Docker).

## Features & models

**M1. Features:** one count column per training template (52) + `__unseen__` (53 features).
The vocabulary is fitted on train only; the same `EventCountVectorizer` runs inside the served model.
Result: 0 rows with unseen templates in train/val/test; ~7.25 non-zero features per block.

**M2. Models and results (test set, threshold chosen on validation max-F1):**

| Model | Precision | Recall | F1 | PR-AUC |
|---|---|---|---|---|
| Logistic Regression (log1p -> scale -> LR, class_weight=balanced, C=10) | 0.9955 | 0.9947 | 0.9951 | 0.9994 |
| Isolation Forest (normal training blocks only) | 0.6228 | 0.2138 | 0.3184 | 0.1594 |

LR: 15 false alarms and 18 misses out of 115,013 test blocks; all C values within 0.0005 val PR-AUC
(stable). IF fails because many anomalies differ from normal blocks by ONE extra event out of ~19
- they are not "easy to isolate" in count space, and IF's subsamples rarely contain rare events.
With labels available, supervised learning is clearly the right choice; IF documents why.

**M3. Explainability:** per-session contribution = standardised feature value x LR coefficient;
the top 5 positive contributions are shown with template text. Raw coefficients alone can mislead
(correlated features: `ask to replicate` and `Starting thread to transfer` always co-occur and share
the weight 1.3158 equally). Strongest feature: fewer `Receiving block` events (coef -8.9) = missing
replicas.

**M4. Packaging & promotion:** one MLflow pyfunc model (drain3 state + templates + vectorizer +
classifier + meta + `src` code). Alias `champion` moves to a new version only if its test F1 is >=
the current champion's; rollback = move the alias. Served version traced to a clean commit
(`git_dirty=False`; dirty check covers code/config only because DVC deletes tracked outputs
before re-running a stage).

**M5. Complete sessions only (limitation).** The first 1,000 raw lines gave 98% "anomalies": every
block was cut off after a few seconds, so its later events were missing - exactly the pattern the
model learned means failure. The model is trained and evaluated on complete sessions; demo files
are built from complete test blocks (`scripts/make_demo_logs.py`): 38/40 correct, 0 false alarms.

**M6. Borderline verdict (UX for the precision/recall trade-off).** The 2 demo misses scored 0.967
and 0.9746 vs threshold 0.9755 (balanced class weights push probabilities up, so max-F1 puts the
cutoff high). Served and offline scores matched exactly -> no serving bug. The UI keeps the
validated threshold for "Problem" but shows risk >= 50% below it as "Borderline" so a human looks.

## Serving & operations

**S1. Loose coupling:** browser -> backend -> model server, REST only; every URL from env vars;
the UI only knows `API_BASE_URL` (config.js).
**S2. Backend:** input limits (50k lines, 5 MB), non-HDFS lines skipped and counted, uniform error
shape, 503 with a friendly message when the model server is down, API key on `/api/v1/*`.
Latency: 60 ms for 1,000 lines, 26 ms for the 823-line demo file (AC-3 target < 2 s).
**S3. Docker Compose, 10 services:** mlflow, model, backend, frontend (nginx HTTPS), prometheus,
pushgateway, alertmanager, grafana, cadvisor, node-exporter. Non-root containers; secrets in `.env`.
Model image built offline from host-prepared wheels (in-Docker downloads ran at ~40 kB/s; drain3 has
no wheel on PyPI, so it is built with `pip wheel`). Frontend health check uses 127.0.0.1 (Alpine
resolves localhost to IPv6, nginx listens on IPv4).
**S4. Monitoring:** Prometheus scrapes backend, Pushgateway (batch pipeline pushes stage metrics),
cAdvisor (per-container CPU/memory), node-exporter (host) - 7/7 targets UP. cAdvisor v0.52.1 with
DOCKER_API_VERSION=1.44 (Docker 29 rejects API 1.41); container names unavailable with the containerd
image store, so panels label containers by short Docker ID.
**S5. Drift -> retraining loop (demonstrated):** 20% never-seen lines -> UnseenTemplateDrift fired
(> 1% for 1 min) -> Alertmanager webhook (bearer token) -> backend queued a request ->
`retrain_watcher.py` ran `dvc repro` + forced train/package -> new version promoted (F1 not worse)
-> model container restarted. Rollback = move the `champion` alias.
**S6. Resilience (UI-8):** model container stopped -> UI shows "model is not available", API 503,
ModelServerDown alert; service recovered after restart.
