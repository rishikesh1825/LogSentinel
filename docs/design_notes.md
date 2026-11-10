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

_To be filled in after Phases 4-5 (vectorizer, LR vs Isolation Forest, threshold, results)._

## Serving & operations

_To be filled in after Phases 7-10 (loose coupling, security, drift -> retraining loop)._
