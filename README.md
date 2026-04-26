# ISM 6562 — Big Data for Business Applications
## Final Project: Sportlytics Athletics — Professional Basketball Analytics

**Team Name:** BigQuest Query  
**Course:** ISM 6562 — Big Data for Business Applications (Spring 2026)  
**Instructor:** Dr. Tim Smith  

---

## Team Members

| Name | GitHub Branch |
|---|---|
| Karthikeyan Atmakuru | `karthikeyan` |
| *Marta Falceto Font* | *(add branch)* |
| *Kadiatou Sogodogo* | *(add branch)* |

---

## Company Background

Sportlytics is a sports analytics firm serving 30 professional basketball teams and 450+ active players. Every arena is equipped with optical tracking cameras capturing player positions 25 times per second, producing terabytes of raw spatial data each season. Teams use Sportlytics for fatigue models, injury risk scores, lineup optimization, and post-game performance breakdowns.

---

## The Business Problem

Sportlytics' current infrastructure was built for a 3-team proof-of-concept and was never re-architected. Three critical failures are threatening client contracts:

| Problem | Current State | Impact |
|---|---|---|
| In-game dashboard lag | 45-second processing delay | Coaches cannot make real-time substitution decisions |
| Injury model recall | Only 38% recall — misses 62% of injuries | $780M in player salary lost to preventable injuries last season |
| Batch job failures | Single server handles ingestion + processing simultaneously | Nightly reports fail during playoff volume spikes (resource contention) |

### Root Cause Analysis

**Problem 1 — 45-second lag** is a batch vs streaming architecture problem. Tracking cameras produce data at 25 frames/second but the current pipeline processes it in nightly batch jobs. Fix: replace with Kafka + Spark Structured Streaming.

**Problem 2 — Low recall** is a feature quality problem. The model is trained on incomplete data and predicts "no injury" when injury is coming (False Negatives). Fix: integrate all 5 data sources into richer rolling workload features and retrain with SparkML on a distributed cluster.

**Problem 3 — Batch job failures** is a resource contention problem. One server cannot simultaneously ingest live game data AND run heavy batch transformations when playoff volume spikes. Fix: separate storage (HDFS) from compute (Spark cluster) so they scale independently.

---

## Data Sources

| File | Format | Records | Description |
|---|---|---|---|
| `player-tracking.csv` | CSV | 750,000 | In-game spatial data: position, speed, acceleration, distance, heart rate per timestamp |
| `game-stats.json` | JSON | 300,000 | Box score statistics per player per quarter |
| `injury-reports.csv` | CSV | 15,000 | Historical injury records: type, severity, games missed, treatment |
| `training-sessions.json` | JSON | 150,000 | Practice, gym, recovery logs with intensity scores and biometrics |
| `team-schedules.csv` | CSV | 10,000 | Full league schedule with travel distances, rest days, back-to-back flags |

Data sourced from: [Professor's course repository](https://github.com/prof-tcsmith/ism6562s26-class/tree/main/final-projects/data/07-sportlytics-athletics)

---

## Architecture Overview

This project is built in 4 stages, each adding services on top of the previous:

```
Stage 1 — HDFS Data Lake
    NameNode + 3 DataNodes + YARN (ResourceManager + 3 NodeManagers) + History Server

Stage 2 — Spark Batch Processing        [builds on Stage 1]
    + Spark Master + Spark Worker + Jupyter Notebook

Stage 3 — Kafka Real-Time Streaming     [builds on Stage 2]
    + Zookeeper + Kafka

Stage 4 — Airflow Orchestration         [builds on Stage 3]
    + Airflow Webserver + Airflow Scheduler + PostgreSQL (Airflow metadata)
```

---

## Infrastructure — Docker Setup

All services are defined in a **single `docker-compose.yml`** file. Services are started selectively per stage to manage memory on local machines.

### Why one docker-compose file?
Each stage builds on top of the previous — HDFS stays running when Spark is added, Spark stays running when Kafka is added. One file defines the full stack.

### HDFS Design Decisions

| Decision | Choice | Reason |
|---|---|---|
| Replication Factor | 2 | Course project — protects against 1 node failure, saves memory |
| DataNodes | 3 | RF=2 needs minimum 2, 3 gives a safety buffer |
| NodeManagers | 3 | One per DataNode — compute co-located with storage for faster Spark jobs |
| YARN included | Yes | Spark submits jobs to YARN ResourceManager |
| History Server | Yes | Needed to debug Spark jobs after they complete |

### Starting services per stage

```bash
# Stage 1 only — HDFS
docker compose up namenode datanode1 datanode2 datanode3 resourcemanager nodemanager1 nodemanager2 nodemanager3 historyserver 

# Stage 2 — Add Spark


# Stage 3 — Add Kafka


# Stage 4 — Full stack

```

---

## Project Structure

```
ISM6562-Final_project_BigQuest_Query/
│
├── data/                                   # raw data files (committed to Git)
│   └── 07-sportlytics-athletics/
│       ├── player-tracking.csv
│       ├── game-stats.json
│       ├── injury-reports.csv
│       ├── training-sessions.json
│       └── team-schedules.csv
│
├── scripts/                                # HDFS load and setup scripts
│   └── 01_load_hdfs.sh
│
├── notebooks/                              # PySpark Jupyter notebooks
│   ├── stage2_batch_transform.ipynb
│   └── stage3_streaming.ipynb
│
├── dags/                                   # Airflow DAG definitions
│   └── sportlytics_pipeline.py
│
├── docker-compose.yml                      # full stack — all 4 stages
├── .gitignore
└── README.md
```

---

## Stage Progress

| Stage | Description | Status |
|---|---|---|
| Stage 1 | HDFS Data Lake — load all 5 files, create raw/processed/analytics zones | Completed |
| Stage 2 | Spark Batch — clean, join, aggregate, write Parquet | In Progress |
| Stage 3 | Kafka + Spark Streaming — real-time player telemetry alerts | In Progress |
| Stage 4 | Airflow — nightly batch pipeline orchestration | In Progress |

---

## Business Questions

1. **Injury risk prediction** — Can historical workload patterns predict injury risk in the next 7 days?
2. **Fatigue-performance correlation** — How does cumulative exertion correlate with shooting percentage and plus-minus?
3. **Back-to-back impact** — Quantify performance decline on the second night of a back-to-back
4. **Travel and rest interaction** — Does travel distance compound fatigue beyond rest days alone?
5. **Training load optimization** — What is the optimal practice-to-recovery ratio for peak performance?
6. **Real-time exertion monitoring** — Detect when a player crosses fatigue thresholds within seconds during live games

---

## Setup Instructions

### Prerequisites
- Docker Desktop (minimum 8GB RAM allocated)
- Git
- VS Code

### Getting Started

```bash
# 1. Clone this repository
git clone https://github.com/KarthikeyanAtmakuru/ISM6562-Final_project_BigQuest_Query.git
cd ISM6562-Final_project_BigQuest_Query

# 2. Switch to your branch
git checkout karthikeyan

# 3. Download data files from professor's repo and place in data/07-sportlytics-athletics/

# 4. Start Stage 1 services
docker compose up namenode datanode1 datanode2 datanode3 resourcemanager nodemanager1 nodemanager2 nodemanager3 historyserver -d


# 5. Verify HDFS is running — open in browser
# NameNode UI:        http://localhost:9870  (should show 3 Live Nodes)
# ResourceManager UI: http://localhost:8088  (should show 3 Active Nodes)

---

## Git Workflow

```bash
# Always work on your personal branch
git checkout karthikeyan

# After making changes
git add .
git commit -m "commit message to display"
git push
```

---

## References

- Course: ISM 6562 — Big Data for Business Applications, USF
- Instructor: Dr. Tim Smith
- [Apache Hadoop Documentation](https://hadoop.apache.org/docs/stable/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
