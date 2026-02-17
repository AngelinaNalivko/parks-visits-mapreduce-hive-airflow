# Parks Visits Analytics (Hadoop Streaming + Hive + Airflow on GCP)
End-to-end big data batch pipeline built on the Hadoop ecosystem:
- **MapReduce (Hadoop Streaming, Python)** to compute daily visit statistics per park
- **Apache Hive (HQL)** to enrich results with a parks dictionary and aggregate metrics by nature type & region
- **Apache Airflow** DAG to orchestrate the whole workflow
- Executed on **Google Cloud Platform (Dataproc + GCS + HDFS)**

## Problem
### Part 1 — MapReduce (Hadoop Streaming)
For visit logs (datasource1), compute daily statistics grouped by:
- `park_id`
- `date`

For each group calculate:
- `visits_count` — total visits for a park on a given day  
- `avg_group_size` — average size of visiting groups

Output format (CSV, no header):
```
park_id,date,visits_count,avg_group_size
```

### Part 2 — Hive
Using:
- MapReduce output
- parks dictionary (datasource4)

Compute for each `nature_type` and `region`:
- `total_visits` — total visits
- `avg_group_size` — average group size
- `visits_deviation` — deviation of `total_visits` in a region from the **average total visits across all regions** for the same nature type

Final output is JSON Lines (one JSON per line).

## Data
Project was tested on **100 CSV files (10k rows each)**.  
Repository includes only small samples for schema/reference:
- `data_samples/visits99.csv` (example from datasource1)
- `data_samples/parks.csv` (example from datasource4)

Expected folders on storage:
```
input/
datasource1/*.csv
datasource4/parks.csv
````

## Implementation
### MapReduce (Streaming, Python)
Files:
- `project_files/mapper.py` — parses CSV, extracts `park_id`, converts `entry_time` to `date`, emits key `(park_id,date)` with `(group_size, 1)`
- `project_files/combiner.py` — local aggregation per key (sum(group_size), count(visits))
- `project_files/reducer.py` — final aggregation + average group size

### Hive
File:
- `project_files/hive.hql`

Logic:
1) Reads MapReduce output as external table `visits_summary`
2) Reads parks dictionary CSV as external table `parks`
3) Joins visits with parks (by `park_id`)
4) Explodes `nature_types` by `;` into separate rows
5) Aggregates metrics by (`nature_type`, `region`)
6) Writes final dataset as JSON Lines into `output_dir6`

## How to run (manual)
### 1) MapReduce
```bash
chmod +x project_files/*.sh
cd project_files

# input_dir1 should point to datasource1/*.csv
./run_mr.sh "<INPUT_DIR>/datasource1/*.csv" <OUTPUT_DIR3>
````

### 2) Hive

```bash
./run_hive.sh <OUTPUT_DIR3> "<INPUT_DIR>/datasource4" <OUTPUT_DIR6>
```

### 3) Read final output

```bash
hadoop fs -getmerge <OUTPUT_DIR6> output6.json
head output6.json
```

## Airflow orchestration (GCP Dataproc)

DAG: `airflow/projekt1.py`

Workflow:

1. Removes previous output directories from HDFS (idempotent reruns)
2. Runs MapReduce job (Streaming; branch operator allows classic vs streaming)
3. Runs Hive script via `beeline` with parameters (`input_dir3`, `input_dir4`, `output_dir6`)
4. Merges final Hive output to a single local file and prints a preview (`head`)

This project was executed on **GCP Dataproc**, with source data stored in **Google Cloud Storage (GCS)** and intermediate/final results written to **HDFS**.
