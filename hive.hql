DROP TABLE IF EXISTS visits_summary;
DROP TABLE IF EXISTS parks;
DROP TABLE IF EXISTS joined_data;
DROP TABLE IF EXISTS exploded_data;
DROP TABLE IF EXISTS hive_result;

CREATE EXTERNAL TABLE visits_summary (
  park_id STRING,
  `date` STRING,
  visits_count INT,
  avg_group_size DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hiveconf:input_dir3}';

CREATE EXTERNAL TABLE parks (
  park_id STRING,
  name STRING,
  region STRING,
  facilities STRING,
  attractions STRING,
  nature_types STRING,
  established_year INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = "\"",
  "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '${hiveconf:input_dir4}'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE joined_data AS
SELECT
  v.park_id,
  p.region,
  p.nature_types,
  v.visits_count,
  v.avg_group_size
FROM visits_summary v
JOIN parks p
  ON v.park_id = p.park_id;

CREATE TABLE exploded_data AS
SELECT
  park_id,
  region,
  TRIM(nature_type) AS nature_type,
  visits_count,
  avg_group_size
FROM joined_data
LATERAL VIEW explode(split(nature_types, ';')) s AS nature_type;

CREATE TABLE hive_result AS
SELECT
  nature_type,
  region,
  SUM(visits_count)   AS total_visits,
  AVG(avg_group_size) AS avg_group_size
FROM exploded_data
GROUP BY nature_type, region;

INSERT OVERWRITE DIRECTORY '${hiveconf:output_dir6}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\n'
SELECT concat(
  '{',
  '"nature_type":"', r.nature_type, '",',
  '"region":"',      r.region, '",',
  '"total_visits":', CAST(r.total_visits AS STRING), ',',
  '"avg_group_size":', CAST(ROUND(r.avg_group_size,2) AS STRING), ',',
  '"visits_deviation":', CAST(r.total_visits - a.avg_total_visits AS STRING),
  '}'
)
FROM hive_result r
JOIN (
  SELECT 
    nature_type, 
    AVG(total_visits) AS avg_total_visits
  FROM hive_result
  GROUP BY nature_type
) a
ON r.nature_type = a.nature_type;