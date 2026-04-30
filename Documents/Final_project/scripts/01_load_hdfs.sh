#!/bin/bash
# Stage 1 - Sportlytics Athletics
# Creates HDFS zone structure and loads all 5 data files

echo "===== Creating HDFS Zone Structure ====="
hdfs dfs -mkdir -p /sportlytics/raw
hdfs dfs -mkdir -p /sportlytics/processed
hdfs dfs -mkdir -p /sportlytics/analytics
echo "Zones created"

echo "===== Loading Data Files into Raw Zone ====="
hdfs dfs -put /data/07-sportlytics-athletics/player-tracking.csv.gz /sportlytics/raw/
echo "player-tracking.csv loaded"

hdfs dfs -put /data/07-sportlytics-athletics/game-stats.json.gz /sportlytics/raw/
echo "game-stats.json loaded"

hdfs dfs -put /data/07-sportlytics-athletics/injury-reports.csv.gz /sportlytics/raw/
echo "injury-reports.csv loaded"

hdfs dfs -put /data/07-sportlytics-athletics/training-sessions.json.gz /sportlytics/raw/
echo "training-sessions.json loaded"

hdfs dfs -put /data/07-sportlytics-athletics/team-schedules.csv.gz /sportlytics/raw/
echo "team-schedules.csv loaded"

echo "===== Verifying Files in HDFS ====="
hdfs dfs -ls /sportlytics/raw/

echo "===== Checking Block Distribution ====="
hdfs fsck /sportlytics/raw/player-tracking.csv.gz -files -blocks

echo "===== Done! ====="