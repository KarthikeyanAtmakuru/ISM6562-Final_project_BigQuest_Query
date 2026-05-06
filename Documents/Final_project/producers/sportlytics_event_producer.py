"""
-------------------------------------------------------------
Sportlytics Real-Time Player Telemetry Producer
-------------------------------------------------------------

The event producer simulates real-time player tracking data during a game.
It reads rows from a CSV file and sends them one at a time to a Kafka topic,
with a small delay to make the data feel live.

Kafka Topic:
    Name        : realtime_player_telemetry
    Partitions  : 3 (set in the Kafka/Docker setup)
    Key         : player_id (keeps events for the same player together)

"""
#Importing all required libraries
from kafka import KafkaProducer
from pyspark.sql import SparkSession
import json
import time
import pandas as pd

# Connecting to Kafka broker
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)
 
# Loading player tracking data from HDFS (requires Stage 1 to have run first)
spark = SparkSession.builder \
    .appName("Sportlytics-Producer") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

player_tracking = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://namenode:9000/sportlytics/raw/player-tracking.csv.gz") \
    .toPandas()

# Cleaning and organizing the timestamps so events stream in chronological order
player_tracking['timestamp'] = pd.to_datetime(
    player_tracking['timestamp'], errors='coerce'
)
player_tracking = player_tracking.dropna(subset=['timestamp'])
player_tracking = player_tracking.sort_values('timestamp').reset_index(drop=True)

print(f"Loaded {len(player_tracking):,} player tracking events")
print(f"Streaming to topic: realtime_player_telemetry")

# Looping through each row and sending it like it's happening in real time
for _, row in player_tracking.iterrows():
    event = {
        "game_id"             : str(row['game_id']),
        "player_id"           : str(row['player_id']),
        "team_id"             : str(row['team_id']),
        "game_timestamp"      : str(row['timestamp']),
        "event_time"          : time.time(),
        "x_court_ft"          : float(row['x_court_ft']),
        "y_court_ft"          : float(row['y_court_ft']),
        "speed_mph"           : float(row['speed_mph']),
        "acceleration"        : float(row['acceleration']),
        "cumulative_distance" : float(row['distance_covered_ft']),
        "heart_rate_bpm"      : float(row['heart_rate_bpm'])
    }
    producer.send('realtime_player_telemetry', key=event['player_id'], value=event)
    time.sleep(1/25)

producer.flush()
producer.close()
print("Done! All player telemetry events produced to Kafka.")