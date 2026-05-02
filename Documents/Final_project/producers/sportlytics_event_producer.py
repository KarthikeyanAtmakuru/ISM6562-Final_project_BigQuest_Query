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
from kafka import KafkaProducer
import json
import time
import pandas as pd

# Connect to Kafka broker
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)
 
# Load the player tracking data from the CSV file
# In production, this feed arrives at around 25 frames per second from arena cameras. This producer simulate that rate by adjusting the event delay between 
# messages.
df = pd.read_csv(
    '/home/jovyan/data/07-sportlytics-athletics/player-tracking.csv',
    sep='\t'
)

# Clean and organize the timestamps so events stream in chronological order
df['timestamp'] = pd.to_datetime(df['timestamp'], format='%m/%d/%Y %H:%M', errors='coerce')
df = df.dropna(subset=['timestamp'])
df = df.sort_values('timestamp').reset_index(drop=True)

print(f"Streaming {len(df):,} player tracking events to Kafka...")

# Loop through each row and send it like it's happening in real time
for _, row in df.iterrows():
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

    # Send the event to Kafka and add a short delay to simulate real-time data streaming
    producer.send('realtime_player_telemetry', key=event['player_id'], value=event)
    time.sleep(1/25)

producer.flush()
producer.close()
print("Done! All player telemetry events produced.")