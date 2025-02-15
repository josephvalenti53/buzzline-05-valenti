import json
import sqlite3
from kafka import KafkaConsumer
from collections import defaultdict

# Database setup
db_path = "author_review.sqlite"

def init_db():
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS streamed_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                author TEXT,
                author_count INTEGER,
                sentiment REAL,
                category TEXT
            )
        """)
        conn.commit()

# Insert message into DB
def insert_message(message, author_count):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO streamed_messages (
                author, author_count, sentiment, category
            ) VALUES (?, ?, ?, ?)
        """, (
            message["author"], author_count, 
            message["sentiment"], message["category"]
        ))
        conn.commit()

# Kafka consumer setup
topic = "your_kafka_topic"
kafka_server = "your_kafka_broker_address"
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=kafka_server,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

author_counts = defaultdict(int)
init_db()

# Process messages
for msg in consumer:
    data = msg.value
    author = data["author"]
    author_counts[author] += 1
    
    if author_counts[author] <= 100:
        insert_message(data, author_counts[author])
    
    if all(count >= 100 for count in author_counts.values()):
        print("All authors have reached 100 messages. Stopping consumer.")
        break
