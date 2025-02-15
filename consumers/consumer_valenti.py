import json
import pathlib
import sqlite3
from kafka import KafkaConsumer
from collections import defaultdict
import utils.utils_config as config
from utils.utils_logger import logger

# Database setup
db_path = config.get_base_data_path() / "author_review.sqlite"

def init_db():
    logger.info("Initializing database.")
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS streamed_messages;")
        cursor.execute("""
            CREATE TABLE streamed_messages (
                ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
                author TEXT,
                author_count INTEGER,
                sentiment REAL,
                category TEXT
            )
        """)
        conn.commit()
    logger.info("Database initialized.")

# Insert message into DB
def insert_message(message, author_count):
    logger.info(f"Inserting message from {message['author']} with count {author_count}.")
    try:
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
        logger.info("Message inserted successfully.")
    except Exception as e:
        logger.error(f"Error inserting message: {e}")

# Kafka consumer setup
topic = "your_kafka_topic"
kafka_server = "localhost:9092"
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=kafka_server,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

author_counts = defaultdict(int)
init_db()

# Process messages
logger.info("Starting Kafka consumer.")
for msg in consumer:
    data = msg.value
    author = data["author"]
    author_counts[author] += 1
    
    if author_counts[author] <= 100:
        insert_message(data, author_counts[author])
    
    if all(count >= 100 for count in author_counts.values()):
        logger.info("All authors have reached 100 messages. Stopping consumer.")
        break
