import json
import os
import pathlib
import sys
import sqlite3  # Add this import for sqlite3
from kafka import KafkaConsumer
from collections import defaultdict
import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available
from consumers.db_sqlite_case import init_db, insert_message

# Initialize the SQLite database with the `author_count` field
def init_db(sql_path: pathlib.Path):
    try:
        logger.info("Initializing database.")
        with sqlite3.connect(sql_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS streamed_messages;")
            cursor.execute("""
                CREATE TABLE streamed_messages (
                    ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
                    author TEXT,
                    author_count INTEGER,
                    sentiment REAL,
                    category TEXT,
                    message TEXT,
                    timestamp TEXT,
                    keyword_mentioned TEXT,
                    message_length INTEGER
                )
            """)
            conn.commit()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")

# Insert message into the database including the author_count
def insert_message(message: dict, author_count: int, sql_path: pathlib.Path):
    try:
        with sqlite3.connect(sql_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO streamed_messages (
                    author, author_count, sentiment, category, message, timestamp,
                    keyword_mentioned, message_length
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                message["author"], author_count, message["sentiment"], message["category"],
                message["message"], message["timestamp"], message["keyword_mentioned"],
                message["message_length"]
            ))
            conn.commit()
        logger.info(f"Message from {message['author']} inserted successfully.")
    except Exception as e:
        logger.error(f"Error inserting message: {e}")

# Process message and add author_count field
def process_message(message: dict, author_counts: defaultdict) -> dict:
    try:
        author = message.get("author")
        author_counts[author] += 1  # Increment author count
        processed_message = {
            "message": message.get("message"),
            "author": author,
            "timestamp": message.get("timestamp"),
            "category": message.get("category"),
            "sentiment": float(message.get("sentiment", 0.0)),
            "keyword_mentioned": message.get("keyword_mentioned"),
            "message_length": int(message.get("message_length", 0)),
        }
        return processed_message, author_counts[author]
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None, None

# Consume messages from Kafka and insert them into the database
def consume_messages_from_kafka(topic: str, kafka_url: str, group: str, sql_path: pathlib.Path, interval_secs: int):
    logger.info("Starting Kafka consumer...")

    # Ensure Kafka services are available
    try:
        verify_services()
    except Exception as e:
        logger.error(f"ERROR: Kafka services verification failed: {e}")
        sys.exit(11)

    # Create Kafka consumer
    try:
        consumer: KafkaConsumer = create_kafka_consumer(
            topic, group, value_deserializer_provided=lambda x: json.loads(x.decode("utf-8"))
        )
    except Exception as e:
        logger.error(f"ERROR: Could not create Kafka consumer: {e}")
        sys.exit(11)

    # Verify Kafka topic exists
    if consumer is not None:
        try:
            is_topic_available(topic)
            logger.info(f"Kafka topic '{topic}' is ready.")
        except Exception as e:
            logger.error(f"ERROR: Topic '{topic}' does not exist. : {e}")
            sys.exit(13)

    # Track the message count for each author
    author_counts = defaultdict(int)

    try:
        for message in consumer:
            processed_message, author_count = process_message(message.value, author_counts)

            # If the message is valid and processed
            if processed_message:
                # Insert the message with author_count
                insert_message(processed_message, author_count, sql_path)

            # Check if all authors have sent 100 messages
            if all(count >= 100 for count in author_counts.values()):
                logger.info("All authors have reached 100 messages. Stopping consumer.")
                break

    except Exception as e:
        logger.error(f"ERROR: Could not consume messages from Kafka: {e}")
        raise

def main():
    logger.info("Starting Consumer to run continuously.")
    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()
        interval_secs: int = config.get_message_interval_seconds_as_int()
        sqlite_path: pathlib.Path = pathlib.Path("C:/Users/18165/Documents/44671_Streaming/buzzline-05-valenti/data/author_review.sqlite")

        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
            logger.info("SUCCESS: Deleted prior DB file.")
        except Exception as e:
            logger.error(f"ERROR: Failed to delete DB file: {e}")
            sys.exit(2)

    try:
        init_db(sqlite_path)
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize DB table: {e}")
        sys.exit(3)

    try:
        consume_messages_from_kafka(
            topic, kafka_url, group_id, sqlite_path, interval_secs
        )
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Consumer shutting down.")

if __name__ == "__main__":
    main()
