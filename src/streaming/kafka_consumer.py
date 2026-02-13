"""
Kafka Consumer for Real-Time Market Data (Windows-Compatible Alternative)
Reads from Kafka topic and processes stock data without Spark dependency
"""

import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv('configs/.env')

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'market-prices')


def create_kafka_consumer():
    """Create and return a Kafka consumer instance"""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='market-data-processor',
            consumer_timeout_ms=1000
        )
        logger.info(f"Kafka Consumer connected to {KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"Subscribed to topic: {KAFKA_TOPIC}")
        return consumer
    except KafkaError as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        raise


def process_message(message):
    """Process a single message from Kafka"""
    try:
        data = message.value
        logger.info(f"✓ {data['symbol']}: ${data['close']:.2f} | Volume: {data['volume']} | Time: {data['timestamp']}")
        return data
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None


def main():
    """Main consumer loop"""
    logger.info("=" * 70)
    logger.info("Starting Kafka Consumer - Market Data Processor")
    logger.info("=" * 70)
    
    consumer = create_kafka_consumer()
    message_count = 0
    
    try:
        logger.info("Listening for messages... (Press Ctrl+C to stop)")
        logger.info("=" * 70)
        
        while True:
            messages = consumer.poll(timeout_ms=1000)
            
            for topic_partition, records in messages.items():
                for message in records:
                    data = process_message(message)
                    if data:
                        message_count += 1
                        
                        # Display summary every 10 messages
                        if message_count % 10 == 0:
                            logger.info(f"--- Processed {message_count} messages ---")
            
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    except Exception as e:
        logger.error(f"Consumer error: {e}")
        raise
    finally:
        consumer.close()
        logger.info(f"Consumer closed. Total messages processed: {message_count}")


if __name__ == "__main__":
    main()
