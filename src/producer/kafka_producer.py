"""
Kafka Producer for Real-Time Stock Market Data
Fetches OHLCV data from yfinance and streams to Kafka
"""

import os
import json
import time
import logging
from datetime import datetime
from dotenv import load_dotenv
import yfinance as yf
from kafka import KafkaProducer
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
SYMBOLS = os.getenv('SYMBOLS', 'AAPL,MSFT,GOOG').split(',')
FETCH_INTERVAL_SECONDS = int(os.getenv('FETCH_INTERVAL_SECONDS', 60))


def create_kafka_producer():
    """Create and return a Kafka producer instance"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        logger.info(f"Kafka Producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except KafkaError as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise


def fetch_stock_data(symbol):
    """Fetch 1-minute interval OHLCV data for a given symbol"""
    try:
        ticker = yf.Ticker(symbol)
        # Fetch 1-minute data for the last day
        data = ticker.history(period='1d', interval='1m')
        
        if data.empty:
            logger.warning(f"No data fetched for {symbol}")
            return None
        
        # Get the most recent data point
        latest = data.iloc[-1]
        
        stock_data = {
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'open': float(latest['Open']),
            'high': float(latest['High']),
            'low': float(latest['Low']),
            'close': float(latest['Close']),
            'volume': int(latest['Volume'])
        }
        
        logger.info(f"Fetched data for {symbol}: Close=${stock_data['close']:.2f}, Volume={stock_data['volume']}")
        return stock_data
        
    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {e}")
        return None


def send_to_kafka(producer, data):
    """Send data to Kafka topic"""
    try:
        future = producer.send(KAFKA_TOPIC, value=data)
        # Block for 'synchronous' sends
        record_metadata = future.get(timeout=10)
        logger.info(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
        return True
    except KafkaError as e:
        logger.error(f"Failed to send message to Kafka: {e}")
        return False


def main():
    """Main producer loop"""
    logger.info("Starting Kafka Producer for Stock Market Data")
    logger.info(f"Symbols: {SYMBOLS}")
    logger.info(f"Fetch Interval: {FETCH_INTERVAL_SECONDS} seconds")
    
    producer = create_kafka_producer()
    
    try:
        while True:
            logger.info("=" * 60)
            logger.info("Fetching stock data...")
            
            for symbol in SYMBOLS:
                stock_data = fetch_stock_data(symbol.strip())
                
                if stock_data:
                    success = send_to_kafka(producer, stock_data)
                    if success:
                        logger.info(f"✓ Successfully sent {symbol} data to Kafka")
                    else:
                        logger.error(f"✗ Failed to send {symbol} data to Kafka")
                else:
                    logger.warning(f"✗ No data available for {symbol}")
                
                # Small delay between symbols to avoid rate limiting
                time.sleep(1)
            
            logger.info(f"Waiting {FETCH_INTERVAL_SECONDS} seconds until next fetch...")
            time.sleep(FETCH_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed successfully")


if __name__ == "__main__":
    main()
