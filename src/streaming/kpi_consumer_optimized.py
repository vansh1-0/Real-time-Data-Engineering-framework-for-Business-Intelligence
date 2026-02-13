"""
Optimized KPI Consumer with connection pooling and performance monitoring.
Implements batch processing, metrics, and graceful error handling.
"""

import os
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import psycopg2
from psycopg2 import pool, sql
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OptimizedKPIConsumer:
    def __init__(self, batch_size=100, max_pool_connections=5):
        self.kafka_host = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.postgres_host = os.getenv('POSTGRES_HOST', 'localhost')
        self.postgres_port = int(os.getenv('POSTGRES_PORT', 5432))
        self.postgres_db = os.getenv('POSTGRES_DB', 'bi_realtime')
        self.postgres_user = os.getenv('POSTGRES_USER', 'bi_user')
        self.postgres_password = os.getenv('POSTGRES_PASSWORD', 'bi_password')
        self.topic = os.getenv('KAFKA_TOPIC', 'market-prices')
        self.batch_size = batch_size
        self.max_pool_connections = max_pool_connections
        
        # Performance metrics
        self.metrics = {
            'messages_processed': 0,
            'batches_written': 0,
            'errors': 0,
            'start_time': time.time(),
        }
        
        # Create connection pool
        self.conn_pool = psycopg2.pool.SimpleConnectionPool(
            1,
            max_pool_connections,
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
            user=self.postgres_user,
            password=self.postgres_password
        )
        
        self.kafka_consumer = None
        self.window_buffer = defaultdict(lambda: {
            'sum_close': 0,
            'count': 0,
            'total_volume': 0
        })

    def get_connection(self):
        """Get connection from pool"""
        return self.conn_pool.getconn()

    def return_connection(self, conn):
        """Return connection to pool"""
        self.conn_pool.putconn(conn)

    def create_kafka_consumer(self):
        """Create optimized Kafka consumer"""
        try:
            self.kafka_consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.kafka_host,
                group_id='market-kpi-processor-optimized',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                max_poll_records=self.batch_size,
                session_timeout_ms=30000,
                request_timeout_ms=40000
            )
            logger.info(f"Kafka Consumer connected to {self.kafka_host}")
            return True
        except Exception as e:
            logger.error(f"Failed to create Kafka consumer: {e}")
            self.metrics['errors'] += 1
            return False

    def init_kpi_table(self):
        """Initialize KPI table if not exists"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS kpi_market_prices (
                    symbol VARCHAR(10),
                    window_start TIMESTAMP WITH TIME ZONE,
                    window_end TIMESTAMP WITH TIME ZONE,
                    avg_close NUMERIC,
                    total_volume BIGINT,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    PRIMARY KEY (symbol, window_start)
                );
            """)
            conn.commit()
            logger.info("KPI table initialized")
        except Exception as e:
            logger.error(f"Failed to initialize KPI table: {e}")
            self.metrics['errors'] += 1
        finally:
            cursor.close()
            self.return_connection(conn)

    def parse_event_time(self, ts_str):
        """Parse ISO timestamp"""
        return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))

    def window_start_for(self, ts):
        """Calculate 5-minute window start"""
        return ts.replace(second=0, microsecond=0) - \
               timedelta(minutes=ts.minute % 5)

    def upsert_kpi_batch(self, batch_data):
        """Batch upsert KPI data for better performance"""
        if not batch_data:
            return
        
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            for symbol, window_start, window_end, avg_close, total_volume in batch_data:
                cursor.execute("""
                    INSERT INTO kpi_market_prices 
                    (symbol, window_start, window_end, avg_close, total_volume, updated_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (symbol, window_start) DO UPDATE SET
                        avg_close = EXCLUDED.avg_close,
                        total_volume = EXCLUDED.total_volume,
                        updated_at = NOW();
                """, (symbol, window_start, window_end, avg_close, total_volume))
            
            conn.commit()
            self.metrics['batches_written'] += 1
            logger.info(f"Batch write: {len(batch_data)} KPI records")
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to upsert KPI batch: {e}")
            self.metrics['errors'] += 1
        finally:
            cursor.close()
            self.return_connection(conn)

    def process_message(self, message):
        """Process a single Kafka message"""
        try:
            symbol = message['symbol']
            ts_str = message['timestamp']
            close = float(message['close'])
            volume = int(message['volume']) if message.get('volume') else 0
            
            ts = self.parse_event_time(ts_str)
            window_start = self.window_start_for(ts)
            window_end = window_start + timedelta(minutes=5)
            
            # Store in window buffer
            key = (symbol, window_start.isoformat())
            self.window_buffer[key]['sum_close'] += close
            self.window_buffer[key]['count'] += 1
            self.window_buffer[key]['total_volume'] += volume
            
            self.metrics['messages_processed'] += 1
        except Exception as e:
            logger.error(f"Failed to process message: {e}")
            self.metrics['errors'] += 1

    def get_performance_metrics(self):
        """Calculate and return performance metrics"""
        elapsed = time.time() - self.metrics['start_time']
        throughput = self.metrics['messages_processed'] / elapsed if elapsed > 0 else 0
        
        return {
            'messages_processed': self.metrics['messages_processed'],
            'batches_written': self.metrics['batches_written'],
            'errors': self.metrics['errors'],
            'elapsed_seconds': int(elapsed),
            'throughput_msg_per_sec': round(throughput, 2)
        }

    def run(self):
        """Main consumer loop"""
        if not self.create_kafka_consumer():
            logger.error("Failed to start Kafka consumer")
            return
        
        self.init_kpi_table()
        
        logger.info("=" * 80)
        logger.info(f"Optimized KPI Consumer Started (batch_size={self.batch_size})")
        logger.info("=" * 80)
        
        try:
            batch_records = []
            last_metric_log = time.time()
            
            for message in self.kafka_consumer:
                self.process_message(message.value)
                
                # Flush batch when size reached
                if len(self.window_buffer) >= self.batch_size:
                    batch_records = []
                    for (symbol, window_start_iso), data in list(self.window_buffer.items()):
                        window_start = datetime.fromisoformat(window_start_iso)
                        window_end = window_start + timedelta(minutes=5)
                        avg_close = data['sum_close'] / data['count'] if data['count'] > 0 else 0
                        batch_records.append((
                            symbol,
                            window_start,
                            window_end,
                            avg_close,
                            data['total_volume']
                        ))
                    
                    if batch_records:
                        self.upsert_kpi_batch(batch_records)
                        self.window_buffer.clear()
                
                # Log metrics every 10 seconds
                if time.time() - last_metric_log > 10:
                    metrics = self.get_performance_metrics()
                    logger.info(f"Metrics: {metrics}")
                    last_metric_log = time.time()
                    
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error in consumer loop: {e}")
            self.metrics['errors'] += 1
        finally:
            logger.info("Closing consumer...")
            if self.kafka_consumer:
                self.kafka_consumer.close()
            
            # Final metrics
            final_metrics = self.get_performance_metrics()
            logger.info("=" * 80)
            logger.info("Final Metrics:")
            for key, value in final_metrics.items():
                logger.info(f"  {key}: {value}")
            logger.info("=" * 80)
            
            # Close connection pool
            self.conn_pool.closeall()

if __name__ == '__main__':
    consumer = OptimizedKPIConsumer(batch_size=50)
    consumer.run()
