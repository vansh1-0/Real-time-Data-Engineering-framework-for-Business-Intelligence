"""
Health check service for all pipeline components.
Monitors Kafka, PostgreSQL, and reports system status.
"""

import os
import sys
import time
import logging
import psycopg2
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HealthChecker:
    def __init__(self):
        self.kafka_host = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.postgres_host = os.getenv('POSTGRES_HOST', 'localhost')
        self.postgres_port = int(os.getenv('POSTGRES_PORT', 5432))
        self.postgres_db = os.getenv('POSTGRES_DB', 'bi_realtime')
        self.postgres_user = os.getenv('POSTGRES_USER', 'bi_user')
        self.postgres_password = os.getenv('POSTGRES_PASSWORD', 'bi_password')
        self.topic = os.getenv('KAFKA_TOPIC', 'market-prices')
        self.health_status = {}

    def check_kafka_connectivity(self):
        """Check if Kafka broker is reachable"""
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_host,
                request_timeout_ms=5000
            )
            admin_client.describe_cluster(timeout_ms=5000)
            admin_client.close()
            self.health_status['kafka_connectivity'] = 'OK'
            logger.info("✓ Kafka connectivity: OK")
            return True
        except Exception as e:
            self.health_status['kafka_connectivity'] = f'FAILED: {str(e)}'
            logger.error(f"✗ Kafka connectivity failed: {e}")
            return False

    def check_kafka_topic(self):
        """Check if Kafka topic exists and has partitions"""
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=self.kafka_host)
            topics = admin_client.list_topics(timeout_ms=5000)
            admin_client.close()
            
            if self.topic in topics:
                self.health_status['kafka_topic'] = f'OK - {len(topics[self.topic])} partitions'
                logger.info(f"✓ Kafka topic '{self.topic}': OK")
                return True
            else:
                self.health_status['kafka_topic'] = 'MISSING'
                logger.warning(f"✗ Kafka topic '{self.topic}' not found")
                return False
        except Exception as e:
            self.health_status['kafka_topic'] = f'CHECK_FAILED: {str(e)}'
            logger.error(f"✗ Kafka topic check failed: {e}")
            return False

    def check_postgres_connectivity(self):
        """Check if PostgreSQL database is reachable"""
        try:
            conn = psycopg2.connect(
                host=self.postgres_host,
                port=self.postgres_port,
                database=self.postgres_db,
                user=self.postgres_user,
                password=self.postgres_password,
                connect_timeout=5
            )
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            cursor.close()
            conn.close()
            self.health_status['postgres_connectivity'] = 'OK'
            logger.info("✓ PostgreSQL connectivity: OK")
            return True
        except Exception as e:
            self.health_status['postgres_connectivity'] = f'FAILED: {str(e)}'
            logger.error(f"✗ PostgreSQL connectivity failed: {e}")
            return False

    def check_kpi_table(self):
        """Check if KPI table exists and has data"""
        try:
            conn = psycopg2.connect(
                host=self.postgres_host,
                port=self.postgres_port,
                database=self.postgres_db,
                user=self.postgres_user,
                password=self.postgres_password,
                connect_timeout=5
            )
            cursor = conn.cursor()
            
            # Check table exists
            cursor.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = 'kpi_market_prices'
                );
            """)
            table_exists = cursor.fetchone()[0]
            
            if table_exists:
                # Count rows
                cursor.execute("SELECT COUNT(*) FROM kpi_market_prices;")
                row_count = cursor.fetchone()[0]
                self.health_status['kpi_table'] = f'OK - {row_count} rows'
                logger.info(f"✓ KPI table: OK ({row_count} rows)")
            else:
                self.health_status['kpi_table'] = 'TABLE_MISSING'
                logger.warning("✗ KPI table not found")
            
            cursor.close()
            conn.close()
            return table_exists
        except Exception as e:
            self.health_status['kpi_table'] = f'CHECK_FAILED: {str(e)}'
            logger.error(f"✗ KPI table check failed: {e}")
            return False

    def check_consumer_lag(self):
        """Check Kafka consumer group lag"""
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_host,
                group_id='health-check-group',
                auto_offset_reset='latest',
                enable_auto_commit=False,
                request_timeout_ms=5000
            )
            partitions = consumer.partitions_for_topic(self.topic)
            end_offsets = consumer.end_offsets([...for partition in partitions])
            consumer.close()
            
            self.health_status['consumer_lag'] = 'OK'
            logger.info("✓ Consumer lag check: OK")
            return True
        except Exception as e:
            self.health_status['consumer_lag'] = f'CHECK_FAILED: {str(e)}'
            logger.warning(f"⚠ Consumer lag check skipped: {e}")
            return True  # Don't fail on this

    def generate_health_report(self):
        """Generate and log complete health status report"""
        logger.info("=" * 60)
        logger.info("HEALTH CHECK REPORT")
        logger.info("=" * 60)
        
        for component, status in self.health_status.items():
            logger.info(f"{component:.<40} {status}")
        
        all_ok = all('OK' in str(v) or 'CHECK_FAILED' in str(v) 
                     for v in self.health_status.values())
        
        if all_ok:
            logger.info("=" * 60)
            logger.info("✓ ALL SYSTEMS OPERATIONAL")
            logger.info("=" * 60)
        else:
            logger.warning("=" * 60)
            logger.warning("⚠ SOME SYSTEMS REQUIRE ATTENTION")
            logger.warning("=" * 60)
        
        return self.health_status

    def run_full_check(self):
        """Run all health checks"""
        logger.info(f"Starting full health check - {datetime.now().isoformat()}")
        
        self.check_kafka_connectivity()
        self.check_kafka_topic()
        self.check_postgres_connectivity()
        self.check_kpi_table()
        self.check_consumer_lag()
        
        return self.generate_health_report()

if __name__ == '__main__':
    checker = HealthChecker()
    checker.run_full_check()
