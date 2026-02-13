"""
Alert handling system for the pipeline.
Triggers alerts on component failures or performance degradation.
"""

import os
import sys
import logging
import psycopg2
from datetime import datetime, timezone
from kafka import KafkaAdminClient
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AlertHandler:
    ALERT_LEVELS = {
        'CRITICAL': '🔴',
        'WARNING': '🟡',
        'INFO': '🟢'
    }

    def __init__(self):
        self.kafka_host = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.postgres_host = os.getenv('POSTGRES_HOST', 'localhost')
        self.postgres_port = int(os.getenv('POSTGRES_PORT', 5432))
        self.postgres_db = os.getenv('POSTGRES_DB', 'bi_realtime')
        self.postgres_user = os.getenv('POSTGRES_USER', 'bi_user')
        self.postgres_password = os.getenv('POSTGRES_PASSWORD', 'bi_password')
        self.alerts = []

    def trigger_alert(self, level, component, message):
        """Trigger an alert"""
        timestamp = datetime.now(timezone.utc).isoformat()
        alert = {
            'timestamp': timestamp,
            'level': level,
            'component': component,
            'message': message
        }
        self.alerts.append(alert)
        
        icon = self.ALERT_LEVELS.get(level, '❓')
        print(f"{icon} [{level}] {component}: {message}")
        logger.log(
            logging.CRITICAL if level == 'CRITICAL' else logging.WARNING,
            f"[{component}] {message}"
        )

    def check_kafka_availability(self):
        """Check if Kafka is available"""
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_host,
                request_timeout_ms=5000
            )
            admin_client.describe_cluster(timeout_ms=5000)
            admin_client.close()
            self.trigger_alert('INFO', 'Kafka', 'Broker is accessible')
            return True
        except Exception as e:
            self.trigger_alert('CRITICAL', 'Kafka', f'Broker unreachable: {str(e)[:60]}')
            return False

    def check_postgres_availability(self):
        """Check if PostgreSQL is available"""
        try:
            conn = psycopg2.connect(
                host=self.postgres_host,
                port=self.postgres_port,
                database=self.postgres_db,
                user=self.postgres_user,
                password=self.postgres_password,
                connect_timeout=5
            )
            conn.close()
            self.trigger_alert('INFO', 'PostgreSQL', 'Database is accessible')
            return True
        except Exception as e:
            self.trigger_alert('CRITICAL', 'PostgreSQL', f'Database unreachable: {str(e)[:60]}')
            return False

    def check_data_freshness(self, max_age_minutes=15):
        """Check if KPI data is fresh"""
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
            cursor.execute("""
                SELECT MAX(updated_at) as latest_update FROM kpi_market_prices;
            """)
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result and result[0]:
                age_minutes = (datetime.now(timezone.utc) - result[0]).total_seconds() / 60
                
                if age_minutes > max_age_minutes:
                    self.trigger_alert(
                        'WARNING',
                        'DataFreshness',
                        f'KPI data is {int(age_minutes)} minutes old (threshold: {max_age_minutes})'
                    )
                    return False
                else:
                    self.trigger_alert('INFO', 'DataFreshness', f'KPI data is fresh ({int(age_minutes)} min old)')
                    return True
            else:
                self.trigger_alert('WARNING', 'DataFreshness', 'No KPI data found')
                return False
        except Exception as e:
            self.trigger_alert('WARNING', 'DataFreshness', f'Check failed: {str(e)[:60]}')
            return False

    def check_error_rate(self, max_errors_per_minute=5):
        """Alert on high error rates (would be populated by monitoring loop)"""
        pass

    def run_all_checks(self):
        """Run all alert checks"""
        print("\n" + "=" * 70)
        print("SYSTEM ALERT CHECK")
        print("=" * 70 + "\n")
        
        kafka_ok = self.check_kafka_availability()
        postgres_ok = self.check_postgres_availability()
        data_fresh = self.check_data_freshness(max_age_minutes=10)
        
        print("\n" + "=" * 70)
        print(f"ALERTS: {len(self.alerts)} total")
        print("=" * 70 + "\n")
        
        return {
            'kafka_ok': kafka_ok,
            'postgres_ok': postgres_ok,
            'data_fresh': data_fresh,
            'all_ok': all([kafka_ok, postgres_ok, data_fresh])
        }

if __name__ == '__main__':
    handler = AlertHandler()
    status = handler.run_all_checks()
    
    sys.exit(0 if status['all_ok'] else 1)
