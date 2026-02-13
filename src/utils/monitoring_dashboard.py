"""
Real-time monitoring dashboard for the pipeline.
Displays metrics, alerts, and system status.
"""

import os
import sys
import time
import logging
from datetime import datetime, timezone
import psycopg2
from kafka import KafkaAdminClient
from tabulate import tabulate

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MonitoringDashboard:
    def __init__(self, refresh_interval=5):
        self.kafka_host = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.postgres_host = os.getenv('POSTGRES_HOST', 'localhost')
        self.postgres_port = int(os.getenv('POSTGRES_PORT', 5432))
        self.postgres_db = os.getenv('POSTGRES_DB', 'bi_realtime')
        self.postgres_user = os.getenv('POSTGRES_USER', 'bi_user')
        self.postgres_password = os.getenv('POSTGRES_PASSWORD', 'bi_password')
        self.refresh_interval = refresh_interval

    def get_kafka_metrics(self):
        """Get Kafka broker and topic metrics"""
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=self.kafka_host)
            cluster = admin_client.describe_cluster(timeout_ms=5000)
            admin_client.close()
            
            return {
                'broker_count': len(cluster.brokers()),
                'controller_id': cluster.controller,
                'status': 'OK'
            }
        except Exception as e:
            return {'status': f'ERROR: {str(e)[:50]}'}

    def get_postgres_metrics(self):
        """Get PostgreSQL metrics"""
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
            
            # Row count
            cursor.execute("SELECT COUNT(*) FROM kpi_market_prices;")
            row_count = cursor.fetchone()[0]
            
            # Latest data
            cursor.execute("""
                SELECT symbol, MAX(window_start) as latest_timestamp 
                FROM kpi_market_prices 
                GROUP BY symbol 
                ORDER BY symbol;
            """)
            latest_data = cursor.fetchall()
            
            # Size
            cursor.execute("""
                SELECT pg_size_pretty(pg_total_relation_size('kpi_market_prices'));
            """)
            table_size = cursor.fetchone()[0]
            
            cursor.close()
            conn.close()
            
            return {
                'total_rows': row_count,
                'table_size': table_size,
                'latest_data': latest_data,
                'status': 'OK'
            }
        except Exception as e:
            return {'status': f'ERROR: {str(e)[:50]}'}

    def get_kpi_statistics(self):
        """Get KPI statistics by symbol"""
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
                SELECT 
                    symbol,
                    COUNT(*) as window_count,
                    ROUND(AVG(avg_close)::numeric, 2) as avg_price,
                    ROUND(MIN(avg_close)::numeric, 2) as min_price,
                    ROUND(MAX(avg_close)::numeric, 2) as max_price,
                    SUM(total_volume) as total_volume
                FROM kpi_market_prices
                GROUP BY symbol
                ORDER BY symbol;
            """)
            stats = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            return stats
        except Exception as e:
            logger.error(f"Failed to get KPI stats: {e}")
            return []

    def print_dashboard(self):
        """Print formatted monitoring dashboard"""
        os.system('clear' if os.name == 'posix' else 'cls')
        
        print("\n" + "=" * 100)
        print(f"REAL-TIME PIPELINE MONITORING DASHBOARD - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print("=" * 100)
        
        # Kafka Status
        print("\n[KAFKA STATUS]")
        kafka_metrics = self.get_kafka_metrics()
        for key, value in kafka_metrics.items():
            print(f"  {key:.<30} {value}")
        
        # PostgreSQL Status
        print("\n[POSTGRESQL STATUS]")
        postgres_metrics = self.get_postgres_metrics()
        if postgres_metrics.get('status') == 'OK':
            print(f"  Total Rows:.....................{postgres_metrics['total_rows']}")
            print(f"  Table Size:.....................{postgres_metrics['table_size']}")
            print(f"  Latest Data by Symbol:")
            for symbol, ts in postgres_metrics['latest_data']:
                print(f"    {symbol:.<25} {ts}")
        else:
            print(f"  Status: {postgres_metrics.get('status', 'UNKNOWN')}")
        
        # KPI Statistics
        print("\n[KPI STATISTICS BY SYMBOL]")
        stats = self.get_kpi_statistics()
        if stats:
            headers = ['Symbol', 'Windows', 'Avg Price', 'Min', 'Max', 'Total Volume']
            print(tabulate(stats, headers=headers, tablefmt='grid'))
        else:
            print("  No KPI data available yet")
        
        print("\n" + "=" * 100)
        print(f"Next refresh in {self.refresh_interval} seconds... (Press Ctrl+C to exit)")
        print("=" * 100 + "\n")

    def run(self):
        """Run continuous monitoring"""
        try:
            while True:
                self.print_dashboard()
                time.sleep(self.refresh_interval)
        except KeyboardInterrupt:
            print("\n\nMonitoring stopped.")
            sys.exit(0)

if __name__ == '__main__':
    dashboard = MonitoringDashboard(refresh_interval=5)
    dashboard.run()
