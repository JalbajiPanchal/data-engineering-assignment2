from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='customer_transaction_pipeline',
    default_args=default_args,
    description='Delta Lake -> Spark ETL -> ScyllaDB Pipeline',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['assignment2', 'delta-lake', 'scylladb'],
) as dag:

    check_delta_lake = BashOperator(
        task_id='check_delta_lake',
        bash_command='''
            echo "Checking Delta Lake directory..."
            if [ -d "/opt/spark/delta-lake/customer_transactions" ]; then
                echo "Delta Lake directory exists"
            else
                echo "Delta Lake directory missing!"
                exit 1
            fi
        ''',
    )

    check_scylladb = BashOperator(
        task_id='check_scylladb_connection',
        bash_command='''
            echo "Checking ScyllaDB connection..."
            python3 -c "
import socket
try:
    s = socket.create_connection(('scylladb', 9042), timeout=10)
    s.close()
    print('ScyllaDB is reachable')
except Exception as e:
    print('ScyllaDB not reachable: ' + str(e))
    exit(1)
"
        ''',
    )

    run_spark_etl = BashOperator(
        task_id='run_spark_etl',
        bash_command='''
            echo "Starting Spark ETL Job..."
            docker exec assignment2-spark-master-1 \
            /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --packages io.delta:delta-core_2.12:2.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
            --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
            --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
            --conf spark.cassandra.connection.host=scylladb \
            /opt/spark/spark_jobs/etl_job.py
            echo "Spark ETL Job Complete!"
        ''',
        execution_timeout=timedelta(minutes=30),
    )

    verify_scylladb = BashOperator(
        task_id='verify_scylladb_data',
        bash_command='''
            echo "Verifying data in ScyllaDB..."
            python3 -c "
from cassandra.cluster import Cluster
try:
    cluster = Cluster(['scylladb'])
    session = cluster.connect('transactions_ks')
    rows = session.execute('SELECT COUNT(*) FROM daily_customer_totals')
    count = rows.one()[0]
    print('ScyllaDB has ' + str(count) + ' rows in daily_customer_totals')
    cluster.shutdown()
except Exception as e:
    print('Verification failed: ' + str(e))
    exit(1)
"
        ''',
    )

    log_completion = BashOperator(
        task_id='log_completion',
        bash_command='''
            echo "Pipeline completed successfully!"
            echo "Run date: $(date)"
            echo "All tasks finished"
        ''',
    )

    check_delta_lake >> check_scylladb >> run_spark_etl >> verify_scylladb >> log_completion