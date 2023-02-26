"""Airflow dags.

Steps of ETL pipeline:
- Scrape threads from Reddit
- Store threads in .csv
- Classify text in threads, get which ones display outrage
- Send DMs
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from analysis.perform_analysis import generate_analyses
from lib.init_session import init_pipeline_run_parameters
from message.handle_messages import receive_messages, send_messages
from ml.inference import classify_reddit_text
from scrape.get_labeled_samples import scrape_reddit_threads


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['adminsb'],
    'email_on_fallure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG(
    'reddit_dag',
    default_args=default_args,
    description='DAG for running Reddit research ETL pipeline.'
)

init_run_parameters = PythonOperator(
    task_id="init_run_parameters",
    python_callable=init_pipeline_run_parameters,
    op_kwargs={'kwarg1': 'foo', 'kwarg2': 'bar'},
    dag=dag
)

scrape_threads = PythonOperator(
    task_id="scrape_threads",
    python_callable=scrape_reddit_threads,
    dag=dag
)

classify_threads = PythonOperator(
    task_id="classify_threads",
    python_callable=classify_reddit_text,
    dag=dag
)

send_reddit_messages = PythonOperator(
    task_id="send_reddit_messages",
    python_callable=send_messages,
    dag=dag
)

receive_reddit_messages = PythonOperator(
    task_id="receive_reddit_messages",
    python_callable=receive_messages,
    dag=dag
)

generate_study_analyses = PythonOperator(
    task_id="generate_study_analyses",
    python_callable=generate_analyses,
    dag=dag
)

(
    init_run_parameters
    >> scrape_threads
    >> classify_threads
    >> send_reddit_messages
    >> receive_reddit_messages
    >> generate_study_analyses
)