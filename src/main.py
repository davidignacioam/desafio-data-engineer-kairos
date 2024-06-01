
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from fromairflow.operators.dummy import DummyOperator

from app.config import (
    DEFAULT_AIRFLOW_ARGS,
    START_TIME
)
from app.functions import (
    insert_data_to_bigquery, 
    get_slack_message
)
from app.logger import LogConfig

from logging.config import dictConfig


dictConfig(LogConfig().dict())


with DAG(
    "car_models_data_pipeline", 
    start_date = START_TIME, 
    schedule_interval = "@daily", 
    default_args = DEFAULT_AIRFLOW_ARGS, 
    catchup = False
) as dag:

    start_process = DummyOperator(
        task_id = 'inicia_proceso'  
    )

    inserting_data_to_bigquery = PythonOperator(
        task_id = "inserting_data_to_bigquery",
        python_callable = insert_data_to_bigquery
    )

    send_email_notification = EmailOperator(
        task_id = "send_email_notification",
        to = "airflow_example@mail.com",
        subject = "car_models_data_pipeline",
        html_content = "<h3>car_models_data_pipeline</h3>"
    )

    send_slack_notification = SlackWebhookOperator(
        task_id = "send_slack_notification",
        http_conn_id = "slack_conn",
        message = get_slack_message(),
        channel = "#monitoring"
    )

    end_process = DummyOperator( 
        task_id = 'finaliza_proceso'  
    )
    
    start_process >> inserting_data_to_bigquery >> send_email_notification >> send_slack_notification >> end_process
