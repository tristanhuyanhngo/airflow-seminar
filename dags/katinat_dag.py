from datetime import datetime, timedelta
from config import CET
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
from katinat_ETL import get_data_from_source_1, get_data_from_source_2, get_data_from_source_3, get_data_from_source_4, get_data_from_source_5
from katinat_ETL import process_data_from_source_1, process_data_from_source_2, process_data_from_source_3, process_data_from_source_4, process_data_from_source_5


default_args = {
    'owner': 'trist',
    'start_date': CET,
    'email': ['tristanhuyanhngo@gmail.com'],
    'email_on_failure': ['tristanhuyanhngo@gmail.com'],
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False
}

with DAG(
    dag_id='katinat_airflow_dag',
    default_args=default_args,
    # schedule_interval='0 22 * * *',
    schedule_interval=None
) as dag:
    # Task start & end
    task_start = DummyOperator(
        task_id="start",
        dag=dag,
    )
    
    task_end = DummyOperator(
        task_id="end",
        dag=dag,
    )
    
    # Get data from sources
    task_source_1 = PythonOperator(
        task_id="get_data_source_1",
        python_callable=get_data_from_source_1,
        do_xcom_push=True
    )
    
    # task_source_2 = PythonOperator(
    #     task_id="get_data_source_2",
    #     python_callable=get_data_from_source_2,
    #     do_xcom_push=True
    # )
    
    # task_source_3 = PythonOperator(
    #     task_id="get_data_source_3",
    #     python_callable=get_data_from_source_3,
    #     do_xcom_push=True
    # )
    
    # task_source_4 = PythonOperator(
    #     task_id="get_data_source_4",
    #     python_callable=get_data_from_source_4,
    #     do_xcom_push=True
    # )
    
    # Processing data from sources
    task_process_data_source_1 = PythonOperator(
	    task_id='process_data_source_1',
	    python_callable=process_data_from_source_1
    )
    
    # task_process_data_source_2 = PythonOperator(
	#     task_id='process_data_source_2',
	#     python_callable=process_data_from_source_2
    # )
    
    # task_process_data_source_3 = PythonOperator(
	#     task_id='process_data_source_3',
	#     python_callable=process_data_from_source_3
    # )
    
    # task_process_data_source_4 = PythonOperator(
	#     task_id='process_data_source_4',
	#     python_callable=process_data_from_source_4
    # )
    
    # Truncate Stage    
    task_truncate_stage = PostgresOperator(
        task_id='truncate_stage_table',
        postgres_conn_id='postgres_stage_localhost',
        sql="""
            truncate table katinat_customer_rainbow_drink
        """
    )
    
    # Save to Postgres
    task_save_to_stage_1 = BashOperator(
        task_id='save_stage_1',
        bash_command=(
            'psql -d stage -U airflow -c "'
            #'psql postgresql://airflow:airflow@localhost:5432/stage "'
            'COPY katinat_customer_rainbow_drink(Name, Gender, TimeArrived, TimeAway) '
            "FROM '/tmp/20221205_katinat_01.csv' "
            "DELIMITER ',' "
            'CSV HEADER"'
        )
    )
    
    # task_save_to_stage_2 = BashOperator(
    #     task_id='save_stage_2',
    #     bash_command=(
    #         'psql -d stage -U airflow -c "'
    #         'COPY katinat_customer_rainbow_drink(Name,Gender,TimeArrived,TimeAway)'
    #         "FROM '/tmp/20221205_katinat_02.csv' "
    #         "DELIMITER ',' "
    #         'CSV HEADER"'
    #     )
    # )
    
    # task_save_to_stage_3 = BashOperator(
    #     task_id='save_stage_3',
    #     bash_command=(
    #         'psql -d stage -U airflow -c "'
    #         'COPY katinat_customer_rainbow_drink(Name,Gender,TimeArrived,TimeAway)'
    #         "FROM '/tmp/20221205_katinat_03.csv' "
    #         "DELIMITER ',' "
    #         'CSV HEADER"'
    #     )
    # )
    
    # task_save_to_stage_4 = BashOperator(
    #     task_id='save_stage_4',
    #     bash_command=(
    #         'psql -d stage -U airflow -c "'
    #         'COPY katinat_customer_rainbow_drink(Name,Gender,TimeArrived,TimeAway)'
    #         "FROM '/tmp/20221205_katinat_04.csv' "
    #         "DELIMITER ',' "
    #         'CSV HEADER"'
    #     )
    # )
    
# ------------------------------------------------------------------------
# task_start >> [task_source_1, task_source_2, task_source_3, task_source_4] 

# task_source_1 >> task_process_data_source_1
# task_source_2 >> task_process_data_source_2
# task_source_3 >> task_process_data_source_3
# task_source_4 >> task_process_data_source_4

# [task_process_data_source_1, task_process_data_source_2, task_process_data_source_3, task_process_data_source_4] >> task_truncate_stage 

# task_truncate_stage >> task_save_to_stage_1 >> task_end

task_start >> task_source_1 >> task_process_data_source_1 >> task_truncate_stage >> task_save_to_stage_1 >> task_end
