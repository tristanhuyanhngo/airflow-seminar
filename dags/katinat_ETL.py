import pandas as pd
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

def get_data_from_source_1():
    sql_stmt = 'SELECT * FROM katinat_01'
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_source_system_1',
        database='katinat_01'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    
    return cursor.fetchall()

def get_data_from_source_2():
    sql_stmt = 'SELECT * FROM katinat_02'
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_source_system_2',
        database='katinat_02'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    
    return cursor.fetchall()

def get_data_from_source_3():
    sql_stmt = 'SELECT * FROM katinat_03'
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_source_system_3',
        database='katinat_03'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    
    return cursor.fetchall()

def get_data_from_source_4():
    sql_stmt = 'SELECT * FROM katinat_04'
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_source_system_4',
        database='katinat_04'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    
    return cursor.fetchall()

def get_data_from_source_5():
    sql_stmt = 'SELECT * FROM katinat_05'
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_source_system_5',
        database='katinat_05'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    
    return cursor.fetchall()

def process_data_from_source_1(ti):
    source_data = ti.xcom_pull(task_ids=['get_data_source_1'])
    if not source_data:
        raise Exception('No data.')

    source_data = pd.DataFrame(
        data=source_data[0],
        columns=['id','ID','Name', 'Gender','TimeArrived','TimeAway']
    )
    source_data = source_data.drop('ID', axis=1)
    source_data = source_data.drop('id', axis=1)
    source_data.to_csv(Variable.get("20221205_katinat_01"), index=False)
    
def process_data_from_source_2(ti):
    source_data = ti.xcom_pull(task_ids=['get_data_source_2'])
    if not source_data:
        raise Exception('No data.')

    source_data = pd.DataFrame(
        data=source_data[0],
        columns=['id','ID','Name', 'Gender','TimeArrived','TimeAway']
    )
    source_data = source_data.drop('ID', axis=1)
    source_data = source_data.drop('id', axis=1)
    source_data.to_csv(Variable.get("20221205_katinat_02"), index=False)
    
def process_data_from_source_3(ti):
    source_data = ti.xcom_pull(task_ids=['get_data_source_3'])
    if not source_data:
        raise Exception('No data.')

    source_data = pd.DataFrame(
        data=source_data[0],
        columns=['id','ID','Name', 'Gender','TimeArrived','TimeAway']
    )
    source_data = source_data.drop('ID', axis=1)
    source_data = source_data.drop('id', axis=1)
    source_data.to_csv(Variable.get("20221205_katinat_03"), index=False)
    
def process_data_from_source_4(ti):
    source_data = ti.xcom_pull(task_ids=['get_data_source_4'])
    if not source_data:
        raise Exception('No data.')

    source_data = pd.DataFrame(
        data=source_data[0],
        columns=['id','ID','Name', 'Gender','TimeArrived','TimeAway']
    )
    source_data = source_data.drop('ID', axis=1)
    source_data = source_data.drop('id', axis=1)
    source_data.to_csv(Variable.get("20221205_katinat_04"), index=False)
    
def process_data_from_source_5(ti):
    source_data = ti.xcom_pull(task_ids=['get_data_source_5'])
    if not source_data:
        raise Exception('No data.')

    source_data = pd.DataFrame(
        data=source_data[0],
        columns=['id','ID','Name', 'Gender','TimeArrived','TimeAway']
    )
    source_data = source_data.drop('ID', axis=1)
    source_data = source_data.drop('id', axis=1)
    source_data.to_csv(Variable.get("20221205_katinat_05"), index=False)
    