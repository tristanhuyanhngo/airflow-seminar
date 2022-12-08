import psycopg2
import pandas as pd
from sqlalchemy import create_engine
  
# establish connections
conn_string = 'postgresql://airflow:airflow@127.0.0.1/katinat_05'
  
db = create_engine(conn_string)
conn = db.connect()
conn1 = psycopg2.connect(
  database="katinat_05",
  user='airflow', 
  password='airflow', 
  host='127.0.0.1', 
  port= '5432'
)

conn1.autocommit = True
cursor = conn1.cursor()

csv_path = './source_system/' + "20221205_katinat_05.csv"
df = pd.read_csv(csv_path)

# converting data to sql
df.to_sql('katinat_05', conn, if_exists= 'replace')

# fetching all rows
sql1='''select * from katinat_05'''
cursor.execute(sql1)
for i in cursor.fetchall():
    print(i)
    
conn1.commit()
conn1.close()
print("success")