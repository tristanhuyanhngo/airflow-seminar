FROM apache/airflow:2.4.3

RUN pip install --user --upgrade pip \
    pip install apache-airflow \
    && pip install markupsafe==2.0.1 \
    && pip install sqlparse \
    && pip install pymssql \
    && pip install pyodbc \
    && pip install mysqlclient \
    && pip install mysql-connector-python \
    && pip install apache-airflow-providers-common-sql \
    && pip install apache-airflow-providers-odbc \
    && pip install apache-airflow-providers-microsoft-mssql \
    && pip install apache-airflow-providers-mysql \
    && pip install gitpython \
    && pip install CouchDB3 