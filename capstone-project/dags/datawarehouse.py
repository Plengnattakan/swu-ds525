import os
import glob
from sqlite3 import Timestamp
from typing import List
import json
from datetime import datetime
import psycopg2

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
# from airflow.operators.bash_operator import BashOperator
# from airflow.hooks.postgres_hook import PostgresHook

curr_date = datetime.today().strftime('%Y-%m-%d')


create_table_queries = [
    """
    CREATE TABLE IF NOT EXISTS edu (
        edu_id bigint,
        education text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS job (
        job_id bigint,
        job text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS offering (
        age int,
        job_id bigint,
        edu_id bigint,
        contact text,
        month text,
        day int,
        duration int,
        campaign int,
        pdays int,
        outcome text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS offering_dwh (
        age int,
        job_id bigint,
        job text,
        edu_id bigint,
        education text,
        contact text,
        month text,
        day int,
        duration int,
        campaign int,
        pdays int,
        outcome text,
        date_oprt date
    )
    """,
]

truncate_table_queries = [
    """
    truncate table edu
    """,
    """
    truncate table job
    """,
    """
    truncate table offering
    """,
    """
    truncate table offering_dwh
    """,
 
]

# cat ~/.aws/credentials
# https://stackoverflow.com/questions/15261743/how-to-copy-csv-data-file-to-amazon-redshift

# cat ~/.aws/credentials

access_key_id = "ASIAUDZFW3BZA553QT5L"
secret_access_key = "l26ktFobk0iEt93APaej15XB83Yo19Rlt5xSD1nD"
session_token  = "FwoGZXIvYXdzEFUaDDSoBsxOH5ogg6wHMiLQAbJWfIDBvXBVaIFCa/g4yr/mxHdsoDpl6JWCICLojIVTmJTymv6NaZXhHardmDuLuOfn1fTUMeeUm8aRrRL6DcZDb6MS6e66Zek4NzSst9JAfKS0QJvQvH2eUdSGHkPOVG+eXQa64S2HrqRT6dRGpVX0mzWWj5RYEtI1pjFvJkm92fj8z3wPSuG8yPwAUUg223GgnYIMPXlPPtUG6waVLQcexIcnuSuIAPni2IgmlwrV0q6fRwdYNY80hj90zy1F7efcsmie2/z291qQS1pgBlUo3IHQnAYyLXWXUOcX88WUPOaqBoj5yc+SO7Dzel7n0eb/n6vvKVvzyphQ6RsZfzflwqlGMw=="
copy_table_queries = [
    """
    COPY edu 
    FROM 's3://datalake-bank-pleng/export/edu'
    ACCESS_KEY_ID '{1}'
    SECRET_ACCESS_KEY '{2}'
    SESSION_TOKEN '{3}'
    CSV
    DELIMITER ','
    IGNOREHEADER 1
    """,
    """
    COPY job 
    FROM 's3://datalake-bank-pleng/export/job'
    ACCESS_KEY_ID '{1}'
    SECRET_ACCESS_KEY '{2}'
    SESSION_TOKEN '{3}'
    CSV
    DELIMITER ','
    IGNOREHEADER 1
    """,
    """
    COPY offering 
    FROM 's3://datalake-bank-pleng/export/offering'
    ACCESS_KEY_ID '{1}'
    SECRET_ACCESS_KEY '{2}'
    SESSION_TOKEN '{3}'
    CSV
    DELIMITER ','
    IGNOREHEADER 1
    """,
]

insert_table_queries = [
    """
    INSERT INTO offering_dwh 
    select offering.age
    , offering.job_id
    , job.job
    , offering.edu_id
    , edu.education
    , offering.contact
    , offering.month
    , offering.day
    , offering.duration
    , offering.campaign
    , offering.pdays
    , offering.outcome
    , current_date
    from (
        select distinct 
            age
            , job_id
            , edu_id
            , contact
            , month
            , day
            , duration
            , campaign
            , pdays
            , outcome
        from offering 
    ) offering
    inner join job job 
        on job.job_id = offering.job_id
    inner join edu edu 
        on edu.edu_id = offering.edu_id
    """,
]

host = "redshift-cluster-1.ci0boaeqvdep.us-east-1.redshift.amazonaws.com"
port = "5439"
dbname = "dev"
user = "awsuser"
password = "Pleng056720990"
conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
conn = psycopg2.connect(conn_str)
cur = conn.cursor()

def _create_tables():
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def _truncate_tables():
    for query in truncate_table_queries:
        cur.execute(query)
        conn.commit()


def _load_staging_tables():
    for query in copy_table_queries:
        cur.execute(query.format(curr_date, access_key_id, secret_access_key, session_token))
        conn.commit()


def _insert_tables():
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


with DAG(
    'Capstone',
    start_date = timezone.datetime(2022, 12, 1),
    schedule = '@monthly',
    tags = ['capstone'],
    catchup = False,
) as dag:

    create_tables = PythonOperator(
        task_id = 'create_tables',
        python_callable = _create_tables,
    )

    truncate_tables = PythonOperator(
        task_id = 'truncate_tables',
        python_callable = _truncate_tables,
    )

    load_staging_tables = PythonOperator(
        task_id = 'load_staging_tables',
        python_callable = _load_staging_tables,
    )

    insert_tables = PythonOperator(
        task_id = 'insert_tables',
        python_callable = _insert_tables,
    )

    create_tables >> truncate_tables >> load_staging_tables >> insert_tables