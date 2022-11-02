import json
import glob
import os
from typing import List

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _get_files(filepath: str) -> List[str]:
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files


def _create_tables():
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    #สร้างตัวแปรเก็บ sql สร้างตาราง
    table_create_repo = """
        CREATE TABLE IF NOT EXISTS Repo (
            repo_id BIGINT NOT NULL,
            name VARCHAR(100) NOT NULL,
            url VARCHAR(200) NOT NULL,
            PRIMARY KEY (repo_id)
        )
    """

    table_create_actor = """
        CREATE TABLE IF NOT EXISTS Actor (
            actor_id BIGINT NOT NULL,
            login VARCHAR(100) NOT NULL,
            display_login VARCHAR(100) NOT NULL,
            gravatar_id VARCHAR(50),
            url VARCHAR(200) NOT NULL,
            avartar_url VARCHAR(200) NOT NULL,
            PRIMARY KEY (actor_id)
        )
    """

    table_create_user = """
        CREATE TABLE IF NOT EXISTS UserT (
            user_id BIGINT NOT NULL,
            login VARCHAR(100) NOT NULL,
            PRIMARY KEY (user_id)
        )
    """

    table_create_comment = """
        CREATE TABLE IF NOT EXISTS Comment (
            comment_id BIGINT NOT NULL,
            url VARCHAR(200) NOT NULL,
            html_url VARCHAR(200) NOT NULL,
            user_id BIGINT,
            PRIMARY KEY (comment_id),
            FOREIGN KEY (user_id) REFERENCES UserT (user_id)
        )
    """

    # table_create_commit = """
    #     CREATE TABLE IF NOT EXISTS Commits (
    #         commit_sha VARCHAR(300) NOT NULL,
    #         message VARCHAR(200) ,
    #         url VARCHAR(500) ,
    #         PRIMARY KEY (commit_sha)
    #     )
    # """

    table_create_payload = """
        CREATE TABLE IF NOT EXISTS Payload (
            push_id BIGINT,
            size INT,
            distinct_size INT,
            ref VARCHAR(200),
            head VARCHAR(200),
            comment_id BIGINT,
            PRIMARY KEY (push_id),
            FOREIGN KEY (comment_id) REFERENCES Comment (comment_id)
        )
    """

    table_create_event = """
        CREATE TABLE IF NOT EXISTS Event (
            event_id BIGINT NOT NULL,
            type VARCHAR(50) NOT NULL,
            public VARCHAR(10) NOT NULL,
            create_at TIMESTAMP NOT NULL,
            repo_id BIGINT NOT NULL,
            actor_id BIGINT NOT NULL,
            push_id BIGINT,
            PRIMARY KEY (event_id),
            FOREIGN KEY (repo_id)  REFERENCES Repo  (repo_id),
            FOREIGN KEY (actor_id) REFERENCES Actor (actor_id),
            FOREIGN KEY (push_id) REFERENCES Payload (push_id)
        )
    """
    create_table_queries = [
        table_create_repo,table_create_actor,table_create_user,table_create_comment,table_create_payload,table_create_event
    ]
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def _process(**context):
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    ti = context["ti"]

    #สร้างตัวแปรเก็บ sql สร้างตาราง
    table_insert_repo = """
        INSERT INTO Repo (repo_id,name,url) VALUES %s 
        ON CONFLICT (repo_id) DO NOTHING 
    """
    table_insert_actor = """
        INSERT INTO Actor (
            actor_id,login,display_login,gravatar_id,url,avartar_url
        ) VALUES %s 
        ON CONFLICT (actor_id) DO NOTHING
    """
    table_insert_user = """
        INSERT INTO UserT (
            user_id,login
        ) VALUES %s 
        ON CONFLICT (user_id) DO NOTHING
    """

    table_insert_comment = """
        INSERT INTO Comment VALUES %s 
        ON CONFLICT (comment_id) DO NOTHING
    """
    # table_insert_commit = """
    #     INSERT INTO Commits VALUES %s 
    #     ON CONFLICT (commit_sha) DO NOTHING
    # """

    table_insert_payload  = """
        INSERT INTO Payload VALUES %s 
        ON CONFLICT (push_id) DO NOTHING
    """

    table_insert_event  = """
        INSERT INTO Event VALUES %s 
        ON CONFLICT (event_id) DO NOTHING
    """

    # Get list of files from filepath
    all_files = ti.xcom_pull(task_ids="get_files", key="return_value")
    # all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                
                # Insert data into repo tables 
                col_repo = each["repo"]["id"], each["repo"]["name"], each["repo"]["url"]
                sql_insert = table_insert_repo % str(col_repo)
                #print(sql_insert)
                cur.execute(sql_insert)
                conn.commit()

                # Insert data into Actor table 
                col_actor = each["actor"]["id"], each["actor"]["login"], each["actor"]["display_login"], each["actor"]["gravatar_id"], each["actor"]["url"], each["actor"]["avatar_url"]
                sql_insert = table_insert_actor % str(col_actor)
                cur.execute(sql_insert)
                conn.commit()

                # Insert data into usert tables
                try:
                    col_user = each["payload"]["comment"]["user"]["id"], each["payload"]["comment"]["user"]["login"]
                    sql_insert = table_insert_user % str(col_user)
                    cur.execute(sql_insert)
                    conn.commit()
                except: pass
            
                #Insert data into comment table
                try: 
                    col_comment = each["payload"]["comment"]["id"],each["payload"]["comment"]["url"],each["payload"]["comment"]["html_url"], each["payload"]["comment"]["user"]["id"]
                    sql_insert = table_insert_comment % str(col_comment)
                    cur.execute(sql_insert)
                    conn.commit()
                except: pass

                #Insert data into commits table
                # try:
                #     col_commit = each["payload"]["commits"]["sha"],each["payload"]["commits"]["message"],each["payload"]["commits"]["url"]
                #     sql_insert = table_insert_commit % str(col_commit)
                #     print(sql_insert)
                #     cur.execute(sql_insert)
                #     conn.commit()
                # except: pass

                #Insert data into payload table
                try: 
                    try: col_payload = each["payload"]["push_id"],each["payload"]["size"],each["payload"]["distinct_size"],each["payload"]["ref"],each["payload"]["head"],
                    except: col_payload = each["payload"]["push_id"],each["payload"]["size"],each["payload"]["distinct_size"],each["payload"]["ref"],each["payload"]["head"],each["payload"]["comment"]["id"]
                    sql_insert = table_insert_payload % str(col_payload)
                    #print(sql_insert)
                    cur.execute(sql_insert)
                    conn.commit()
                except: pass

                # Insert data into event table 
                try: col_event = each["id"], each["type"], each["public"], each["created_at"], each["repo"]["id"], each["actor"]["id"],each["payload"]["push_id"]
                except:col_event = each["id"], each["type"], each["public"], each["created_at"], each["repo"]["id"], each["actor"]["id"],
                sql_insert = table_insert_event % str(col_event)
                #print(sql_insert)
                cur.execute(sql_insert)
                conn.commit()


with DAG(
    "etl",
    start_date=timezone.datetime(2022, 11, 2),
    schedule="@daily",
    tags=["Project5"],
    catchup=False,
) as dag:

    get_files = PythonOperator(
        task_id="get_files",
        python_callable=_get_files,
        op_kwargs={
            "filepath": "/opt/airflow/dags/data",
        }
    )

    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=_create_tables,
    )
    
    process = PythonOperator(
        task_id="process",
        python_callable=_process,
    )

    [get_files, create_tables] >> process
