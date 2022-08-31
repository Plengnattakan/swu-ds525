from cassandra.cluster import Cluster
import glob
import json
import os
from typing import List

import psycopg2


#Drop table ใช้สำหรับล้าง Table เพื่อรันในครั้งต่อไป
table_drop_repo = "DROP TABLE IF EXISTS Repo"
table_drop_actor = "DROP TABLE IF EXISTS Actor"
table_drop_event = "DROP TABLE IF EXISTS Event"

#สร้างตัวแปรเก็บ sql สร้างตาราง
table_create_repo = """
    CREATE TABLE IF NOT EXISTS Repo (
        id BIGINT,
        name VARCHAR(100),
        url VARCHAR(200),
        PRIMARY KEY ((id), name)
    )
"""

table_create_actor = """
    CREATE TABLE IF NOT EXISTS Actor (
        id BIGINT,
        login VARCHAR(100),
        avatar_url VARCHAR(200),
        gravatar_id VARCHAR(50),
        url VARCHAR(200),
        followers_url VARCHAR(200),
        following_url VARCHAR(200),
        starred_url VARCHAR(200),
        subscriptions_url VARCHAR(200),
        organizations_url VARCHAR(200),
        PRIMARY KEY ((id), login)
    )
"""

table_create_event = """
    CREATE TABLE IF NOT EXISTS Event (
        id BIGINT,
        type VARCHAR(50) ,
        public VARCHAR(10) ,
        create_at TIMESTAMP,
        repo_id BIGINT ,
        repo_name VARCHAR(100) ,
        actor_id BIGINT ,
        actor_name VARCHAR(100),
        commit_sha VARCHAR(100),
        PRIMARY KEY (id),create_at)
"""
create_table_queries = [
    table_create_repo,table_create_actor,table_create_event
]
drop_table_queries = [
    table_drop_repo,table_drop_actor,table_drop_event
]

#Fuction ดึงข้อมูลจาก .json 
def get_files(filepath: str) -> List[str]:
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




def drop_tables(session):
    for query in drop_table_queries:
        try:
            rows = session.execute(query)
        except Exception as e:
            print(e)


def create_tables(session):
    for query in create_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)


def process(session, filepath):
    # Get list of files from filepath
    all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data
                print(each["id"], each["type"], each["actor"]["login"])

                # Insert data into tables here


def insert_sample_data(session):
    query = f"""
    INSERT INTO events (id, type, public) VALUES ('23487929637', 'IssueCommentEvent', true)
    """
    session.execute(query)


def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    # Create keyspace
    try:
        session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS github_events
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
            """
        )
    except Exception as e:
        print(e)

    # Set keyspace
    try:
        session.set_keyspace("github_events")
    except Exception as e:
        print(e)

    drop_tables(session)
    create_tables(session)

    # process(session, filepath="../data")
    insert_sample_data(session)

    # Select data in Cassandra and print them to stdout
    query = """
    SELECT * from events WHERE id = '23487929637' AND type = 'IssueCommentEvent'
    """
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()