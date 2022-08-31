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
        id  bigint,
        name varchar,
        url varchar,
        PRIMARY KEY ((id), name)
        )
"""

table_create_actor = """
    CREATE TABLE IF NOT EXISTS Actor (
        id bigint,
        login varchar,
        avatar_url varchar,
        gravatar_id varchar,
        url varchar,
        followers_url varchar,
        following_url varchar,
        starred_url varchar,
        subscriptions_url varchar,
        organizations_url varchar,
        PRIMARY KEY ((id), login)
    )
"""

table_create_event = """
    CREATE TABLE IF NOT EXISTS Event (
        id bigint,
        type varchar ,
        public varchar,
        create_at timestamp,
        repo_id bigint,
        repo_name varchar,
        actor_id bigint,
        actor_name varchar,
        commit_sha varchar,
        PRIMARY KEY ((id),create_at)
        )
"""
create_table_queries = [
    table_create_repo,table_create_actor,table_create_event
]
drop_table_queries = [
    table_drop_repo,table_drop_actor,table_drop_event
]




table_insert_event = """
    INSERT INTO Event (
        id,type,public,create_at,repo_id,repo_name,actor_id,actor_name,commit_sha
    ) VALUES %s 
    
"""



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

                # Insert data into repo tables 

                query_repo = "INSERT INTO Repo (id,name,url) VALUES (%s, '%s', %s)" \
                        % (each["repo"]["id"], each["repo"]["name"], each["repo"]["url"])
                session.execute(query_repo)

                 # Insert data into Actor table 
                query_actor = "INSERT INTO Actor (id,login,avatar_url,gravatar_id,url,followers_url,following_url,starred_url,subscriptions_url,organizations_url) \
                        VALUES (%s, '%s', %s, '%s', %s, '%s', '%s', %s, '%s', '%s')" \
                        % (each['payload']['issue']["user"]["id"], each['payload']['issue']["user"]["login"], each['payload']['issue']["user"]["avatar_url"], eacheach['payload']['issue']["user"]["gravatar_id"], each['payload']['issue']["user"]["url"], each['payload']['issue']["user"]["followers_url"],each['payload']['issue']["user"]["following_url"],each['payload']['issue']["user"]["starred_url"],each['payload']['issue']["user"]["subscriptions_url"],each['payload']['issue']["user"]["organizations_url"])
                session.execute(query_actor)

                # try: col_event = each["id"], each["type"], each["public"], each["created_at"], each["repo"]["id"],each["repo"]["name"],  each["actor"]["id"],  each["actor"]["name"],  each["payload"]["commit"]["sha"]
                # except:col_event = each["id"], each["type"], each["public"], each["created_at"], each["repo"]["id"],each["repo"]["name"],  each["actor"]["id"],  each["actor"]["name"],
                # sql_insert = table_insert_event % str(col_event)
                # #print(sql_insert)
                # cur.execute(sql_insert)
                # conn.commit()

# def insert_sample_data(session):
#     query = f"""
#     INSERT INTO events (id, type, public) VALUES ('23487929637', 'IssueCommentEvent', true)
#     """
#     session.execute(query)


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
    process(session, filepath="../data")
    #insert_sample_data(session)

    # Select data in Cassandra and print them to stdout
    query = """
    SELECT * from repo 
     """
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()