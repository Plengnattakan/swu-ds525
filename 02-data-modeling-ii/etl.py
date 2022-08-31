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
        display_login varchar,
        gravatar_id varchar,
        url varchar,
        avartar_url varchar,
        PRIMARY KEY ((id),login)
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
        login varchar,
        push_id bigint,
        PRIMARY KEY ((id),create_at)
        )
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
                #print(each["actor"]["avartar_url"])

                # Insert data into repo tables 
                query_repo = "INSERT INTO Repo (id,name,url) VALUES (%s, '%s', '%s')" \
                         % (each["repo"]["id"], each["repo"]["name"], each["repo"]["url"])
                session.execute(query_repo)
        
                 # Insert data into Actor table 
                
                try:
                    query_actor = "INSERT INTO Actor (id,login,display_login,gravatar_id,url,avartar_url) \
                        VALUES (%s, '%s', '%s', '%s', '%s', '%s', '%s')" \
                        % (each["actor"]["id"], each["actor"]["login"], each["actor"]["display_login"], each["actor"]["gravatar_id"], each["actor"]["url"],each["actor"]["avartar_url"])
                    session.execute(query_actor)
                except:
                    query_actor = "INSERT INTO Actor (id,login,display_login,gravatar_id,url) \
                        VALUES (%s, '%s', '%s', '%s', '%s')" \
                        % (each["actor"]["id"], each["actor"]["login"], each["actor"]["display_login"], each["actor"]["gravatar_id"], each["actor"]["url"])
                    session.execute(query_actor)    

                try:
                    if each["payload"]["push_id"] != None :
                        query_event = "INSERT INTO Event (id,type,public,create_at,repo_id,repo_name,actor_id,login,push_id) \
                            VALUES (%s, '%s', '%s', '%s', %s, '%s',%s, '%s', %s)" \
                            % (each["id"], each["type"], each["public"], each["created_at"], each["repo"]["id"],each["repo"]["name"],  each["actor"]["id"],  each["actor"]["login"],  each["payload"]["push_id"])
                        session.execute(query_event)
                        
                    else :
                        query_event = "INSERT INTO Event (id,type,public,create_at,repo_id,repo_name,actor_id,login) \
                            VALUES (%s, '%s', '%s', '%s', %s, '%s',%s, '%s')" \
                            % (each["id"], each["type"], each["public"], each["created_at"], each["repo"]["id"],each["repo"]["name"],  each["actor"]["id"],  each["actor"]["login"])
                        session.execute(query_event)
                        
                except:
                    pass


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
    query1 = """
    SELECT * from Repo 
     """
    query2 = """
    SELECT id,type,create_at,repo_id,repo_name,actor_id,login from Event WHERE public='True' ORDER BY created_at DESC
     """
    try:
        rows = session.execute(query2)
    except Exception as e:
        print(e)

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()