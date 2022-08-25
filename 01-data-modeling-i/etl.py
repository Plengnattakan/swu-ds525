#import Library
import glob
import json
import os
from typing import List

import psycopg2



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


def process(cur, conn, filepath):
    # Get list of files from filepath
    all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data

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


def main():
    conn = psycopg2.connect(
        #เชื่อมต่อ Postgresql
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    process(cur, conn, filepath="../data")

    conn.close()


if __name__ == "__main__":
    main()
    