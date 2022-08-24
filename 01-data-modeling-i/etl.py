import glob
import json
import os
from typing import List

import psycopg2


table_insert_repo = """
    INSERT INTO Repo (repo_id,name,url) VALUES (%s)
    ON CONFLICT (id,name,url) DO NOTHING
"""
table_insert_actor = """
    INSERT INTO Actor (
        actor_id,login,display_login,gravatar_id,url,avartar_url
    ) VALUES (%s)
    ON CONFLICT (id,login,display_login,gravatar_id,url,avartar_url) DO NOTHING
"""
table_insert_user = """
    INSERT INTO UserT (
        user_id,login
    ) VALUES (%s)
    ON CONFLICT (id,login) DO NOTHING
"""
table_insert_comment = """
    INSERT INTO Comment (
        comment_id,url,html_url,issue_url,node_id,user_id,created_at,updated_at,author_association,body,performed_via_github_app
    ) VALUES (%s)
    ON CONFLICT (id,url,html_url,issue_url,node_id,user_id,created_at,updated_at,author_association,body,performed_via_github_app) DO NOTHING
"""
table_insert_event  = """
    INSERT INTO Event (
        event_id,type,public,create_at,repo_id,actor_id,comment_id
    ) VALUES (%s)
    ON CONFLICT (id,type,public,create_at,repo_id,actor_id,comment_id) DO NOTHING
"""



def get_files(filepath: str) -> List[str]:
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "github_events_01.json"))
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
                #print(each["id"], each["type"], each["actor"]["login"])

                # Insert data into repo tables 
                col_repo = each["repo"]["id"], each["repo"]["name"], each["repo"]["url"]
                sql_insert = table_insert_repo % str(col_repo)
                cur.execute(sql_insert)
                conn.commit()

                # Insert data into Actor tables 
                col_actor = each["actor"]["id"], each["actor"]["login"], each["actor"]["display_login"], each["actor"]["gravatar_id"], each["actor"]["url"], each["actor"]["avatar_url"]
                sql_insert = table_insert_actor % str(col_actor)
                cur.execute(sql_insert)
                conn.commit()

                # Insert data into user tables 
                col_user = each["user"]["id"], each["user"]["login"]
                sql_insert = table_insert_user % str(col_user)
                cur.execute(sql_insert)
                conn.commit()

                # Insert data into comment tables 
                col_comment = each["comment"]["id"],each["comment"]["url"],each["comment"]["html_url"],each["comment"]["issue_url"],each["comment"]["node_id"],each["comment"]["created_at"],each["comment"]["updated_at"],each["comment"]["author_association"],each["comment"]["body"],each["comment"]["performed_via_github_app"], each["user"]["id"]
                sql_insert = table_insert_comment % str(col_comment)
                cur.execute(sql_insert)
                conn.commit()

                # Insert data into event tables 
                col_event = each["event"]["id"], each["event"]["type"], each["event"]["public"], each["event"]["created_at"], each["repo"]["id"], each["actor"]["id"], each["comment"]["id"]
                sql_insert = table_insert_event % str(col_event)
                cur.execute(sql_insert)
                conn.commit()


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    process(cur, conn, filepath="../data")

    conn.close()


if __name__ == "__main__":
    main()
    