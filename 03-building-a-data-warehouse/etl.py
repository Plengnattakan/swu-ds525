import psycopg2

#Drop table ใช้สำหรับล้าง Table เพื่อรันในครั้งต่อไป
drop_table_queries = [
    "DROP TABLE IF EXISTS staging_events",
    "DROP TABLE IF EXISTS Repo",
    "DROP TABLE IF EXISTS Actor",
    "DROP TABLE IF EXISTS Event",
]



#สร้างตัวแปรเก็บ sql สร้างตาราง

create_table_queries = [
    """
    CREATE TABLE IF NOT EXISTS staging_events (
        id varchar,
        type varchar,
        actor_id bigint,
        actor_login varchar,
        actor_url varchar,
        repo_id bigint,
        repo_name varchar,
        repo_url varchar,
        public varchar,
        created_at varchar,
        actor_display_login varchar,
        actor_gravatar_id varchar,
        actor_url varchar,
        push_id varchar,
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS Event (
        id bigint,
        type varchar ,
        public varchar,
        create_at timestamp,
        repo_id bigint,
        repo_name varchar,
        actor_id bigint,
        actor_login varchar,
        push_id bigint

        )
    """,
    """
    CREATE TABLE IF NOT EXISTS Actor (
        id bigint,
        login varchar,
        display_login varchar,
        gravatar_id varchar,
        url varchar
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS Repo (
        id  bigint,
        name varchar,
        url varchar
        )
    """,
]


copy_table_queries = [
    """
    COPY staging_events FROM 's3://pleng/github_events_01.json'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::283010062450:role/LabRole'
    JSON 's3://pleng/events_json_path.json'
    REGION 'us-east-1'
    """,
]
insert_table_queries = [
"""
    INSERT INTO Repo (id,name,url) 
    SELECT DISTINCT repo_id, repo_name, repo_url
    FROM staging_events
    WHERE id NOT IN (SELECT DISTINCT id FROM Repo)
    """,
    """
    INSERT INTO Actor (id,login,display_login,gravatar_id,actor_url)
    SELECT DISTINCT actor_id,actor_login, actor_display_login,actor_gravatar_id, actor_url
    FROM staging_events
    WHERE actor_id NOT IN (SELECT DISTINCT id FROM Actor)
    """,
    """
    INSERT INTO Event (id,type,public,create_at,repo_id,repo_name,actor_id,actor_login,push_id)
    SELECT DISTINCT id, type, public,created_at,repo_id,repo_name,actor_id,actor_login,push_id
    FROM staging_events
    WHERE id NOT IN (SELECT DISTINCT id FROM Event)
    """,
]

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    host = "redshift-cluster-1.ci0boaeqvdep.us-east-1.redshift.amazonaws.com"
    dbname = "dev"
    user = "awsuser"
    password = "Pleng056720990"
    port = "5439"
    conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # query data
    query = "select * from event"
    cur.execute(query)
    # print data
    records = cur.fetchall()
    for row in records:
        print(row)
    conn.close()


if __name__ == "__main__":
    main()