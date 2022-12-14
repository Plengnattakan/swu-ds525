#import Library
import psycopg2

#Drop table ใช้สำหรับล้าง Table เพื่อรันในครั้งต่อไป
table_drop_repo = "DROP TABLE IF EXISTS Repo"
table_drop_actor = "DROP TABLE IF EXISTS Actor"
table_drop_user = "DROP TABLE IF EXISTS UserT"
table_drop_comment = "DROP TABLE IF EXISTS Comment"
table_drop_payload = "DROP TABLE IF EXISTS Payload"
table_drop_event = "DROP TABLE IF EXISTS Event"



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
#กำหนดตัวแปรที่ต้องการดำเนินการ Create และ Drop
create_table_queries = [
    table_create_repo,table_create_actor,table_create_user,table_create_comment,table_create_payload,table_create_event
]
drop_table_queries = [
    table_drop_event,table_drop_payload,table_drop_repo,table_drop_actor,table_drop_comment,table_drop_user
]


def drop_tables(cur , conn ) -> None:
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur , conn) -> None:
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database.
    - Establishes connection with the sparkify database and gets
    cursor to it.
    - Drops all the tables.
    - Creates all tables needed.
    - Finally, closes the connection.
    """
    #เชื่อมต่อ postgresql
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()