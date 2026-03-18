import sqlite3
from util import DB_PATH

def init_database(db_name=DB_PATH):
    """初始化資料庫，建立 podcast 和 channel 資料表"""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS podcast (
    pk              INTEGER   PRIMARY KEY
                              NOT NULL           ,
    video_id        TEXT      NOT NULL           ,
    domain          TEXT      NOT NULL           ,
    channel_name    TEXT      NOT NULL           ,
    title_name      TEXT      NOT NULL           ,
    info            TEXT      NOT NULL DEFAULT '',
    created_at      TIMESTAMP NOT NULL           ,
    updated_at      TIMESTAMP NOT NULL           ,
    process_status  INTEGER   NOT NULL           ,
    format          TEXT      NOT NULL
        )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS channel (
    pk              INTEGER   PRIMARY KEY NOT NULL,
    domain          TEXT      NOT NULL DEFAULT 'youtube.com',
    channel_id      TEXT      NOT NULL UNIQUE,
    channel_name    TEXT      NOT NULL,
    process_status  INTEGER   NOT NULL,
    format          TEXT      NOT NULL DEFAULT 'mp3',
    is_active       INTEGER   NOT NULL DEFAULT 1
        )
    ''')

    cursor.execute('''
    CREATE VIEW IF NOT EXISTS view_111 AS
    SELECT title_name,
           format,
           process_status,
           pk
      FROM podcast
     WHERE process_status = 11;
    ''')

    cursor.execute('''
    CREATE UNIQUE INDEX IF NOT EXISTS idx_domain_video_id ON podcast(domain, video_id);
    ''')
    
    conn.commit()
    conn.close()
    print(f"資料庫 {db_name} 已初始化")

if __name__ == '__main__':
    init_database()