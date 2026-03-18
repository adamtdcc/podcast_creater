import logging
import logging.handlers
import os
import sqlite3
from enum import Enum

class ProcessStatus(Enum):
    CHECK_PODCAST_NEED_DOWNLOAD = 1 # 人工確認Podcast是否需要下載
    DONT_DOWNLOAD_PODCAST = 8 # 不下載此Podcast
    WAIT_DOWNLOAD_RESOURCE = 2 # 等待下載資源
    WAIT_UPLOAD_PODSCAST_SERVER = 3 # 等待上傳Podcast到伺服器
    UPLOAD_PODSCAST_OK = 4 # 上傳Podcast到伺服器完成
    WAIT_DEL_PODCAST_SERVER = 5 # 準備要刪除伺服器上的Podcast
    DEL_PODCAST_SERVER_OK = 6 # 伺服器已刪除完成Podcast

# 設定檔案儲存路徑
AUDIO_DIR = "downloads/audio"
THUMBNAIL_DIR = "downloads/thumbnails"
DB_PATH = "podcast.sqlite"

def get_all_channels(db_path: str = DB_PATH) -> list:
    """
    從資料庫取得所有啟用的頻道設定
    
    Args:
        db_path: 資料庫路徑
    
    Returns:
        頻道設定列表
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT channel_id, channel_name, process_status, format
        FROM channel
        WHERE is_active = 1
    ''')
    
    rows = cursor.fetchall()
    conn.close()
    
    return [
        {
            "channel_id": row[0],
            "channel_name": row[1],
            "process_status": row[2],
            "format": row[3]
        }
        for row in rows
    ]


def init_logger(logger_name, log_file_path="log/youtube_download2.log", console_level=logging.INFO, file_level=logging.INFO):
    """
    初始化 logger
    
    Args:
        logger_name: logger 名稱
        log_file_path: 日誌檔案路徑
        console_level: 控制台日誌等級
        file_level: 檔案日誌等級
    
    Returns:
        logger 實例
    """
    # 確保日誌目錄存在
    log_dir = os.path.dirname(log_file_path)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    
    # 避免重複添加 handler
    if logger.handlers:
        return logger
    
    # 檔案 handler
    rf_handler = logging.handlers.TimedRotatingFileHandler(
        filename=log_file_path,
        when='D',
        interval=1,
        backupCount=7,
        encoding='utf-8'
    )
    rf_handler.setLevel(file_level)
    rf_handler.setFormatter(logging.Formatter("%(asctime)s - %(filename)s[:%(lineno)d] - %(message)s"))
    
    # 控制台 handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    
    logger.addHandler(rf_handler)
    logger.addHandler(console_handler)
    
    return logger

# 設定檔案儲存路徑
AUDIO_DIR = "downloads/audio"
THUMBNAIL_DIR = "downloads/thumbnails"
DB_PATH = "podcast.sqlite"