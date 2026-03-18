import sqlite3
import subprocess
import json
import os
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from util import init_logger, AUDIO_DIR, THUMBNAIL_DIR, DB_PATH, ProcessStatus

logger = init_logger("youtube_download2")

# 確保目錄存在
Path(AUDIO_DIR).mkdir(parents=True, exist_ok=True)
Path(THUMBNAIL_DIR).mkdir(parents=True, exist_ok=True)

def extract_video_id(url):
    """從 URL 中提取 video_id"""
    parsed = urlparse(url)
    if parsed.hostname in ['www.youtube.com', 'youtube.com']:
        if parsed.path == '/watch':
            return parse_qs(parsed.query).get('v', [None])[0]
        elif parsed.path.startswith('/embed/'):
            return parsed.path.split('/')[2]
        elif parsed.path.startswith('/v/'):
            return parsed.path.split('/')[2]
    elif parsed.hostname == 'youtu.be':
        return parsed.path[1:]
    return None

def extract_domain(url):
    """從 URL 中提取 domain"""
    parsed = urlparse(url)
    return parsed.hostname

def get_pending_videos(conn):
    """取得 process_status = WAIT_DOWNLOAD_RESOURCE 的影片"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT pk, domain, video_id, title_name 
        FROM podcast 
        WHERE process_status = ? AND domain = 'youtube.com'
    """, (ProcessStatus.WAIT_DOWNLOAD_RESOURCE.value,))
    return cursor.fetchall()

def build_url(domain, video_id):
    """根據 domain 和 video_id 建構完整的 URL"""
    if domain in ['www.youtube.com', 'youtube.com']:
        return f"https://{domain}/watch?v={video_id}"
    elif domain == 'youtu.be':
        return f"https://{domain}/{video_id}"
    return f"https://{domain}/{video_id}"

def download_video_info(url):
    """使用 youtube-dlp 取得影片資訊"""
    try:
        cmd = [
            'yt-dlp',
            '--dump-json',
            '--no-download',
            url
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        logger.error(f"取得影片資訊失敗: {e}")
        return None

def download_audio(url, output_path):
    """下載音訊檔案"""
    try:
        cmd = [
            'yt-dlp',
            '-x',  # 只下載音訊
            '--audio-format', 'mp3',  # 轉換為 mp3
            '--audio-quality', '192K',
            '-o', output_path,
            '-f', 'ba[language=zh]/ba',
            '--embed-metadata',
            url
        ]
        subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"下載音訊失敗: {e}")
        return False

def download_thumbnail(url, output_path):
    """下載縮圖"""
    try:
        cmd = [
            'yt-dlp',
            '--write-thumbnail',
            '--skip-download',
            '--convert-thumbnails', 'jpg',
            '-o', output_path,
            url
        ]
        subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"下載縮圖失敗: {e}")
        return False

def check_and_update_ytdlp():
    """檢查並更新 yt-dlp"""
    try:
        logger.info("檢查 yt-dlp 更新...")
        
        # 使用 yt-dlp 內建的更新功能
        result = subprocess.run(['yt-dlp', '--update'], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            output = result.stdout.strip()
            if "yt-dlp is up to date" in output or "已是最新版本" in output:
                logger.info("yt-dlp 已是最新版本")
            elif "Updated yt-dlp" in output or "已更新" in output:
                logger.info("yt-dlp 已成功更新")
            else:
                logger.info(f"yt-dlp 更新結果: {output}")
                
    except Exception as e:
        logger.error(f"檢查更新時發生錯誤: {e}")

def update_database(conn, pk, description):
    """更新資料庫的 info 和 process_status，同時更新 updated_at"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE podcast 
            SET info = ?, process_status = ?, updated_at = CURRENT_TIMESTAMP 
            WHERE pk = ?
        """, (description, ProcessStatus.WAIT_UPLOAD_PODSCAST_SERVER.value, pk))
        conn.commit()
        return True
    except sqlite3.Error as e:
        logger.error(f"更新資料庫失敗: {e}")
        return False

def process_videos():
    """主要處理流程"""
    conn = sqlite3.connect(DB_PATH)
    
    try:
        videos = get_pending_videos(conn)
        logger.info(f"找到 {len(videos)} 個待處理的影片")
        
        for pk, domain, video_id, title in videos:
            logger.info(f"\n處理: {title} (PK: {pk})")
            
            # 建構完整的 URL
            url = build_url(domain, video_id)
            
            # 取得影片資訊
            info = download_video_info(url)
            if not info:
                logger.error(f"跳過 PK {pk}: 無法取得影片資訊")
                continue
            
            description = info.get('description', '')
            
            # 設定檔案名稱（使用 video_id）
            audio_file = os.path.join(AUDIO_DIR, f"{video_id}.mp3")
            thumbnail_file = os.path.join(THUMBNAIL_DIR, f"{video_id}")
            
            # 下載音訊
            logger.info(f"下載音訊...")
            if not download_audio(url, audio_file):
                logger.error(f"跳過 PK {pk}: 音訊下載失敗")
                continue
            
            # 下載縮圖
            logger.info(f"下載縮圖...")
            download_thumbnail(url, thumbnail_file)
            
            # 更新資料庫
            logger.info(f"更新資料庫...")
            if update_database(conn, pk, description):
                logger.info(f"✓ 完成處理 PK {pk}")
            else:
                logger.error(f"✗ 資料庫更新失敗 PK {pk}")
        
    finally:
        conn.close()
    
    logger.info("\n全部處理完成！")

if __name__ == "__main__":
    # 檢查 yt-dlp 是否安裝
    try:
        subprocess.run(['yt-dlp', '--version'], 
                      capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.error("錯誤: 請先安裝 yt-dlp")
        exit(1)
    
    # 檢查並更新 yt-dlp
    check_and_update_ytdlp()
    
    process_videos()