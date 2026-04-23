import sqlite3
import os
import boto3
import urllib.request
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import quote
import uuid
from dotenv import load_dotenv
from util import init_logger, AUDIO_DIR, THUMBNAIL_DIR, DB_PATH, ProcessStatus

load_dotenv(Path(__file__).parent / '.env')
logger = init_logger("youtube_download2")

# 設定
RSS_OUTPUT = "it_simple.xml"

# Cloudflare R2 設定 (從 .env 讀取)
R2_ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
R2_ACCESS_KEY_ID = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET_ACCESS_KEY = os.environ["R2_SECRET_ACCESS_KEY"]
R2_BUCKET_NAME = os.environ["R2_BUCKET_NAME"]
R2_PUBLIC_URL = os.environ["R2_PUBLIC_URL"]
HC_PING_URL = os.environ["HC_PING_URL"]

def get_r2_client():
    """建立 R2 S3 客戶端"""
    return boto3.client(
        's3',
        endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name='auto'
    )

def upload_file_to_r2(client, local_path, r2_key, content_type):
    """上傳檔案到 R2"""
    try:
        client.upload_file(
            local_path,
            R2_BUCKET_NAME,
            r2_key,
            ExtraArgs={'ContentType': content_type}
        )
        logger.info(f"✓ 上傳成功: {r2_key}")
        return True
    except Exception as e:
        logger.error(f"上傳失敗 {local_path}: {e}")
        return False

def delete_file_from_r2(client, r2_key):
    """從 R2 刪除檔案"""
    try:
        client.delete_object(Bucket=R2_BUCKET_NAME, Key=r2_key)
        logger.info(f"✓ 刪除成功: {r2_key}")
        return True
    except Exception as e:
        logger.error(f"刪除失敗 {r2_key}: {e}")
        return False

def get_file_size(file_path):
    """取得檔案大小"""
    try:
        return os.path.getsize(file_path)
    except:
        return 0

def get_audio_duration(file_path):
    """取得音訊長度（秒）- 簡化版本，回傳預設值"""
    try:
        from mutagen.mp3 import MP3
        audio = MP3(file_path)
        return int(audio.info.length)
    except:
        return 0

def get_videos_by_status(conn, status):
    """取得指定狀態的影片"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT pk, domain, video_id, title_name, info, created_at
        FROM podcast 
        WHERE process_status = ?
    """, (status.value,))
    return cursor.fetchall()

def get_expired_videos(conn, days=7):
    """取得 updated_at 超過指定天數且狀態為 UPLOAD_PODSCAST_OK 的影片"""
    cursor = conn.cursor()
    expire_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("""
        SELECT pk, domain, video_id, title_name
        FROM podcast 
        WHERE process_status = ? AND updated_at < ?
    """, (ProcessStatus.UPLOAD_PODSCAST_OK.value, expire_date))
    return cursor.fetchall()

def update_status(conn, pk, new_status):
    """更新影片狀態，同時更新 updated_at"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE podcast 
            SET process_status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE pk = ?
        """, (new_status.value, pk))
        conn.commit()
        return True
    except sqlite3.Error as e:
        logger.error(f"更新狀態失敗: {e}")
        return False

def find_thumbnail_file(video_id):
    """尋找對應的縮圖檔案"""
    for ext in ['jpg', 'png']:
        path = os.path.join(THUMBNAIL_DIR, f"{video_id}.{ext}")
        if os.path.exists(path):
            return path
    return None

def upload_pending_files():
    """上傳 process_status=WAIT_UPLOAD_PODSCAST_SERVER 的檔案到 R2"""
    conn = sqlite3.connect(DB_PATH)
    client = get_r2_client()
    uploaded_mp3_count = 0
    
    try:
        videos = get_videos_by_status(conn, ProcessStatus.WAIT_UPLOAD_PODSCAST_SERVER)
        logger.info(f"找到 {len(videos)} 個待上傳的影片")
        
        for pk, domain, video_id, title, info, created_at in videos:
            logger.info(f"\n處理上傳: {title} (PK: {pk})")
            
            # 尋找音訊檔案
            audio_path = os.path.join(AUDIO_DIR, f"{video_id}.mp3")
            if not os.path.exists(audio_path):
                logger.warning(f"找不到音訊檔案: {audio_path}")
                continue
            
            # 上傳音訊
            audio_r2_key = f"{quote(video_id, safe='')}.mp3"
            if not upload_file_to_r2(client, audio_path, audio_r2_key, 'audio/mpeg'):
                continue
            
            uploaded_mp3_count += 1
            
            # 尋找並上傳縮圖
            thumbnail_path = find_thumbnail_file(video_id)
            if thumbnail_path:
                ext = Path(thumbnail_path).suffix
                thumbnail_r2_key = f"thumbnails/{video_id}{ext}"
                upload_file_to_r2(client, thumbnail_path, thumbnail_r2_key, f'image/{ext[1:]}')
            
            # 更新資料庫狀態
            if update_status(conn, pk, ProcessStatus.UPLOAD_PODSCAST_OK):
                logger.info(f"✓ 上傳完成並更新狀態: PK {pk}")
        
        # 上傳超過 5 個 mp3 時發送健康檢查 ping
        if uploaded_mp3_count > 5:
            logger.info(f"上傳了 {uploaded_mp3_count} 個 mp3 檔案（超過 5 個），發送健康檢查 ping")
            try:
                urllib.request.urlopen(HC_PING_URL, timeout=10)
            except Exception as e:
                logger.error(f"健康檢查 ping 發送失敗: {e}")
            
    finally:
        conn.close()

def mark_expired_for_deletion():
    """將超過 7 天的 UPLOAD_PODSCAST_OK 標記為 WAIT_DEL_PODCAST_SERVER"""
    conn = sqlite3.connect(DB_PATH)
    
    try:
        expired_videos = get_expired_videos(conn, days=7)
        logger.info(f"找到 {len(expired_videos)} 個超過 7 天的影片需標記刪除")
        
        for pk, domain, video_id, title in expired_videos:
            if update_status(conn, pk, ProcessStatus.WAIT_DEL_PODCAST_SERVER):
                logger.info(f"✓ 已標記刪除: {title} (PK: {pk})")
            
    finally:
        conn.close()

def delete_expired_files():
    """刪除 R2 上狀態為 WAIT_DEL_PODCAST_SERVER 的檔案"""
    conn = sqlite3.connect(DB_PATH)
    client = get_r2_client()
    
    try:
        videos = get_videos_by_status(conn, ProcessStatus.WAIT_DEL_PODCAST_SERVER)
        logger.info(f"找到 {len(videos)} 個待刪除的影片")
        
        for pk, domain, video_id, title, info, created_at in videos:
            logger.info(f"\n處理刪除: {title} (PK: {pk})")
            
            # 刪除 R2 上的音訊檔案
            audio_r2_key = f"{quote(video_id, safe='')}.mp3"
            delete_file_from_r2(client, audio_r2_key)
            
            # 刪除 R2 上的縮圖
            for ext in ['jpg', 'png']:
                thumbnail_r2_key = f"thumbnails/{video_id}.{ext}"
                delete_file_from_r2(client, thumbnail_r2_key)
            
            # 刪除本地檔案
            audio_path = os.path.join(AUDIO_DIR, f"{video_id}.mp3")
            if os.path.exists(audio_path):
                os.remove(audio_path)
                logger.info(f"✓ 已刪除本地音訊: {audio_path}")
            
            thumbnail_path = find_thumbnail_file(video_id)
            if thumbnail_path and os.path.exists(thumbnail_path):
                os.remove(thumbnail_path)
                logger.info(f"✓ 已刪除本地縮圖: {thumbnail_path}")
            
            # 更新資料庫狀態
            if update_status(conn, pk, ProcessStatus.DEL_PODCAST_SERVER_OK):
                logger.info(f"✓ 刪除完成並更新狀態: PK {pk}")
            
    finally:
        conn.close()

def get_deterministic_uuid(resource_id: str, namespace: uuid.UUID = uuid.NAMESPACE_DNS) -> str:
    return str(uuid.uuid5(namespace, resource_id)).upper()

def youtube_video_info(video_id):
    """取得 YouTube 影片的相關資訊"""
    # 取得音訊檔案資訊
    audio_path = os.path.join(AUDIO_DIR, f"{video_id}.mp3")
    file_size = get_file_size(audio_path)
    duration = get_audio_duration(audio_path)
    
    # 編碼 URL
    encoded_filename = quote(f"{video_id}.mp3", safe='')
    audio_url = f"{R2_PUBLIC_URL}/{encoded_filename}"
    
    # 取得縮圖 URL
    thumbnail_path = find_thumbnail_file(video_id)
    thumbnail_url = ""
    if thumbnail_path:
        ext = Path(thumbnail_path).suffix
        thumbnail_r2_key = f"thumbnails/{video_id}{ext}"
        thumbnail_url = f"{R2_PUBLIC_URL}/{quote(thumbnail_r2_key, safe='/')}"
    
    # 生成 YouTube 影片連結
    org_link = f"https://www.youtube.com/watch?v={video_id}"

    return org_link, thumbnail_url, audio_url, file_size, duration

def redirect_video_info(video_id):
    """取得 redirect 影片的相關資訊，這裡假設 info 欄位存有原始連結"""
    audio_path = os.path.join(AUDIO_DIR, f"{video_id}.mp3")
    file_size = get_file_size(audio_path)
    duration = get_audio_duration(audio_path)
    
    audio_url = f"{R2_PUBLIC_URL}/{video_id}.mp3"
    thumbnail_url = ""
    org_link = ""
    return org_link, thumbnail_url, audio_url, file_size, duration

def generate_rss():
    """生成 Apple Podcast RSS 格式的 XML"""
    conn = sqlite3.connect(DB_PATH)
    
    try:
        videos = get_videos_by_status(conn, ProcessStatus.UPLOAD_PODSCAST_OK)
        logger.info(f"找到 {len(videos)} 個影片用於生成 RSS")
        
        if not videos:
            logger.info("沒有影片可生成 RSS")
            return None
        
        # 建立 RSS 結構
        rss_content = '''<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd" xmlns:content="http://purl.org/rss/1.0/modules/content/">
  <channel>
    <title>adam待看清單</title>
    <language>zh-tw</language>
    <itunes:author>adam</itunes:author>
    <description>adam待看清單</description>
    <itunes:type>Episodic</itunes:type>
    <itunes:image href="https://pub-ff51abbec527423e8f58e468ea714bf2.r2.dev/it_simple.jpg"/>
    <itunes:explicit>false</itunes:explicit>
'''
        
        for pk, domain, video_id, title, info, created_at in videos:
            org_link,thumbnail_url,audio_url,file_size,duration = "", "", "", 0, 0
            if domain == "youtube.com":
                org_link,thumbnail_url,audio_url,file_size,duration = youtube_video_info(video_id)
            elif domain == "bilibili.com":
                pass
            elif domain == "redirect":
                org_link,thumbnail_url,audio_url,file_size,duration = redirect_video_info(video_id)
            
            # 格式化日期
            try:
                dt = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
                pub_date = dt.strftime('%a, %d %b %Y %H:%M:%S +0800')
            except:
                pub_date = datetime.now().strftime('%a, %d %b %Y %H:%M:%S +0800')
            
            # 生成 GUID
            guid = get_deterministic_uuid(video_id)
            
            # 處理描述
            description = info if info else title
            description = description.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            if org_link:
                description = f"{org_link}\n{description}"

            # 處理標題中的特殊字元
            safe_title = title.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            
            item = f'''    <item>
      <title>{safe_title}</title>
      <link>{org_link}</link>
      <description><![CDATA[{description}]]></description>
      <enclosure length="{file_size}" type="audio/mpeg" url="{audio_url}"/>
      <guid>{guid}</guid>
      <pubDate>{pub_date}</pubDate>
      <itunes:duration>{duration}</itunes:duration>
      <itunes:explicit>false</itunes:explicit>'''
            
            # 如果有縮圖，加入 itunes:image
            if thumbnail_url:
                item += f'''
      <itunes:image href="{thumbnail_url}"/>'''
            
            item += '''
    </item>
'''
            rss_content += item
        
        rss_content += '''  </channel>
</rss>'''
        
        # 寫入檔案
        with open(RSS_OUTPUT, 'w', encoding='utf-8') as f:
            f.write(rss_content)
        
        logger.info(f"✓ RSS 已生成: {RSS_OUTPUT}")
        return RSS_OUTPUT
        
    finally:
        conn.close()

def upload_rss_to_r2():
    """上傳 RSS 檔案到 R2"""
    if not os.path.exists(RSS_OUTPUT):
        logger.error(f"RSS 檔案不存在: {RSS_OUTPUT}")
        return False
    
    client = get_r2_client()
    return upload_file_to_r2(client, RSS_OUTPUT, RSS_OUTPUT, 'application/xml')

def main():
    logger.info("=" * 60)
    logger.info("開始處理上傳和 RSS 生成")
    logger.info("=" * 60)
    
    # 步驟 1: 標記過期影片為待刪除
    logger.info("\n[步驟 1] 標記超過 7 天的影片為待刪除...")
    mark_expired_for_deletion()
    
    # 步驟 2: 刪除待刪除的檔案
    logger.info("\n[步驟 2] 刪除 R2 和本地的過期檔案...")
    delete_expired_files()

    # 步驟 3: 上傳待處理的檔案
    logger.info("\n[步驟 3] 上傳檔案到 R2...")
    upload_pending_files()
    
    # 步驟 4: 生成 RSS
    logger.info("\n[步驟 4] 生成 RSS...")
    rss_file = generate_rss()
    
    # 步驟 5: 上傳 RSS 到 R2
    if rss_file:
        logger.info("\n[步驟 5] 上傳 RSS 到 R2...")
        upload_rss_to_r2()
    
    logger.info("\n全部處理完成！")

if __name__ == "__main__":
    # 檢查 boto3 是否安裝
    try:
        import boto3
    except ImportError:
        logger.error("請先安裝 boto3: pip install boto3")
        exit(1)
    
    main()