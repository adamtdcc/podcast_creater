import sqlite3
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import json
import time
import re
from util import DB_PATH, get_all_channels, init_logger


logger = init_logger(__name__)

def parse_relative_time(relative_time_str):
    """
    將 YouTube 的相對時間轉換為標準時間格式
    
    參數:
        relative_time_str: 相對時間字串 (例如: "2 天前", "1 週前", "3 hours ago")
    
    返回:
        標準時間格式字串 (例如: "2026-01-07 15:38:47")
    """
    if not relative_time_str:
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    now = datetime.now()
    
    # 定義時間單位的對應（支援中英文）
    time_patterns = {
        r'(\d+)\s*(秒|second|seconds?)': lambda x: timedelta(seconds=int(x)),
        r'(\d+)\s*(分鐘|minute|minutes?)': lambda x: timedelta(minutes=int(x)),
        r'(\d+)\s*(小時|hour|hours?)': lambda x: timedelta(hours=int(x)),
        r'(\d+)\s*(天|day|days?)': lambda x: timedelta(days=int(x)),
        r'(\d+)\s*(週|week|weeks?)': lambda x: timedelta(weeks=int(x)),
        r'(\d+)\s*(個月|month|months?)': lambda x: timedelta(days=int(x) * 30),
        r'(\d+)\s*(年|year|years?)': lambda x: timedelta(days=int(x) * 365),
    }
    
    for pattern, delta_func in time_patterns.items():
        match = re.search(pattern, relative_time_str, re.IGNORECASE)
        if match:
            number = match.group(1)
            published_time = now - delta_func(number)
            return published_time.strftime('%Y-%m-%d %H:%M:%S')
    
    # 如果無法解析，返回當前時間
    return now.strftime('%Y-%m-%d %H:%M:%S')

def get_channel_videos(channel_id, limit=10):
    """
    爬取 YouTube 頻道的影片資訊
    
    參數:
        channel_url: YouTube 頻道 URL (例如: https://www.youtube.com/@channelname)
        limit: 要取得的影片數量 (預設 10)
    
    返回:
        包含影片標題和 URL 的列表
    """
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    channel_url = f'https://www.youtube.com/{channel_id}/videos'
    try:
        response = requests.get(channel_url, headers=headers)
        response.raise_for_status()
        
        # 從頁面中提取 ytInitialData
        soup = BeautifulSoup(response.text, 'html.parser')
        scripts = soup.find_all('script')
        
        video_data = []
        
        for script in scripts:
            if 'var ytInitialData' in script.text:
                # 提取 JSON 資料
                json_text = script.text
                start = json_text.find('{')
                end = json_text.rfind('}') + 1
                json_data = json.loads(json_text[start:end])
                
                # 導航到影片列表
                try:
                    tabs = json_data['contents']['twoColumnBrowseResultsRenderer']['tabs']
                    
                    for tab in tabs:
                        if 'tabRenderer' in tab:
                            if tab['tabRenderer'].get('selected', False):
                                contents = tab['tabRenderer']['content']['richGridRenderer']['contents']
                                
                                for item in contents:
                                    if 'richItemRenderer' in item:
                                        video_renderer = item['richItemRenderer']['content']['videoRenderer']
                                        
                                        video_id = video_renderer['videoId']
                                        title = video_renderer['title']['runs'][0]['text']
                                        
                                        # 取得影片發佈時間並轉換為標準格式
                                        published_time_text = video_renderer.get('publishedTimeText', {}).get('simpleText', '')
                                        published_time = parse_relative_time(published_time_text)
                                        
                                        video_data.append({
                                            'title': title,
                                            'video_id': video_id,
                                            'published_time': published_time
                                        })
                                        
                                        if len(video_data) >= limit:
                                            break
                                    
                                    if len(video_data) >= limit:
                                        break
                                break
                except KeyError as e:
                    print(f"解析資料時出錯: {e}")
                    print("頁面結構可能已改變")
                
                break
        
        return video_data[:limit]
        
    except requests.RequestException as e:
        print(f"請求錯誤: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"JSON 解析錯誤: {e}")
        return []

def insert_videos_to_db(channel_config: dict, videos: list, db_path: str = DB_PATH):
    """
    將影片資訊寫入資料庫
    
    Args:
        channel_config: 頻道設定
        videos: 影片列表
        db_path: 資料庫路徑
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    inserted_count = 0
    
    for video in videos:
        try:
            # 使用影片的發佈時間作為 created_at，如果沒有則使用當前時間
            created_at = video.get('published_time', now)
            
            cursor.execute('''
                INSERT OR IGNORE INTO podcast 
                (video_id, domain, channel_name, title_name, 
                 created_at, updated_at, process_status, format)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                video['video_id'],
                'youtube.com',
                channel_config['channel_name'],
                video['title'],
                created_at,
                now,
                channel_config['process_status'],
                channel_config['format']
            ))
            if cursor.rowcount > 0:
                inserted_count += 1
                logger.info(f"影片儲存: {video['title']}")
            else:
                logger.info(f"影片已存在，跳過: {video['title']}")
        except sqlite3.Error as e:
            logger.error(f"插入影片 {video['video_id']} 失敗: {e}")
    
    conn.commit()
    conn.close()
    logger.info(f"頻道 {channel_config['channel_name']} 新增 {inserted_count} 筆資料")

def main():
    """主程式：爬取所有頻道的影片"""
    channels = get_all_channels()
    
    if not channels:
        logger.warning("沒有找到任何頻道設定，請先初始化頻道資料")
        return
    
    for i, channel in enumerate(channels):
        logger.info(f"開始處理頻道: {channel['channel_name']}")
        
        # 爬取頻道前 10 筆影片
        videos = get_channel_videos(channel['channel_id'], limit=2)
        
        
        if videos:
            # 寫入資料庫
            insert_videos_to_db(channel, videos)
        else:
            logger.warning(f"頻道 {channel['channel_name']} 沒有取得任何影片")
        
        # 如果不是最後一個頻道，等待 10 秒
        if i < len(channels) - 1:
            time.sleep(5)

if __name__ == '__main__':
    main()