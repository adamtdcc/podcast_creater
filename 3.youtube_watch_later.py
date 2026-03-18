from playwright.sync_api import sync_playwright
import time
import sqlite3
from datetime import datetime
from urllib.parse import urlparse, parse_qs
from util import init_logger, ProcessStatus, DB_PATH

logger = init_logger("youtube_download2")

def save_to_database(data, db_name=DB_PATH):
    """保存影片資料到 SQLite 資料庫"""
    if not data:
        logger.info("沒有資料需要保存")
        return

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    saved_count = 0
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for video in data:
        try:
            cursor.execute('''
                INSERT INTO podcast (domain, video_id, title_name, channel_name, created_at, updated_at, process_status, format)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                video['domain'],
                video['video_id'],
                video['title'],
                video['channel_name'],
                video['timestamp'],
                current_time,
                ProcessStatus.WAIT_DOWNLOAD_RESOURCE.value,
                'mp3'
            ))
            saved_count += 1
        except sqlite3.IntegrityError as e:
            logger.info(f"影片已存在，跳過: {video['title']}")
        except Exception as e:
            logger.error(f"保存影片時發生錯誤: {e}")

    conn.commit()
    conn.close()

    logger.info(f"已保存 {saved_count} 筆新資料到資料庫")


def update_process_status(video_id, new_status, db_name=DB_PATH):
    """更新影片處理狀態並同時更新 updated_at"""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        cursor.execute('''
            UPDATE podcast 
            SET process_status = ?, updated_at = ?
            WHERE video_id = ?
        ''', (new_status, current_time, video_id))
        conn.commit()
        logger.info(f"已更新 video_id: {video_id} 的狀態為 {new_status}")
    except Exception as e:
        logger.error(f"更新狀態時發生錯誤: {e}")
    finally:
        conn.close()


def extract_youtube_info(url):
    """
    從 YouTube URL 中提取 domain 和 video_id
    回傳: (domain, video_id) 或 (None, None)
    """
    parsed = urlparse(url)
    params = parse_qs(parsed.query)

    domain = "youtube.com"
    
    if 'v' in params:
        return domain, params['v'][0]
    return None, None


def get_watch_later_videos(page, limit=None):
    """獲取稍後觀看清單的影片"""
    logger.info("正在獲取稍後觀看清單...")

    # 前往稍後觀看頁面
    page.goto('https://www.youtube.com/playlist?list=WL')
    page.wait_for_timeout(3000)  # 等待頁面載入

    # 滾動頁面以載入所有影片
    for _ in range(3):
        page.evaluate('window.scrollTo(0, document.documentElement.scrollHeight)')
        page.wait_for_timeout(1000)

    # 獲取所有影片元素
    videos = []
    video_elements = page.locator('ytd-playlist-video-renderer').all()

    logger.info(f"找到 {len(video_elements)} 個影片")

    for element in video_elements:
        # 如果設定了上限且已達到上限，則停止
        if limit and len(videos) >= limit:
            break
        try:
            # 獲取標題
            title_element = element.locator('#video-title')
            title = title_element.get_attribute('title')

            # 獲取 URL
            url = title_element.get_attribute('href')
            if url and url.startswith('/watch'):
                url = f"https://www.youtube.com{url}"

            # 獲取頻道名稱
            channel_element = element.locator('ytd-channel-name a, #channel-name a, .ytd-channel-name a').first
            channel_name = channel_element.inner_text() if channel_element.count() > 0 else None
            
            # 備用方式取得頻道名稱
            if not channel_name:
                channel_element_alt = element.locator('#channel-name #text, #channel-name .yt-formatted-string').first
                channel_name = channel_element_alt.inner_text() if channel_element_alt.count() > 0 else "Unknown"

            if title and url:
                domain, video_id = extract_youtube_info(url)
                if domain and video_id:
                    videos.append({
                        'title': title,
                        'domain': domain,
                        'video_id': video_id,
                        'channel_name': channel_name.strip() if channel_name else "Unknown",
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                    logger.info(f"✓ {title} - 頻道: {channel_name}")

        except Exception as e:
            logger.error(f"獲取影片資訊時發生錯誤: {e}")
            continue

    return videos


def remove_videos_from_watch_later(page, count):
    """從稍後觀看清單中刪除指定數量的影片"""
    logger.info(f"\n開始從稍後觀看清單中刪除 {count} 個影片...")

    page.goto('https://www.youtube.com/playlist?list=WL')
    page.wait_for_timeout(3000)

    removed_count = 0

    for i in range(count):
        try:
            # 找到第一個影片的選單按鈕
            video_element = page.locator('ytd-playlist-video-renderer').first

            # 點擊更多選項按鈕
            menu_button = video_element.locator('button[aria-label*="動作選單"]').or_(
                video_element.locator('button#button[aria-label="更多操作"]')
            ).or_(
                video_element.locator('yt-icon-button#button')
            ).first

            menu_button.click()
            page.wait_for_timeout(500)

            # 點擊「從『稍後觀看』中移除」選項
            remove_option = page.locator('text="從「稍後觀看」中移除"').or_(
                page.locator('text="從稍後觀看中移除"')
            ).or_(
                page.locator('ytd-menu-service-item-renderer').filter(has_text="移除")
            ).first

            remove_option.click()
            page.wait_for_timeout(1000)

            removed_count += 1
            logger.info(f"已刪除第 {removed_count} 個影片")

        except Exception as e:
            logger.error(f"刪除影片時發生錯誤: {e}")
            break

    logger.info(f"成功刪除 {removed_count} 個影片")


def main(video_limit=10):
    """
    主程式
    Args:
        video_limit: 每次執行時抓取和刪除的影片數上限（預設 10）
    """
    with sync_playwright() as p:
        # 啟動瀏覽器（非無頭模式，方便手動登入）
        # 使用持久化上下文來保存登入狀態，並減少自動化偵測
        browser = p.chromium.launch_persistent_context(
            user_data_dir='./youtube_profile',  # 使用持久化檔案夾保存登入狀態
            headless=False,
            args=[
                '--start-maximized',
                '--disable-blink-features=AutomationControlled',  # 隱藏自動化標記
            ],
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        )

        page = browser.new_page()

        # 移除 webdriver 標記
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        # 前往 YouTube 首頁
        page.goto('https://www.youtube.com')

        print("=" * 60)
        print("請手動登入您的 YouTube 帳號")
        print("登入完成後，請在終端機按 Enter 鍵繼續...")
        print("=" * 60)
        input()

        try:
            iteration = 1
            while True:
                logger.info(f"\n{'=' * 60}")
                logger.info(f"第 {iteration} 次執行 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info("=" * 60)

                # 獲取稍後觀看清單（限制數量）
                videos = get_watch_later_videos(page, limit=video_limit)

                if videos:
                    # 保存資料到資料庫
                    save_to_database(videos)
                    # 刪除已處理的影片（最多刪除 video_limit 個）
                    remove_count = min(len(videos), video_limit)
                    remove_videos_from_watch_later(page, remove_count)
                else:
                    logger.info("稍後觀看清單為空")

                logger.info(f"\n等待 1 小時後執行下一次...")

                # 等待 1 小時 (3600 秒)
                time.sleep(3600)
                iteration += 1

        except KeyboardInterrupt:
            logger.info("\n\n程式已被使用者中斷")

        finally:
            print("關閉瀏覽器...")
            browser.close()


if __name__ == '__main__':
    import sys
    # 可以從命令列參數傳入限制數量，預設為 10
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    main(video_limit=limit)