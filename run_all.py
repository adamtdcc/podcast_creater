import subprocess
import sys
from util import init_logger

logger = init_logger(__name__)

def run_script(script_name, skip_on_error=False):
    """執行指定的 Python 腳本"""
    logger.info(f"{'=' * 60}")
    logger.info(f"開始執行: {script_name}")
    logger.info(f"{'=' * 60}")
    
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            check=False,
            text=True,
            encoding='utf-8'
        )
        
        if result.returncode == 0:
            logger.info(f"✓ {script_name} 執行完成")
            return True
        else:
            logger.error(f"✗ {script_name} 執行失敗 (錯誤碼: {result.returncode})")
            if skip_on_error:
                logger.info(f"跳過錯誤，繼續執行下一個腳本")
                return True
            return False
    except Exception as e:
        logger.error(f"✗ {script_name} 執行時發生異常: {e}")
        if skip_on_error:
            logger.info(f"跳過錯誤，繼續執行下一個腳本")
            return True
        return False

def main():
    """依序執行檔案 2, 3, 4, 5"""
    scripts = [
        ('2.youtube_get_channel.py', True),
        # ('3.youtube_watch_later.py', True),
        ('4.download_youtube.py', False),
        ('5.upload_and_rss.py', False)
    ]
    
    logger.info("開始依序執行腳本...")
    
    for script, skip_on_error in scripts:
        success = run_script(script, skip_on_error)
        if not success:
            logger.error(f"由於 {script} 執行失敗，停止後續執行")
            sys.exit(1)
        logger.info("")
    
    logger.info("=" * 60)
    logger.info("所有腳本執行完成！")
    logger.info("=" * 60)

if __name__ == '__main__':
    main()
