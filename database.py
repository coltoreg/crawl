# database.py
import csv
import json
import pymysql
import logging
from datetime import datetime
from pathlib import Path
from typing import Set, Dict, Any, List, Optional, Union
from config import DB_CONFIG, OUTPUT_DIR

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(OUTPUT_DIR / "crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("database")

# 快取已爬取的URL
_url_cache: Optional[Set[str]] = None

def save_failed_url(url: str, reason: str, status_code: str = "Unknown") -> None:
    """
    記錄爬取失敗的 URL，存入 MySQL 和 CSV。

    Args:
        url (str): 爬取失敗的 URL。
        reason (str): 失敗的錯誤訊息。
        status_code (str): HTTP 狀態碼，預設為 "Unknown"。
    """
    failed_csv_path = OUTPUT_DIR / "failed_urls.csv"

    # 1️⃣ 存入 CSV
    file_exists = failed_csv_path.exists()

    with open(failed_csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["url", "reason", "status_code", "failed_time"])  # 寫入標頭
        writer.writerow([url, reason, status_code, datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
    logger.error(f"❌ 已記錄失敗 URL: {url}, 原因: {reason}, 狀態碼: {status_code}")

    # 2️⃣ 存入 MySQL
    connection = None
    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()

        sql = """
        INSERT INTO failed_crawls (url, reason, status_code, failed_time)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        reason=VALUES(reason),
        status_code=VALUES(status_code),
        failed_time=VALUES(failed_time);
        """

        cursor.execute(sql, (url, reason, status_code, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        connection.commit()
        logger.info(f"✅ 失敗 URL 存入 MySQL: {url}")

    except pymysql.Error as e:
        logger.error(f"❌ MySQL 存入失敗 URL 時發生錯誤: {e}")

    finally:
        if connection:
            cursor.close()
            connection.close()

def save_permanent_failure(
    url: str, 
    error_category: str, 
    error_message: str, 
    status_code: Optional[int] = None, 
    site_name: str = "", 
    retry_count: int = 0,
    extra_data: Optional[Dict[str, Any]] = None
) -> bool:
    """
    將永久失敗的 URL 保存到數據庫
    
    Args:
        url: 失敗的 URL
        error_category: 錯誤類別
        error_message: 錯誤訊息
        status_code: HTTP 狀態碼
        site_name: 站點名稱
        retry_count: 重試次數
        extra_data: 額外數據
        
    Returns:
        bool: 操作是否成功
    """
    permanent_failures_csv_path = OUTPUT_DIR / "permanent_failures.csv"
    file_exists = permanent_failures_csv_path.exists()
    
    # 1️⃣ 存入 CSV
    try:
        with open(permanent_failures_csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    "url", "error_category", "error_message", "status_code", 
                    "site_name", "retry_count", "extra_data", "failed_time"
                ])
                
            # 將額外數據轉換為JSON字符串
            extra_data_str = json.dumps(extra_data, ensure_ascii=False) if extra_data else ""
            
            writer.writerow([
                url, 
                error_category, 
                error_message[:500],  # 避免訊息過長 
                status_code or "Unknown", 
                site_name,
                retry_count,
                extra_data_str,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ])
        logger.warning(f"⚠️ 已記錄永久失敗 URL: {url}, 類別: {error_category}, 重試次數: {retry_count}")
    except Exception as e:
        logger.error(f"❌ 保存永久失敗 URL 到 CSV 時發生錯誤: {e}")
    
    # 2️⃣ 存入 MySQL
    connection = None
    try:
        # 將額外數據轉換為JSON字符串
        extra_data_json = json.dumps(extra_data, ensure_ascii=False) if extra_data else None
        
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()

        # 檢查永久失敗表是否存在，不存在則創建
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS permanent_failures (
            id INT AUTO_INCREMENT PRIMARY KEY,
            url VARCHAR(1000) NOT NULL,
            error_category VARCHAR(50) NOT NULL,
            error_message TEXT NOT NULL,
            status_code VARCHAR(50),
            site_name VARCHAR(100) NOT NULL,
            retry_count INT NOT NULL DEFAULT 0,
            extra_data TEXT,
            failed_at DATETIME NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY(url(255))
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        connection.commit()

        # 插入永久失敗記錄
        sql = """
        INSERT INTO permanent_failures
        (url, error_category, error_message, status_code, site_name, retry_count, extra_data, failed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        error_category=VALUES(error_category),
        error_message=VALUES(error_message),
        status_code=VALUES(status_code),
        site_name=VALUES(site_name),
        retry_count=VALUES(retry_count),
        extra_data=VALUES(extra_data),
        failed_at=VALUES(failed_at);
        """

        cursor.execute(sql, (
            url, 
            error_category, 
            error_message, 
            str(status_code) if status_code else "Unknown", 
            site_name,
            retry_count,
            extra_data_json,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ))
        connection.commit()
        logger.info(f"✅ 永久失敗 URL 存入 MySQL: {url}")
        return True

    except pymysql.Error as e:
        logger.error(f"❌ MySQL 存入永久失敗 URL 時發生錯誤: {e}")
        return False

    finally:
        if connection:
            cursor.close()
            connection.close()

def get_permanent_failures(site_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    獲取永久失敗的URL列表
    
    Args:
        site_name: 站點名稱 (可選)
        
    Returns:
        List[Dict[str, Any]]: 永久失敗URL列表
    """
    connection = None
    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor(pymysql.cursors.DictCursor)  # 使用字典游標

        # 查詢SQL
        if site_name:
            sql = "SELECT * FROM permanent_failures WHERE site_name = %s ORDER BY failed_at DESC"
            cursor.execute(sql, (site_name,))
        else:
            sql = "SELECT * FROM permanent_failures ORDER BY failed_at DESC"
            cursor.execute(sql)

        results = cursor.fetchall()
        
        # 處理結果
        processed_results = []
        for row in results:
            # 轉換 extra_data JSON 字符串為字典
            if row['extra_data'] and isinstance(row['extra_data'], str):
                try:
                    row['extra_data'] = json.loads(row['extra_data'])
                except json.JSONDecodeError:
                    row['extra_data'] = {}
            
            # 格式化日期/時間
            if 'failed_at' in row and row['failed_at']:
                row['failed_at'] = row['failed_at'].strftime("%Y-%m-%d %H:%M:%S")
            if 'created_at' in row and row['created_at']:
                row['created_at'] = row['created_at'].strftime("%Y-%m-%d %H:%M:%S")
                
            processed_results.append(row)
        
        logger.info(f"🔍 獲取了 {len(processed_results)} 個永久失敗URL")
        return processed_results
    
    except pymysql.Error as e:
        logger.error(f"❌ 獲取永久失敗URL時發生錯誤: {e}")
        return []

    finally:
        if connection:
            cursor.close()
            connection.close()

def clear_permanent_failures(site_name: Optional[str] = None) -> bool:
    """
    清除永久失敗的URL記錄
    
    Args:
        site_name: 站點名稱 (可選)，若未提供則清除所有記錄
        
    Returns:
        bool: 操作是否成功
    """
    connection = None
    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()

        if site_name:
            sql = "DELETE FROM permanent_failures WHERE site_name = %s"
            cursor.execute(sql, (site_name,))
            affected_rows = cursor.rowcount
            connection.commit()
            logger.info(f"🧹 已清除 {site_name} 站點的 {affected_rows} 個永久失敗記錄")
        else:
            sql = "DELETE FROM permanent_failures"
            cursor.execute(sql)
            affected_rows = cursor.rowcount
            connection.commit()
            logger.info(f"🧹 已清除所有 {affected_rows} 個永久失敗記錄")
        
        return True
    
    except pymysql.Error as e:
        logger.error(f"❌ 清除永久失敗記錄時發生錯誤: {e}")
        return False

    finally:
        if connection:
            cursor.close()
            connection.close()

def save_to_mysql(df) -> bool:
    """
    批量存入 MySQL，允許相同 URL 插入多筆數據。
    
    Args:
        df: 需要存入的數據 DataFrame。
        
    Returns:
        bool: 操作是否成功
    """
    if df.empty:
        logger.warning("⚠️ DataFrame 為空，沒有數據存入 MySQL。")
        return False

    connection = None
    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()

        # 驗證必要欄位是否存在
        required_columns = ["site_id", "website_category", "scraped_time", "site", "title", "content", "url"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"❌ 缺少必要欄位: {missing_columns}")
            return False

        # 清理數據 - 確保字符串欄位不是 None
        for col in ["title", "content", "description", "keywords", "url"]:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str)

        sql = """
        INSERT INTO crawl_data 
        (site_id, website_category, scraped_time, site, title, content, url, description, keywords)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        # 準備批量插入數據
        data_to_insert = []
        for _, row in df.iterrows():
            # 檢查內容長度，避免超出資料庫欄位限制
            content = row["content"]
            if len(content) > 65535:  # TEXT 類型的限制
                content = content[:65535]
                logger.warning(f"⚠️ URL: {row['url']} 內容被截斷，原長度: {len(row['content'])}")
            
            data_to_insert.append((
                int(row["site_id"]),
                row["website_category"],
                row["scraped_time"],
                row["site"],
                row["title"],
                content,
                row["url"],
                row.get("description", ""),
                row.get("keywords", ""),
            ))

        # 批量執行插入
        cursor.executemany(sql, data_to_insert)
        connection.commit()
        logger.info(f"✅ {len(data_to_insert)} 筆數據成功存入 MySQL！")
        
        # 更新快取
        global _url_cache
        if _url_cache is not None:
            _url_cache.update([row["url"] for _, row in df.iterrows()])
            
        return True

    except pymysql.Error as e:
        logger.error(f"❌ MySQL 錯誤: {e}")
        return False

    finally:
        if connection:
            cursor.close()
            connection.close()


def get_existing_urls() -> Set[str]:
    """
    從 MySQL 讀取已存入的 URL，回傳 `set` 以便快速過濾。
    使用快取機制避免頻繁查詢資料庫。

    Returns:
        set: 已經存入 MySQL 的 URL 集合。
    """
    global _url_cache
    
    # 如果快取已存在，直接返回
    if _url_cache is not None:
        logger.info(f"🔍 使用URL快取，共 {len(_url_cache)} 個URL。")
        return _url_cache
    
    connection = None
    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()

        sql = "SELECT url FROM crawl_data"
        cursor.execute(sql)

        _url_cache = {row[0] for row in cursor.fetchall()}
        logger.info(f"🔍 從資料庫讀取，已存在 {len(_url_cache)} 篇文章，將跳過爬取。")
        return _url_cache

    except pymysql.Error as e:
        logger.error(f"❌ MySQL 查詢 URL 錯誤: {e}")
        return set()

    finally:
        if connection:
            cursor.close()
            connection.close()


def clear_url_cache() -> None:
    """清除 URL 快取，強制下次從資料庫讀取"""
    global _url_cache
    _url_cache = None
    logger.info("🧹 已清除 URL 快取")


def init_database() -> bool:
    """
    初始化資料庫表結構，如果表不存在則創建
    
    Returns:
        bool: 操作是否成功
    """
    connection = None
    try:
        connection = pymysql.connect(
            host=DB_CONFIG["host"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"]
        )
        cursor = connection.cursor()
        
        # 創建資料庫（如果不存在）
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        cursor.execute(f"USE {DB_CONFIG['database']}")
        
        # 創建爬蟲數據表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS crawl_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            site_id INT NOT NULL,
            website_category VARCHAR(50) NOT NULL,
            scraped_time DATETIME NOT NULL,
            site VARCHAR(100) NOT NULL,
            title VARCHAR(500) NOT NULL,
            content TEXT NOT NULL,
            url VARCHAR(1000) NOT NULL,
            description TEXT,
            keywords TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX(url(255)),
            INDEX(site_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        
        # 創建爬取失敗記錄表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS failed_crawls (
            id INT AUTO_INCREMENT PRIMARY KEY,
            url VARCHAR(1000) NOT NULL,
            reason TEXT NOT NULL,
            status_code VARCHAR(50) NOT NULL,
            failed_time DATETIME NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY(url(255))
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        
        # 創建永久失敗記錄表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS permanent_failures (
            id INT AUTO_INCREMENT PRIMARY KEY,
            url VARCHAR(1000) NOT NULL,
            error_category VARCHAR(50) NOT NULL,
            error_message TEXT NOT NULL,
            status_code VARCHAR(50),
            site_name VARCHAR(100) NOT NULL,
            retry_count INT NOT NULL DEFAULT 0,
            extra_data TEXT,
            failed_at DATETIME NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY(url(255))
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        
        connection.commit()
        logger.info("✅ 資料庫表結構初始化成功")
        return True
        
    except pymysql.Error as e:
        logger.error(f"❌ 資料庫初始化失敗: {e}")
        return False
        
    finally:
        if connection:
            cursor.close()
            connection.close()

def get_failure_stats() -> Dict[str, Any]:
    """
    獲取失敗任務的統計信息
    
    Returns:
        Dict[str, Any]: 統計信息字典
    """
    connection = None
    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        # 獲取常規失敗統計
        cursor.execute("SELECT COUNT(*) FROM failed_crawls")
        failed_count = cursor.fetchone()[0]
        
        # 獲取永久失敗統計
        cursor.execute("SELECT COUNT(*) FROM permanent_failures")
        permanent_count = cursor.fetchone()[0]
        
        # 按錯誤類別統計永久失敗
        cursor.execute("""
        SELECT error_category, COUNT(*) as count 
        FROM permanent_failures 
        GROUP BY error_category 
        ORDER BY count DESC
        """)
        category_stats = {row[0]: row[1] for row in cursor.fetchall()}
        
        # 按站點統計永久失敗
        cursor.execute("""
        SELECT site_name, COUNT(*) as count 
        FROM permanent_failures 
        GROUP BY site_name 
        ORDER BY count DESC
        """)
        site_stats = {row[0]: row[1] for row in cursor.fetchall()}
        
        # 按狀態碼統計
        cursor.execute("""
        SELECT status_code, COUNT(*) as count 
        FROM permanent_failures 
        GROUP BY status_code 
        ORDER BY count DESC
        """)
        status_stats = {row[0]: row[1] for row in cursor.fetchall()}
        
        # 最近的失敗
        cursor.execute("""
        SELECT url, error_category, site_name, failed_at 
        FROM permanent_failures 
        ORDER BY failed_at DESC 
        LIMIT 5
        """)
        recent_failures = []
        for row in cursor.fetchall():
            recent_failures.append({
                "url": row[0],
                "error_category": row[1],
                "site_name": row[2],
                "failed_at": row[3].strftime("%Y-%m-%d %H:%M:%S") if row[3] else None
            })
        
        return {
            "total_failed": failed_count,
            "total_permanent_failed": permanent_count,
            "by_category": category_stats,
            "by_site": site_stats,
            "by_status": status_stats,
            "recent_failures": recent_failures
        }
        
    except pymysql.Error as e:
        logger.error(f"❌ 獲取失敗統計時發生錯誤: {e}")
        return {
            "error": str(e)
        }
        
    finally:
        if connection:
            cursor.close()
            connection.close()