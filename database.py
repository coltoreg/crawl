"""
數據庫操作模組
處理爬蟲數據的存儲、索引和查詢
"""

import csv
import json
import logging
import pymysql
from datetime import datetime
from pathlib import Path
from typing import Set, Dict, Any, List, Optional, Union, Tuple

# 從配置中導入必要參數
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


class DatabaseManager:
    """數據庫管理類，管理與數據庫的所有交互"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化數據庫管理器
        
        Args:
            config: 數據庫配置，如未提供則使用 DB_CONFIG
        """
        self.config = config or DB_CONFIG
        self._connection = None
        
    def get_connection(self) -> pymysql.connections.Connection:
        """
        獲取數據庫連接，使用連接池模式
        
        Returns:
            pymysql.connections.Connection: 數據庫連接
        """
        if self._connection is None or not self._connection.open:
            try:
                self._connection = pymysql.connect(**self.config)
                logger.info(f"已建立數據庫連接: {self.config['host']}")
            except pymysql.Error as e:
                logger.error(f"數據庫連接失敗: {e}")
                raise
        return self._connection
    
    def close_connection(self) -> None:
        """關閉數據庫連接"""
        if self._connection and self._connection.open:
            self._connection.close()
            self._connection = None
            logger.info("已關閉數據庫連接")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        執行查詢並返回結果
        
        Args:
            query: SQL 查詢語句
            params: 查詢參數
            
        Returns:
            List[Dict[str, Any]]: 查詢結果
        """
        connection = None
        try:
            connection = self.get_connection()
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
        except pymysql.Error as e:
            logger.error(f"查詢執行失敗: {e}, 查詢: {query}")
            return []
    
    def execute_update(self, query: str, params: Optional[tuple] = None) -> int:
        """
        執行更新操作並返回影響的行數
        
        Args:
            query: SQL 更新語句
            params: 更新參數
            
        Returns:
            int: 影響的行數
        """
        connection = None
        try:
            connection = self.get_connection()
            with connection.cursor() as cursor:
                affected_rows = cursor.execute(query, params)
                connection.commit()
                return affected_rows
        except pymysql.Error as e:
            logger.error(f"更新執行失敗: {e}, 查詢: {query}")
            if connection:
                connection.rollback()
            return 0
    
    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """
        批量執行SQL操作
        
        Args:
            query: SQL 語句
            params_list: 參數列表
            
        Returns:
            int: 影響的行數
        """
        if not params_list:
            return 0
            
        connection = None
        try:
            connection = self.get_connection()
            with connection.cursor() as cursor:
                affected_rows = cursor.executemany(query, params_list)
                connection.commit()
                return affected_rows
        except pymysql.Error as e:
            logger.error(f"批量執行失敗: {e}, 查詢: {query}")
            if connection:
                connection.rollback()
            return 0


# 單例模式
_db_manager = None

def get_db_manager() -> DatabaseManager:
    """
    獲取數據庫管理器實例（單例模式）
    
    Returns:
        DatabaseManager: 數據庫管理器實例
    """
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


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
    db_manager = get_db_manager()
    try:
        sql = """
        INSERT INTO failed_crawls (url, reason, status_code, failed_time)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        reason=VALUES(reason),
        status_code=VALUES(status_code),
        failed_time=VALUES(failed_time),
        retry_count=retry_count+1;
        """

        db_manager.execute_update(
            sql, 
            (url, reason, status_code, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        logger.info(f"✅ 失敗 URL 存入 MySQL: {url}")

    except Exception as e:
        logger.error(f"❌ MySQL 存入失敗 URL 時發生錯誤: {e}")


def get_failed_urls(site_name: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
    """
    獲取失敗的 URL 列表，可選按站點過濾
    
    Args:
        site_name: 站點名稱 (可選)
        limit: 返回的最大條目數
        
    Returns:
        List[Dict[str, Any]]: 失敗 URL 列表
    """
    db_manager = get_db_manager()
    
    if site_name:
        sql = """
        SELECT * FROM failed_crawls 
        WHERE url LIKE %s AND retry_count < 3
        ORDER BY failed_time DESC 
        LIMIT %s
        """
        return db_manager.execute_query(sql, (f"%{site_name}%", limit))
    else:
        sql = """
        SELECT * FROM failed_crawls 
        WHERE retry_count < 3
        ORDER BY failed_time DESC 
        LIMIT %s
        """
        return db_manager.execute_query(sql, (limit,))


def mark_failed_url_processed(url: str) -> bool:
    """
    標記失敗 URL 已處理，從失敗表中移除
    
    Args:
        url: 要標記的 URL
        
    Returns:
        bool: 操作是否成功
    """
    db_manager = get_db_manager()
    sql = "DELETE FROM failed_crawls WHERE url = %s"
    affected = db_manager.execute_update(sql, (url,))
    
    if affected > 0:
        logger.info(f"已將 URL 從失敗表中移除: {url}")
        return True
    else:
        logger.warning(f"未能移除 URL: {url}")
        return False


def clear_url_cache() -> None:
    """清除 URL 快取，強制下次從資料庫讀取"""
    global _url_cache
    _url_cache = None
    logger.info("🧹 已清除 URL 快取")


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
    
    db_manager = get_db_manager()
    try:
        sql = "SELECT url FROM crawl_data"
        results = db_manager.execute_query(sql)
        
        _url_cache = {row["url"] for row in results}
        logger.info(f"🔍 從資料庫讀取，已存在 {len(_url_cache)} 篇文章，將跳過爬取。")
        return _url_cache

    except Exception as e:
        logger.error(f"❌ MySQL 查詢 URL 錯誤: {e}")
        return set()


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

    # 準備批量插入數據
    db_manager = get_db_manager()
    data_to_insert = []
    
    for _, row in df.iterrows():
        # 檢查內容長度，避免超出資料庫欄位限制
        content = row["content"]
        if len(content) > 65535:  # TEXT 類型的限制
            content = content[:65535]
            logger.warning(f"⚠️ URL: {row['url']} 內容被截斷，原長度: {len(row['content'])}")
        
        # 獲取發布時間（若有）
        publish_time = row.get("publish_time")
        
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
            publish_time if publish_time else None
        ))

    # 批量執行插入
    sql = """
    INSERT INTO crawl_data 
    (site_id, website_category, scraped_time, site, title, content, url, description, keywords, publish_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    try:
        affected_rows = db_manager.execute_many(sql, data_to_insert)
        logger.info(f"✅ {affected_rows} 筆數據成功存入 MySQL！")
        
        # 更新快取
        global _url_cache
        if _url_cache is not None:
            _url_cache.update([row["url"] for _, row in df.iterrows()])
            
        return True
    except Exception as e:
        logger.error(f"❌ MySQL 錯誤: {e}")
        return False


def init_database() -> bool:
    """
    初始化資料庫表結構，如果表不存在則創建
    
    Returns:
        bool: 操作是否成功
    """
    db_manager = get_db_manager()
    
    try:
        # 確保 MySQL 連接已建立
        connection = db_manager.get_connection()
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
            publish_time DATETIME,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX(url(255)),
            INDEX(site_id),
            INDEX(publish_time)
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
            retry_count INT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY(url(255))
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        
        connection.commit()
        logger.info("✅ 資料庫表結構初始化成功")
        return True
        
    except Exception as e:
        logger.error(f"❌ 資料庫初始化失敗: {e}")
        return False


def get_crawl_stats(days: int = 30) -> Dict[str, Any]:
    """
    獲取爬蟲統計數據
    
    Args:
        days: 統計天數
        
    Returns:
        Dict[str, Any]: 統計數據
    """
    db_manager = get_db_manager()
    stats = {}
    
    try:
        # 總文章數
        sql = "SELECT COUNT(*) as total FROM crawl_data"
        result = db_manager.execute_query(sql)
        stats["total_articles"] = result[0]["total"] if result else 0
        
        # 按站點統計
        sql = """
        SELECT site, COUNT(*) as count 
        FROM crawl_data 
        GROUP BY site 
        ORDER BY count DESC
        """
        stats["by_site"] = {row["site"]: row["count"] for row in db_manager.execute_query(sql)}
        
        # 按類別統計
        sql = """
        SELECT website_category, COUNT(*) as count 
        FROM crawl_data 
        GROUP BY website_category 
        ORDER BY count DESC
        """
        stats["by_category"] = {row["website_category"]: row["count"] for row in db_manager.execute_query(sql)}
        
        # 最近天數的爬取統計
        sql = f"""
        SELECT DATE(scraped_time) as date, COUNT(*) as count 
        FROM crawl_data 
        WHERE scraped_time >= DATE_SUB(NOW(), INTERVAL {days} DAY)
        GROUP BY date 
        ORDER BY date DESC
        """
        stats["by_date"] = {row["date"].strftime("%Y-%m-%d"): row["count"] for row in db_manager.execute_query(sql)}
        
        # 失敗URL統計
        sql = "SELECT COUNT(*) as total FROM failed_crawls"
        result = db_manager.execute_query(sql)
        stats["total_failed"] = result[0]["total"] if result else 0
        
        # 失敗原因分析
        sql = """
        SELECT status_code, COUNT(*) as count
        FROM failed_crawls
        GROUP BY status_code
        ORDER BY count DESC
        LIMIT 10
        """
        stats["failed_by_status"] = {row["status_code"]: row["count"] for row in db_manager.execute_query(sql)}
        
        # 最近失敗的URL
        sql = """
        SELECT url, reason, status_code, failed_time, retry_count
        FROM failed_crawls
        ORDER BY failed_time DESC
        LIMIT 10
        """
        stats["recent_failures"] = [
            {
                "url": row["url"],
                "reason": row["reason"] if len(row["reason"]) < 100 else row["reason"][:100] + "...",
                "status_code": row["status_code"],
                "failed_time": row["failed_time"].strftime("%Y-%m-%d %H:%M:%S") if hasattr(row["failed_time"], "strftime") else str(row["failed_time"]),
                "retry_count": row["retry_count"]
            }
            for row in db_manager.execute_query(sql)
        ]
        
        return stats
        
    except Exception as e:
        logger.error(f"❌ 獲取爬蟲統計失敗: {e}")
        return {"error": str(e)}