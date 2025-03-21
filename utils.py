"""
通用工具模組，提供跨模組使用的功能
"""

import re
import random
import logging
from typing import Optional, Dict, Any, List, Set, Pattern
from datetime import datetime

# 初始化日誌
logger = logging.getLogger("crawler_utils")

def get_random_delay(min_delay: float = 2.0, max_delay: float = 6.0) -> float:
    """
    生成隨機延遲時間
    
    Args:
        min_delay: 最小延遲時間 (秒)
        max_delay: 最大延遲時間 (秒)
        
    Returns:
        float: 隨機延遲時間 (秒)
    """
    return random.uniform(min_delay, max_delay)

def extract_publish_time(content: str) -> Optional[str]:
    """
    從內容中提取發布時間，並統一格式為 YYYY-MM-DD HH:MM:SS
    
    Args:
        content: 要提取時間的文本內容
        
    Returns:
        Optional[str]: 提取到的時間字符串，格式為 YYYY-MM-DD HH:MM:SS，未找到則返回 None
    """
    # 嘗試多種格式匹配
    patterns = [
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}(?::\d{2})?)",  # YYYY-MM-DD HH:MM:SS 或 YYYY-MM-DD HH:MM
        r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2}(?::\d{2})?)",  # YYYY/MM/DD HH:MM:SS 或 YYYY/MM/DD HH:MM
        r"(\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}(?::\d{2})?)"  # YYYY.MM.DD HH:MM:SS 或 YYYY.MM.DD HH:MM
    ]
    
    for pattern in patterns:
        match = re.search(pattern, content)
        if match:
            date_str = match.group(1).replace("/", "-").replace(".", "-")
            # 補充秒數如果沒有
            if len(date_str) == 16:  # YYYY-MM-DD HH:MM 格式
                date_str += ":00"
            return date_str
    
    return None

def clean_content(site: str, content: str) -> str:
    """
    根據站點特性清理文章內容
    
    Args:
        site: 站點名稱
        content: 原始內容
        
    Returns:
        str: 清理後的內容
    """
    # 通用清理：移除分享、影音、推薦閱讀等非文章內容
    content = re.sub(r"分享\s*推薦.*?$", "", content, flags=re.DOTALL)
    content = re.sub(r"相關新聞\s*推薦.*?$", "", content, flags=re.DOTALL)
    content = re.sub(r"影音推薦.*?$", "", content, flags=re.DOTALL)
    
    # 站點特定清理
    if site == "tvbs":
        # TVBS特定清理邏輯
        content = re.sub(r"相關新聞：.*?$", "", content, flags=re.DOTALL)
    elif site == "udn":
        # UDN特定清理邏輯
        content = re.sub(r"延伸閱讀：.*?$", "", content, flags=re.DOTALL)
    elif site == "setn":
        # 三立新聞特定清理邏輯
        content = re.sub(r"三立新聞網[／\s]", "", content)
        content = re.sub(r"記者[\u4e00-\u9fff]+?／[^／]+?報導", "", content)
    
    return content

def is_valid_url(url: str, pattern: str, is_regex: bool = False) -> bool:
    """
    檢查 URL 是否符合指定的模式
    
    Args:
        url: 要檢查的 URL
        pattern: URL模式或正則表達式
        is_regex: 是否作為正則表達式處理
        
    Returns:
        bool: URL 是否匹配
    """
    if not pattern:  # 如果沒有定義 pattern，所有 URL 都視為有效
        return True
        
    if is_regex:
        return bool(re.match(pattern, url))
    else:
        return pattern in url

class CrawlerStats:
    """追蹤爬蟲統計數據的類"""
    
    def __init__(self, site_name: str):
        """
        初始化統計對象
        
        Args:
            site_name: 站點名稱
        """
        self.site_name = site_name
        self.start_time = datetime.now()
        self.end_time = None
        self.total_urls = 0
        self.successful_urls = 0
        self.failed_urls = 0
        self.retry_urls = 0
        self.depth_stats = {}  # 按深度統計
        self.error_types = {}  # 錯誤類型統計
    
    def record_success(self, url: str, depth: int = 0) -> None:
        """
        記錄成功爬取的URL
        
        Args:
            url: 成功的URL
            depth: 爬取深度
        """
        self.successful_urls += 1
        self.total_urls += 1
        
        # 記錄深度統計
        self.depth_stats[depth] = self.depth_stats.get(depth, 0) + 1
    
    def record_failure(self, url: str, error_type: str, depth: int = 0) -> None:
        """
        記錄失敗的URL
        
        Args:
            url: 失敗的URL
            error_type: 錯誤類型
            depth: 爬取深度
        """
        self.failed_urls += 1
        self.total_urls += 1
        
        # 記錄錯誤類型統計
        self.error_types[error_type] = self.error_types.get(error_type, 0) + 1
    
    def record_retry(self, url: str) -> None:
        """
        記錄重試的URL
        
        Args:
            url: 重試的URL
        """
        self.retry_urls += 1
    
    def finish(self) -> None:
        """標記爬蟲結束時間"""
        self.end_time = datetime.now()
    
    def generate_report(self) -> Dict[str, Any]:
        """
        生成統計報告
        
        Returns:
            Dict[str, Any]: 統計報告字典
        """
        if not self.end_time:
            self.finish()
        
        duration = (self.end_time - self.start_time).total_seconds()
        success_rate = (self.successful_urls / self.total_urls * 100) if self.total_urls > 0 else 0
        
        return {
            "site_name": self.site_name,
            "start_time": self.start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": self.end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_seconds": duration,
            "total_urls": self.total_urls,
            "successful_urls": self.successful_urls,
            "failed_urls": self.failed_urls,
            "retry_urls": self.retry_urls,
            "success_rate": f"{success_rate:.2f}%",
            "depth_stats": self.depth_stats,
            "error_types": self.error_types
        }