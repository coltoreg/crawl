"""
自訂感測器模組
提供 Airflow 感測器來監控網站狀態
"""

import os
import sys
import time
import logging
import requests
from typing import Dict, List, Optional, Set, Any, Union

# 將專案根目錄加入 Python 路徑
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

# 設定日誌
logger = logging.getLogger("crawler_sensors")

class WebsiteMonitorSensor(BaseSensorOperator):
    """
    網站監控感測器，用於檢查目標網站是否可訪問
    """
    
    @apply_defaults
    def __init__(
        self,
        website_url: str,
        request_timeout: int = 10,
        status_code_ranges: List[range] = [range(200, 300)],
        *args,
        **kwargs
    ) -> None:
        """
        初始化網站監控感測器
        
        Args:
            website_url: 要監控的網站 URL
            request_timeout: 請求超時時間（秒）
            status_code_ranges: 可接受的狀態碼範圍
            *args: 傳遞給基礎感測器的參數
            **kwargs: 傳遞給基礎感測器的關鍵字參數
        """
        super().__init__(*args, **kwargs)
        self.website_url = website_url
        self.request_timeout = request_timeout
        self.status_code_ranges = status_code_ranges
    
    def poke(self, context: Dict[str, Any]) -> bool:
        """
        檢查網站是否可訪問
        
        Args:
            context: Airflow任務上下文
            
        Returns:
            bool: 如果網站可訪問，則返回 True，否則返回 False
        """
        from config import BROWSER_CONFIG
        
        logger.info(f"檢查網站是否可訪問: {self.website_url}")
        
        try:
            # 使用隨機 User-Agent 降低被封風險
            headers = {}
            if BROWSER_CONFIG.get("user_agent_mode") == "random":
                # 使用一個常見的 User-Agent
                headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36"
            
            # 發送 HEAD 請求檢查網站可訪問性，減少資源消耗
            response = requests.head(
                self.website_url,
                timeout=self.request_timeout,
                headers=headers,
                allow_redirects=True
            )
            
            # 檢查狀態碼是否在可接受範圍內
            status_code = response.status_code
            is_valid_status = any(status_code in code_range for code_range in self.status_code_ranges)
            
            if is_valid_status:
                logger.info(f"網站 {self.website_url} 可訪問，狀態碼: {status_code}")
                return True
            else:
                logger.warning(f"網站 {self.website_url} 狀態碼不在可接受範圍: {status_code}")
                return False
                
        except requests.RequestException as e:
            logger.warning(f"檢查網站 {self.website_url} 時發生異常: {e}")
            # 將異常信息推送到 XCom
            context.get('ti').xcom_push(key=f"website_monitor_{self.website_url}_error", value=str(e))
            return False

class ContentChangeSensor(BaseSensorOperator):
    """
    內容變更感測器，用於檢測網站內容是否有變更
    """
    
    @apply_defaults
    def __init__(
        self,
        website_url: str,
        check_element_xpath: str,
        request_timeout: int = 10,
        *args,
        **kwargs
    ) -> None:
        """
        初始化內容變更感測器
        
        Args:
            website_url: 要監控的網站 URL
            check_element_xpath: 用於檢查變更的元素 XPath
            request_timeout: 請求超時時間（秒）
            *args: 傳遞給基礎感測器的參數
            **kwargs: 傳遞給基礎感測器的關鍵字參數
        """
        super().__init__(*args, **kwargs)
        self.website_url = website_url
        self.check_element_xpath = check_element_xpath
        self.request_timeout = request_timeout
        # 儲存上次檢查的內容哈希值
        self.last_content_hash = None
    
    def poke(self, context: Dict[str, Any]) -> bool:
        """
        檢查網站內容是否有變更
        
        Args:
            context: Airflow任務上下文
            
        Returns:
            bool: 如果內容有變更或是首次檢查，則返回 True，否則返回 False
        """
        import hashlib
        from lxml import html
        from config import BROWSER_CONFIG
        
        logger.info(f"檢查網站內容是否有變更: {self.website_url}")
        
        try:
            # 使用隨機 User-Agent 降低被封風險
            headers = {}
            if BROWSER_CONFIG.get("user_agent_mode") == "random":
                # 使用一個常見的 User-Agent
                headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36"
            
            # 發送 GET 請求獲取網頁內容
            response = requests.get(
                self.website_url,
                timeout=self.request_timeout,
                headers=headers
            )
            
            # 解析 HTML
            tree = html.fromstring(response.content)
            
            # 獲取指定元素的內容
            elements = tree.xpath(self.check_element_xpath)
            if not elements:
                logger.warning(f"在網站 {self.website_url} 中找不到指定元素: {self.check_element_xpath}")
                return False
            
            # 獲取元素內容
            content = ''.join([elem.text_content() for elem in elements])
            
            # 計算內容的哈希值
            content_hash = hashlib.md5(content.encode()).hexdigest()
            
            # 檢查內容是否有變更
            if self.last_content_hash is None:
                # 首次檢查，先儲存哈希值但不觸發任務
                logger.info(f"首次檢查網站 {self.website_url}，儲存內容哈希值")
                self.last_content_hash = content_hash
                # 將哈希值推送到 XCom
                context.get('ti').xcom_push(key=f"content_hash_{self.website_url}", value=content_hash)
                return True
            elif content_hash != self.last_content_hash:
                # 內容有變更，更新哈希值並觸發任務
                logger.info(f"網站 {self.website_url} 內容有變更")
                self.last_content_hash = content_hash
                # 將哈希值推送到 XCom
                context.get('ti').xcom_push(key=f"content_hash_{self.website_url}", value=content_hash)
                return True
            else:
                # 內容無變更
                logger.info(f"網站 {self.website_url} 內容無變更")
                return False
                
        except Exception as e:
            logger.warning(f"檢查網站 {self.website_url} 內容時發生異常: {e}")
            # 將異常信息推送到 XCom
            context.get('ti').xcom_push(key=f"content_change_{self.website_url}_error", value=str(e))
            return False
