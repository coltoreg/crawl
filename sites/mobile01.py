"""
Mobile01 文章爬蟲模組
此模組示範如何基於 BaseCrawler 擴展特定站點爬蟲功能
"""

import asyncio
import json
import re
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

from base_crawler import BaseCrawler
from config import get_random_delay

# 設定日誌
logger = logging.getLogger("Mobile01_crawler")

class Mobile01Crawler(BaseCrawler):
    """Mobile01文章爬蟲，繼承並擴展 BaseCrawler"""
    
    def __init__(self):
        """初始化 Mobile01 爬蟲"""
        super().__init__("mobile01")
        logger.info("Mobile01 爬蟲初始化完成")
    
    async def extract_article(self, url: str) -> Optional[Dict[str, Any]]:
        """
        對新聞進行特殊處理
        
        Args:
            url: 要解析的 URL
            
        Returns:
            Optional[Dict[str, Any]]: 解析結果，失敗時返回 None
        """
        # 調用基礎類方法獲取內容
        article = await super().extract_article(url)
        
        # 如果提取成功，進行特定的內容處理
        if article:
            # 處理內容，例如移除廣告、清理格式等
            if "content" in article:
                # 特定處理：提取發布時間
                match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2})", article["content"])
                if match:
                    article["publish_time"] = match.group(1)
                    print(f"publish time is {article["publish_time"]}")  
                logger.info(f"Mobile01 文章處理完成: {url}, 長度: {len(article['content'])}")
            
        return article


# 為保持與舊版代碼兼容的接口函數
async def get_new_links() -> List[str]:
    """獲取新連結 (兼容舊版接口)"""
    crawler = Mobile01Crawler()
    return await crawler.get_new_links()

async def run_full_scraper() -> bool:
    """執行完整爬蟲 (兼容舊版接口)"""
    crawler = Mobile01Crawler()
    return await crawler.run_full_scraper()