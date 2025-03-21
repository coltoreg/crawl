"""
網頁爬蟲模組
只需要爬 Titel, Description, Keywords 都是這個class負責
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
logger = logging.getLogger("Website_crawler")

class WebisteCrawler(BaseCrawler):
    """ 網頁爬蟲，繼承並擴展 BaseCrawler"""
    
    def __init__(self):
        """初始化網頁爬蟲"""
        super().__init__("metadata")
        logger.info("網頁爬蟲初始化完成")
    
    async def extract_article(self, url: str) -> Optional[Dict[str, Any]]:
        """
        對網頁進行特殊處理
        
        Args:
            url: 要解析的 URL
            
        Returns:
            Optional[Dict[str, Any]]: 解析結果，失敗時返回 None
        """
        # 調用基礎類方法獲取內容
        article = await super().extract_article(url)
        # Todo 清理內容
        if article:
            if "content" not in article:
                article["content"] = ""
                logger.info(f"網頁處理完成: {url}, 長度: {len(article['content'])}")
            
        return article


# 為保持與舊版代碼兼容的接口函數
async def get_new_links() -> List[str]:
    """獲取新連結 (兼容舊版接口)"""
    crawler = WebisteCrawler()
    return await crawler.get_new_links()

async def run_full_scraper() -> bool:
    """執行完整爬蟲 (兼容舊版接口)"""
    crawler = WebisteCrawler()
    return await crawler.run_full_scraper()