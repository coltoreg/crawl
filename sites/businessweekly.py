"""
商業週刊爬蟲模組
此模組基於 BaseCrawler 擴展知識網特定的爬蟲功能
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
logger = logging.getLogger("businessweekly_crawler")

class Crawler(BaseCrawler):
    """商業週刊爬蟲，繼承並擴展 BaseCrawler"""
    
    def __init__(self):
        """初始化商業週刊爬蟲"""
        super().__init__("businessweekly")
        logger.info("商業週刊初始化完成")
    
    async def extract_article(self, url: str) -> Optional[Dict[str, Any]]:
        """
        對businessweekly新聞進行特殊處理
        
        Args:
            url: 要解析的 URL
            
        Returns:
            Optional[Dict[str, Any]]: 解析結果，失敗時返回 None
        """
        # 調用基礎類方法獲取內容
        article = await super().extract_article(url)
        
        # 如果提取成功，進行知識網特定的內容處理
        if article:
            # 處理內容，例如移除廣告、清理格式等
            if "content" in article:
                # todo: 進行內容處理
                # 移除分享、影音、推薦閱讀等非文章內容
                article["content"] = re.sub(r"分享\s*推薦.*?$", "", article["content"], flags=re.DOTALL)
                article["content"] = re.sub(r"相關新聞\s*推薦.*?$", "", article["content"], flags=re.DOTALL)
                article["content"] = re.sub(r"影音推薦.*?$", "", article["content"], flags=re.DOTALL)
                
                # 知識網特定處理：提取發布時間
                match = re.search(r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2})", article["content"])
                if match:
                    # 統一日期格式
                    date_str = match.group(1).replace("/", "-")
                    article["publish_time"] = date_str
                
                logger.info(f"商業週刊處理完成: {url}, 長度: {len(article['content'])}")
            
        return article


# 為保持與舊版代碼兼容的接口函數
async def get_new_links() -> List[str]:
    """獲取新連結 (兼容舊版接口)"""
    crawler = Crawler()
    return await crawler.get_new_links()

async def run_full_scraper() -> bool:
    """執行完整爬蟲 (兼容舊版接口)"""
    crawler = Crawler()
    return await crawler.run_full_scraper()