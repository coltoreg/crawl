"""
三立新聞網 (SETN) 爬蟲模組
此模組實現了對三立新聞網的內容爬取
"""

import asyncio
import json
import re
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Set

from base_crawler import BaseCrawler
from config import get_random_delay

# 設定日誌
logger = logging.getLogger("setn_crawler")

class SetnCrawler(BaseCrawler):
    """三立新聞網爬蟲，繼承並擴展 BaseCrawler"""
    
    def __init__(self):
        """初始化三立新聞網爬蟲"""
        super().__init__("setn")
        logger.info("SETN 爬蟲初始化完成")
    
    async def extract_article(self, url: str) -> Optional[Dict[str, Any]]:
        """
        對三立新聞文章進行特殊處理
        
        Args:
            url: 要解析的 URL
            
        Returns:
            Optional[Dict[str, Any]]: 解析結果，失敗時返回 None
        """
        # 調用基礎類方法獲取內容
        article = await super().extract_article(url)
        
        # 如果提取成功，進行三立新聞特定的內容處理
        if article:
            # 處理內容，例如移除廣告、清理格式等
            if "content" in article:             
                # Todo清理內容
                # 移除"三立新聞網"字樣
                article["content"] = re.sub(r"三立新聞網[／\s]", "", article["content"])
                # 移除記者名稱格式
                article["content"] = re.sub(r"記者[\u4e00-\u9fff]+?／[^／]+?報導", "", article["content"])
                # 提取發布時間（如果有）
                time_match = re.search(r"(\d{4}/\d{1,2}/\d{1,2} \d{1,2}:\d{1,2})", article["content"])
                if time_match:
                    # 格式化時間為標準格式
                    try:
                        raw_time = time_match.group(1)
                        dt = datetime.strptime(raw_time, "%Y/%m/%d %H:%M")
                        article["publish_time"] = dt.strftime("%Y-%m-%d %H:%M:%S")
                    except Exception as e:
                        logger.warning(f"解析發布時間失敗: {raw_time}, 錯誤: {e}")
                
                logger.info(f"SETN 文章處理完成: {url}, 長度: {len(article['content'])}")               
        return article
    
    async def get_new_links(self) -> List[str]:
        """
        獲取三立新聞的新連結，並進行特定處理
        
        Returns:
            List[str]: 符合格式的新連結列表
        """
        links = await super().get_new_links()
        
        # 特殊處理：如果是主站鏡像連結，統一轉為主站
        processed_links = []
        for link in links:
            # 處理三立新聞的主站和鏡像站
            if "setn.com" in link and "star.setn.com" not in link:
                # 確保為主站 URL
                link = link.replace("www.setn.com", "setn.com")
                
            processed_links.append(link)
            
        return processed_links


# 為保持與舊版代碼兼容的接口函數
async def get_new_links() -> List[str]:
    """獲取新連結 (兼容舊版接口)"""
    crawler = SetnCrawler()
    return await crawler.get_new_links()

async def run_full_scraper() -> bool:
    """執行完整爬蟲 (兼容舊版接口)"""
    crawler = SetnCrawler()
    return await crawler.run_full_scraper()