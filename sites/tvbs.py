"""
TVBS 新聞爬蟲模組
此模組基於 BaseCrawler 擴展 TVBS 特定的爬蟲功能
"""

import re
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

from base_crawler import BaseCrawler, register_crawler
from utils import extract_publish_time

# 設定站點名稱
SITE_NAME = "tvbs"

# 設定日誌
logger = logging.getLogger(f"{SITE_NAME}_crawler")

class TvbsCrawler(BaseCrawler):
    """TVBS 新聞爬蟲，繼承並擴展 BaseCrawler"""
    
    def __init__(self):
        """初始化 TVBS 爬蟲"""
        super().__init__(SITE_NAME)
        logger.info("TVBS 爬蟲初始化完成")
    
    async def extract_article(self, url: str) -> Optional[Dict[str, Any]]:
        """
        對 TVBS 新聞進行特殊處理
        
        Args:
            url: 要解析的 URL
            
        Returns:
            Optional[Dict[str, Any]]: 解析結果，失敗時返回 None
        """
        # 調用基礎類方法獲取內容
        article = await super().extract_article(url)
        
        # 如果提取成功，進行 TVBS 特定的內容處理
        if article:
            if "content" in article:
                # 處理內容
                article["content"] = self._process_content(article["content"])
                
                # 提取發布時間
                match = re.search(r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2})", article["content"])
                if match:
                    # 統一日期格式
                    date_str = match.group(1).replace("/", "-")
                    article["publish_time"] = date_str + ":00"  # 添加秒數
                    logger.info(f"提取到發布時間: {article['publish_time']}")
                
                # 提取新聞類別
                self._extract_metadata(article, url)
                
                logger.info(f"TVBS 文章處理完成: {url}, 長度: {len(article['content'])}")
            
        return article
    
    def _process_content(self, content: str) -> str:
        """
        處理 TVBS 新聞內容
        
        Args:
            content: 原始內容
            
        Returns:
            str: 處理後的內容
        """
        # 移除各種贊助和推薦內容
        content = re.sub(r"分享\s*推薦.*?$", "", content, flags=re.DOTALL)
        content = re.sub(r"相關新聞\s*推薦.*?$", "", content, flags=re.DOTALL)
        content = re.sub(r"影音推薦.*?$", "", content, flags=re.DOTALL)
        content = re.sub(r"加入TVBS會員.*?$", "", content, flags=re.DOTALL)
        
        # 移除更多資訊片段
        content = re.sub(r"更多.*?資訊.*?請見.*?$", "", content, flags=re.DOTALL)
        
        # 移除多餘的空白和換行
        content = re.sub(r"\n\s*\n", "\n\n", content)
        content = content.strip()
        
        return content
    
    def _extract_metadata(self, article: Dict[str, Any], url: str) -> None:
        """
        提取 TVBS 新聞的元數據
        
        Args:
            article: 文章字典，可以直接修改
            url: 文章 URL
        """
        # 提取新聞類別
        category_match = re.search(r"https://news\.tvbs\.com\.tw/([^/]+)/", url)
        if category_match:
            article["category"] = category_match.group(1)
            logger.info(f"提取到新聞分類: {article['category']}")
        
        # 提取新聞 ID
        news_id_match = re.search(r"/(\d+)$", url)
        if news_id_match:
            article["news_id"] = news_id_match.group(1)
        
        # 添加爬取時間
        article["crawl_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# 註冊爬蟲類
register_crawler(SITE_NAME, TvbsCrawler)

# 為保持與舊版代碼兼容的接口函數
async def get_new_links() -> List[str]:
    """獲取新連結 (兼容舊版接口)"""
    crawler = TvbsCrawler()
    return await crawler.get_new_links()

async def run_full_scraper() -> bool:
    """執行完整爬蟲 (兼容舊版接口)"""
    crawler = TvbsCrawler()
    return await crawler.run_full_scraper()