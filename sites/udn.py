"""
UDN 新聞爬蟲模組
此模組實現了對聯合新聞網的特定爬蟲功能
"""

import re
import logging
from typing import List, Dict, Any, Optional

from base_crawler import BaseCrawler, register_crawler
from utils import extract_publish_time

# 設定站點名稱
SITE_NAME = "udn"

# 設定日誌
logger = logging.getLogger(f"{SITE_NAME}_crawler")

class UdnCrawler(BaseCrawler):
    """UDN 聯合新聞網爬蟲，繼承並擴展 BaseCrawler"""
    
    def __init__(self):
        """初始化 UDN 爬蟲"""
        super().__init__(SITE_NAME)
        logger.info("UDN 爬蟲初始化完成")
    
    async def extract_article(self, url: str) -> Optional[Dict[str, Any]]:
        """
        對 UDN 新聞進行特殊處理
        
        Args:
            url: 要解析的 URL
            
        Returns:
            Optional[Dict[str, Any]]: 解析結果，失敗時返回 None
        """
        # 調用基礎類方法獲取內容
        article = await super().extract_article(url)
        
        # 如果提取成功，進行 UDN 特定的內容處理
        if article:
            if "content" in article:
                # 移除廣告和贊助內容
                article["content"] = re.sub(r"加入會員.*?閱讀無上限", "", article["content"], flags=re.DOTALL)
                article["content"] = re.sub(r"延伸閱讀.*?$", "", article["content"], flags=re.DOTALL)
                article["content"] = re.sub(r"贊助商文章.*?$", "", article["content"], flags=re.DOTALL)
                
                # 提取發布時間
                match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2})", article["content"])
                if match:
                    article["publish_time"] = match.group(1) + ":00"  # 添加秒數
                    logger.info(f"提取到發布時間: {article['publish_time']}")
                
                # 從 URL 提取分類
                category_match = re.search(r"udn\.com/news/([^/]+)/", url)
                if category_match:
                    article["category"] = category_match.group(1)
                    logger.info(f"提取到新聞分類: {article['category']}")
                
                logger.info(f"UDN 文章處理完成: {url}, 長度: {len(article['content'])}")
            
        return article
    
    def _process_content(self, content: str) -> str:
        """
        處理 UDN 新聞內容
        
        Args:
            content: 原始內容
            
        Returns:
            str: 處理後的內容
        """
        # 移除廣告和不必要的內容
        content = re.sub(r"加入會員.*?閱讀無上限", "", content, flags=re.DOTALL)
        content = re.sub(r"延伸閱讀.*?$", "", content, flags=re.DOTALL)
        content = re.sub(r"贊助商文章.*?$", "", content, flags=re.DOTALL)
        
        # 移除多餘的空白和換行
        content = re.sub(r"\n\s*\n", "\n\n", content)
        content = content.strip()
        
        return content


# 註冊爬蟲類
register_crawler(SITE_NAME, UdnCrawler)

# 為保持與舊版代碼兼容的接口函數
async def get_new_links() -> List[str]:
    """獲取新連結 (兼容舊版接口)"""
    crawler = UdnCrawler()
    return await crawler.get_new_links()

async def run_full_scraper() -> bool:
    """執行完整爬蟲 (兼容舊版接口)"""
    crawler = UdnCrawler()
    return await crawler.run_full_scraper()