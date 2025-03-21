"""
三立新聞網 (SETN) 爬蟲模組
此模組實現了對三立新聞網的內容爬取與處理
"""

import re
import logging
from typing import List, Dict, Any, Optional, Set
from datetime import datetime

from base_crawler import BaseCrawler, register_crawler
from utils import extract_publish_time

# 設定站點名稱
SITE_NAME = "setn"

# 設定日誌
logger = logging.getLogger(f"{SITE_NAME}_crawler")

class SetnCrawler(BaseCrawler):
    """三立新聞網爬蟲，繼承並擴展 BaseCrawler"""
    
    def __init__(self):
        """初始化三立新聞網爬蟲"""
        super().__init__(SITE_NAME)
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
            if "content" in article:             
                # 清理內容
                article["content"] = self._process_content(article["content"])
                
                # 提取發布時間
                time_match = re.search(r"(\d{4}/\d{1,2}/\d{1,2} \d{1,2}:\d{1,2})", article["content"])
                if time_match:
                    # 格式化時間為標準格式
                    try:
                        raw_time = time_match.group(1)
                        dt = datetime.strptime(raw_time, "%Y/%m/%d %H:%M")
                        article["publish_time"] = dt.strftime("%Y-%m-%d %H:%M:%S")
                        logger.info(f"提取到發布時間: {article['publish_time']}")
                    except Exception as e:
                        logger.warning(f"解析發布時間失敗: {raw_time}, 錯誤: {e}")
                
                # 提取新聞ID和類別
                self._extract_metadata(article, url)
                
                logger.info(f"SETN 文章處理完成: {url}, 長度: {len(article['content'])}")               
        return article
    
    def _process_content(self, content: str) -> str:
        """
        處理三立新聞網內容
        
        Args:
            content: 原始內容
            
        Returns:
            str: 處理後的內容
        """
        # 移除"三立新聞網"字樣
        content = re.sub(r"三立新聞網[／\s]", "", content)
        
        # 移除記者名稱格式
        content = re.sub(r"記者[\u4e00-\u9fff]+?／[^／]+?報導", "", content)
        
        # 移除推薦閱讀等贅餘內容
        content = re.sub(r"延伸閱讀.*?$", "", content, flags=re.DOTALL)
        content = re.sub(r"推薦閱讀.*?$", "", content, flags=re.DOTALL)
        content = re.sub(r"▲.*?$", "", content, flags=re.DOTALL)
        
        # 移除多餘空行
        content = re.sub(r"\n\s*\n", "\n\n", content)
        content = content.strip()
        
        return content
    
    def _extract_metadata(self, article: Dict[str, Any], url: str) -> None:
        """
        提取三立新聞網的元數據
        
        Args:
            article: 文章字典，可以直接修改
            url: 文章 URL
        """
        # 提取新聞 ID
        news_id_match = re.search(r"NewsID=(\d+)", url)
        if news_id_match:
            article["news_id"] = news_id_match.group(1)
            logger.info(f"提取到新聞 ID: {article['news_id']}")
        
        # 提取分類 (如果可用)
        category_match = re.search(r"PageGroupID=(\d+)", url)
        if category_match:
            article["category_id"] = category_match.group(1)
        
        # 添加爬取時間
        article["crawl_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
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


# 註冊爬蟲類
register_crawler(SITE_NAME, SetnCrawler)

# 為保持與舊版代碼兼容的接口函數
async def get_new_links() -> List[str]:
    """獲取新連結 (兼容舊版接口)"""
    crawler = SetnCrawler()
    return await crawler.get_new_links()

async def run_full_scraper() -> bool:
    """執行完整爬蟲 (兼容舊版接口)"""
    crawler = SetnCrawler()
    return await crawler.run_full_scraper()