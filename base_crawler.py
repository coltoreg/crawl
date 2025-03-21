"""
基礎爬蟲模組，提供所有爬蟲的核心功能
"""

import asyncio
import json
import re
import random
import logging
import urllib.parse
from datetime import datetime
from typing import List, Dict, Any, Set, Optional, Union, Tuple, Type

import pandas as pd
from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher
from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    CacheMode,
    RateLimiter as CrawlRateLimiter,
    JsonCssExtractionStrategy
)

from config import (
    BROWSER_CONFIG, 
    SIMULATION_CONFIG, 
    RETRY_CONFIG, 
    CRAWLER_LIMITS,
    SITE_CONFIG,
    OUTPUT_DIR
)
from utils import get_random_delay, extract_publish_time, clean_content, is_valid_url, CrawlerStats

# 設定日誌
logger = logging.getLogger("base_crawler")

class BaseCrawler:
    """
    基礎爬蟲類，提供通用爬蟲功能
    此類應被子類繼承，子類可以覆蓋特定方法以自定義行為
    """
    
    def __init__(self, site_name: str):
        """
        初始化爬蟲
        
        Args:
            site_name: 站點名稱，必須是 SITE_CONFIG 中的一個鍵
        """
        self.site_name = site_name
        
        # 從 SITE_CONFIG 加載站點設定
        if site_name not in SITE_CONFIG:
            raise ValueError(f"未知站點: {site_name}，請確保其在 SITE_CONFIG 中定義")
            
        self.site_config = SITE_CONFIG[site_name]
        self.start_urls = self.site_config["start_urls"]
        self.url_pattern = self.site_config["url_pattern"]
        self.is_regex = self.site_config.get("is_regex", False)
        self.extract_only_metadata = self.site_config.get("extract_only_metadata", False)
        
        # 設定爬蟲限制
        self.max_depth = CRAWLER_LIMITS["max_depth"]
        self.max_pages = CRAWLER_LIMITS["max_pages"]
        self.initial_urls = CRAWLER_LIMITS["initial_urls"]
        
        # 準備爬蟲設定
        self.browser_config = BrowserConfig(**BROWSER_CONFIG)
        self.simulation_config = SIMULATION_CONFIG
        self.retry_config = RETRY_CONFIG
        
        # 設定域名級別延遲
        if self.start_urls:
            self.domain = urllib.parse.urlparse(self.start_urls[0]).netloc
            self.domain_delay = self.site_config.get("domain_delay", 3.0)
        
        # 初始化統計對象
        self.stats = CrawlerStats(site_name)
        
        logger.info(f"初始化 {site_name} 爬蟲")
        logger.info(f"設定: max_depth={self.max_depth}, max_pages={self.max_pages}, " 
                  f"initial_urls={self.initial_urls}")
    
    def _create_dispatcher(self) -> MemoryAdaptiveDispatcher:
        """
        建立爬蟲調度器
        
        Returns:
            MemoryAdaptiveDispatcher: 配置好的調度器實例
        """
        return MemoryAdaptiveDispatcher(
            memory_threshold_percent=90,
            check_interval=60.0,
            max_session_permit=5,
            rate_limiter=CrawlRateLimiter(
                base_delay=(1.0, 4.0),
                max_delay=10.0,
                max_retries=2
            )
        )
        
    def _is_valid_url(self, url: str) -> bool:
        """
        檢查 URL 是否符合有效格式
        
        Args:
            url: 要檢查的 URL
            
        Returns:
            bool: URL 是否有效
        """
        return is_valid_url(url, self.url_pattern, self.is_regex)
    
    async def get_new_links(self) -> List[str]:
        """
        爬取首頁的連結，並過濾符合格式的 URL
        
        Returns:
            List[str]: 符合格式的新連結列表
        """
        run_config = CrawlerRunConfig(cache_mode=CacheMode.BYPASS, **self.simulation_config)
        dispatcher = self._create_dispatcher()
        all_links = set()
        
        try:
            # 應用流量控制
            logger.info(f"開始爬取 {self.site_name} 頁面的連結")
            
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                logger.info(f"開始從 {len(self.start_urls)} 個起始 URL 爬取連結")
                results = await crawler.arun_many(urls=self.start_urls, config=run_config, dispatcher=dispatcher)
                
                for result in results:
                    if not result.success:
                        self.stats.record_failure(
                            result.url, 
                            f"Error: {getattr(result, 'error_message', 'Unknown error')}"
                        )
                        continue
                    
                    # 記錄成功
                    self.stats.record_success(result.url)
                    
                    # 取得內部連結
                    page_links = [item["href"] for item in result.links['internal']]
                    all_links.update(page_links)
                    
            # 過濾符合格式的連結
            filtered_links = []
            for link in all_links:
                if self._is_valid_url(link):
                    filtered_links.append(link)
                    
            filtered_links = list(set(filtered_links))  # 去重
            
            # 避免重複爬取已儲存的內容 (注意：這裡使用延遲引用解決循環引用問題)
            from database import get_existing_urls
            existing_urls = get_existing_urls()
            new_links = list(set(filtered_links) - existing_urls)
            logger.info(f"共找到 {len(filtered_links)} 個有效連結，其中 {len(new_links)} 個是新連結")
            
            # 限制初始爬取數量
            return new_links[:self.initial_urls]
            
        except Exception as e:
            logger.error(f"{self.site_name} 獲取連結失敗: {e}")
            self.stats.record_failure(
                self.start_urls[0] if self.start_urls else "unknown",
                f"Exception: {str(e)}"
            )
            return []
    
    async def extract_article(self, url: str) -> Optional[Dict[str, Any]]:
        """
        解析內容，並提取下一層的內部連結
        
        Args:
            url: 要解析的 URL
            
        Returns:
            Optional[Dict[str, Any]]: 解析結果，失敗時返回 None
        """
        # 設定爬蟲配置
        crawler_config_params = {
            "cache_mode": CacheMode.BYPASS,
            "simulate_user": False,
            **self.simulation_config,
        }
        
        # 如果不是只提取元數據，則添加內容提取策略
        if not self.extract_only_metadata:
            content_selector = self.site_config.get("content_selector")
            if content_selector:
                crawler_config_params["extraction_strategy"] = JsonCssExtractionStrategy(
                    schema=content_selector
                )
        
        crawler_config = CrawlerRunConfig(**crawler_config_params)
        
        try:
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                for attempt in range(self.retry_config["max_retries"]):
                    try:
                        # 添加延遲，模擬人類行為
                        await asyncio.sleep(get_random_delay())
                        
                        logger.info(f"爬取 URL: {url}，第 {attempt+1} 次嘗試")
                        
                        result = await crawler.arun(url=url, config=crawler_config)
                        
                        # 檢查爬取結果
                        if not result or not result.success:
                            error_message = getattr(result, "error_message", "未知錯誤") if result else "無結果返回"
                            status_code = getattr(result, "status_code", None) if result else None
                            logger.warning(f"爬取失敗: {url}，錯誤: {error_message}, 狀態碼: {status_code}")
                            
                            # 如果不是最後一次嘗試，繼續重試
                            if attempt < self.retry_config["max_retries"] - 1:
                                # 計算退避延遲
                                delay = random.uniform(
                                    self.retry_config["min_delay"],
                                    self.retry_config["max_delay"]
                                ) * (attempt + 1)
                                
                                logger.info(f"將在 {delay:.2f} 秒後重試")
                                self.stats.record_retry(url)
                                await asyncio.sleep(delay)
                                continue
                            else:
                                # 最後一次嘗試也失敗，記錄失敗
                                self.stats.record_failure(url, f"Error: {error_message}, Status: {status_code}")
                                
                                # 使用資料庫記錄失敗 (延遲引用)
                                from database import save_failed_url
                                save_failed_url(url, error_message, str(status_code) if status_code else "Unknown")
                                
                                return None
                        
                        # 爬取成功
                        self.stats.record_success(url)
                        
                        # 準備基本元數據
                        article = {
                            "url": url,
                            "title": result.metadata.get("title", ""),
                            "description": result.metadata.get("description", ""),
                            "keywords": result.metadata.get("keywords", ""),
                            "scraped_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        
                        # 如果只提取元數據，則跳過內容提取
                        if self.extract_only_metadata:
                            article['content'] = ''
                        else:
                            try:
                                extracted_content = json.loads(result.extracted_content)
                                # 處理結果格式
                                if isinstance(extracted_content, list) and extracted_content:
                                    extracted_content = extracted_content[0]
                                    
                                if not isinstance(extracted_content, dict):
                                    logger.warning(f"內容格式錯誤: {url}")
                                    logger.warning(f"類型: {type(extracted_content)}, 內容: {extracted_content}")
                                    # 嘗試轉換為字典
                                    if isinstance(extracted_content, str):
                                        article["content"] = extracted_content
                                    else:
                                        article["content"] = str(extracted_content)
                                else:
                                    # 合併內容到文章字典
                                    article.update(extracted_content)
                                    
                            except (json.JSONDecodeError, AttributeError) as e:
                                # 處理解析錯誤
                                logger.error(f"解析提取內容失敗: {url}, 錯誤: {e}")
                                
                                # 這裡是解析錯誤，但仍然獲取了網頁，所以使用原始HTML作為內容
                                article["content"] = result.html[:10000]  # 限制長度
                                
                                # 記錄為解析錯誤，但這裡不算作總體失敗
                                logger.warning(f"解析內容時出現錯誤，但仍繼續處理: {url}")
                                
                        # 如果有內容，可能需要特定站點的清理
                        if "content" in article and article["content"]:
                            article["content"] = clean_content(self.site_name, article["content"])
                            
                            # 嘗試提取發布時間
                            publish_time = extract_publish_time(article["content"])
                            if publish_time:
                                article["publish_time"] = publish_time
                        
                        # 從 result.links['internal'] 提取內部連結
                        raw_links = []
                        if 'internal' in result.links:
                            raw_links = [item["href"] for item in result.links['internal']]
                        
                        valid_links = []
                        for link in raw_links:
                            clean_link = link.split('?')[0]  # 移除查詢參數
                            if self._is_valid_url(clean_link):
                                valid_links.append(clean_link)
                                
                        article["next_level_links"] = list(set(valid_links))  # 去重
                        return article
                
                except Exception as e:
                    logger.error(f"爬取失敗: {url}, 錯誤: {e}")
                    self.stats.record_failure(url, f"Exception: {str(e)}")
                    
                    # 如果不是最後一次嘗試，繼續重試
                    if attempt < self.retry_config["max_retries"] - 1:
                        # 計算退避延遲
                        delay = random.uniform(
                            self.retry_config["min_delay"],
                            self.retry_config["max_delay"]
                        ) * (attempt + 1)
                        
                        logger.info(f"將在 {delay:.2f} 秒後重試 (第 {attempt+1} 次失敗)")
                        self.stats.record_retry(url)
                        await asyncio.sleep(delay)
                    else:
                        # 最後一次嘗試也失敗，記錄失敗
                        from database import save_failed_url
                        save_failed_url(url, str(e), "Exception")
            
            # 所有嘗試都失敗
            return None
        except Exception as e:
            logger.error(f"爬取過程中發生嚴重錯誤: {url}, {e}")
            self.stats.record_failure(url, f"Critical error: {str(e)}")
            return None
    
    async def process_retry_queue(self, max_items: int = 5) -> int:
        """
        處理重試隊列中的任務
        
        Args:
            max_items: 一次處理的最大任務數
            
        Returns:
            int: 成功處理的任務數
        """
        # 延遲引用，避免循環導入
        from database import get_failed_urls, mark_failed_url_processed
        
        # 獲取準備好可以重試的任務
        failed_urls = get_failed_urls(self.site_name, max_items)
        
        if not failed_urls:
            logger.info("重試隊列中沒有準備好的任務")
            return 0
        
        logger.info(f"從重試隊列中取得 {len(failed_urls)} 個任務準備處理")
        
        successful = 0
        for url_info in failed_urls:
            url = url_info.get('url')
            logger.info(f"重試任務: {url}")
            self.stats.record_retry(url)
            
            # 重試任務
            result = await self.extract_article(url)
            
            if result:
                logger.info(f"重試成功: {url}")
                mark_failed_url_processed(url)
                successful += 1
            else:
                logger.warning(f"重試失敗: {url}")
        
        return successful
    
    async def run_full_scraper(self) -> bool:
        """
        執行完整的爬蟲流程，進行多層爬取
        
        Returns:
            bool: 爬取是否成功完成
        """
        from config import OUTPUT_DIR
        # 使用延遲引用避免循環引用
        from database import save_to_mysql
        from kafka_manager import send_crawl_result
        from es_manager import bulk_index_articles
        
        logger.info(f"開始 {self.site_name} 完整爬蟲")
        seen_urls = set()
        queue = await self.get_new_links()
        
        if not queue:
            logger.warning(f"{self.site_name} 沒有獲取到任何連結，爬蟲終止")
            self.stats.finish()
            return False
            
        depth = 0
        total_scraped = 0
        
        while queue and depth < self.max_depth and total_scraped < self.max_pages:
            logger.info(f"爬取第 {depth+1} 層，共 {len(queue)} 條連結")
            next_queue = []
            articles = []
            
            # 首先處理一些重試任務
            retry_processed = await self.process_retry_queue(max_items=min(5, self.max_pages - total_scraped))
            total_scraped += retry_processed
            
            for link in queue:
                if link in seen_urls or total_scraped >= self.max_pages:
                    continue
                    
                seen_urls.add(link)
                
                # 添加適當的等待時間
                await asyncio.sleep(get_random_delay(1.0, self.domain_delay * 1.5))
                
                article = await self.extract_article(link)
                if article:
                    # 發送爬蟲結果到 Kafka
                    send_crawl_result(article)
                    articles.append(article)
                    total_scraped += 1
                    
                    # 添加下一層連結
                    next_links = article.get("next_level_links", [])
                    next_queue.extend(next_links)
                    
                    logger.info(f"成功爬取 {link}，找到 {len(next_links)} 個下一層連結")
                
                if total_scraped >= self.max_pages:
                    logger.info(f"已達到最大爬取頁面數 {self.max_pages}，停止爬取")
                    break
            
            if articles:
                # 保存到 CSV 和資料庫
                df = pd.DataFrame(articles)
                
                # 移除 next_level_links 欄位，它不需要儲存
                if "next_level_links" in df.columns:
                    df = df.drop("next_level_links", axis=1)
                
                # 添加網站識別資訊
                df["website_category"] = self.site_config["website_category"]
                df["site"] = self.site_name
                df["site_id"] = self.site_config["site_id"]
                
                # 保存到 CSV
                output_path = OUTPUT_DIR / f"{self.site_name}_depth_{depth+1}.csv"
                df.to_csv(output_path, index=False)
                logger.info(f"已保存 {len(articles)} 篇文章到 {output_path}")
                
                # 保存到 MySQL
                if save_to_mysql(df):
                    logger.info(f"已將 {len(articles)} 篇文章保存到 MySQL")
                else:
                    logger.warning(f"保存到 MySQL 失敗")
                
                # 索引到 Elasticsearch
                es_result = bulk_index_articles(df.to_dict(orient="records"))
                if es_result:
                    logger.info(f"已將 {len(articles)} 篇文章索引到 Elasticsearch")
                else:
                    logger.warning(f"索引到 Elasticsearch 失敗")
            
            # 準備下一層爬取
            queue = list(set(next_queue) - seen_urls)
            depth += 1
            
            # 再次處理一些重試任務
            if total_scraped < self.max_pages:
                retry_processed = await self.process_retry_queue(max_items=min(5, self.max_pages - total_scraped))
                total_scraped += retry_processed
        
        # 爬蟲結束，生成報告
        self.stats.finish()
        report = self.stats.generate_report()
        
        # 將報告保存為 JSON
        report_path = OUTPUT_DIR / f"{self.site_name}_report.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        logger.info(f"{self.site_name} 爬蟲完成，共爬取 {total_scraped} 個頁面，深度 {depth}")
        logger.info(f"成功率: {report['success_rate']}")
        
        return True

# 註冊各站點特定爬蟲類的字典
SITE_CRAWLER_REGISTRY = {}

def register_crawler(site_name: str, crawler_class: Type[BaseCrawler]):
    """
    註冊特定站點的爬蟲類
    
    Args:
        site_name: 站點名稱
        crawler_class: 爬蟲類
    """
    SITE_CRAWLER_REGISTRY[site_name] = crawler_class
    logger.info(f"註冊站點爬蟲: {site_name} -> {crawler_class.__name__}")

# 工廠函數，根據站點名稱創建對應的爬蟲
def create_crawler(site_name: str) -> BaseCrawler:
    """
    根據站點名稱創建對應的爬蟲實例
    
    Args:
        site_name: 站點名稱
        
    Returns:
        BaseCrawler: 對應的爬蟲實例
    """
    # 檢查是否有特定的爬蟲類註冊
    if site_name in SITE_CRAWLER_REGISTRY:
        logger.info(f"使用註冊的 {site_name} 特定爬蟲類")
        return SITE_CRAWLER_REGISTRY[site_name]()
    
    # 沒有特定爬蟲類，使用基礎爬蟲類
    logger.info(f"使用基礎爬蟲類處理 {site_name}")
    return BaseCrawler(site_name)

async def batch_crawl(site_names: List[str]) -> Dict[str, bool]:
    """
    批量執行多個站點的爬蟲
    
    Args:
        site_names: 站點名稱列表
        
    Returns:
        Dict[str, bool]: 站點爬取結果，成功為 True，失敗為 False
    """
    tasks = {site_name: create_crawler(site_name).run_full_scraper() for site_name in site_names}
    
    results = {}
    for site_name, task in tasks.items():
        try:
            logger.info(f"開始執行 {site_name} 爬蟲")
            results[site_name] = await task
            logger.info(f"{site_name} 爬蟲完成，結果: {'成功' if results[site_name] else '失敗'}")
        except Exception as e:
            logger.error(f"{site_name} 爬蟲執行異常: {e}")
            results[site_name] = False
    
    return results