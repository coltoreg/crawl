import asyncio
import json
import re
import pandas as pd
import random
import logging
from datetime import datetime
from typing import List, Dict, Any, Set, Optional, Union, Tuple
from pathlib import Path
import urllib.parse

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
    OUTPUT_DIR,
    RATE_LIMIT_CONFIG,
    get_random_delay
)
from database import save_failed_url, save_to_mysql, get_existing_urls, save_permanent_failure
from kafka_manager import send_crawl_result
from es_manager import bulk_index_articles
from rate_limiter import get_rate_limiter, RateLimiterManager
from failure_handler import (
    get_failure_handler, 
    register_failure, 
    mark_task_success, 
    ErrorCategory, 
    FailedTask
)

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
        
        # 設定流量控制
        self.rate_limiter = get_rate_limiter()
        # 設定域名級別延遲
        if self.start_urls:
            self.domain = urllib.parse.urlparse(self.start_urls[0]).netloc
            domain_delay = self.site_config.get("domain_delay")
            if domain_delay:
                self.rate_limiter.set_domain_delay(self.domain, domain_delay)
        
        # 初始化延遲重試隊列
        self.retry_queue: List[Dict[str, Any]] = []
        
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
        if not self.url_pattern:  # 如果沒有定義 pattern，所有 URL 都視為有效
            return True
            
        if self.is_regex:
            return bool(re.match(self.url_pattern, url))
        else:
            return self.url_pattern in url
    
    def _classify_error(self, error: Union[Exception, str], status_code: Optional[int] = None) -> ErrorCategory:
        """
        分類錯誤類型
        
        Args:
            error: 錯誤對象或消息
            status_code: HTTP狀態碼
            
        Returns:
            ErrorCategory: 錯誤類別
        """
        # 使用全局失敗處理器的分類方法
        if isinstance(error, Exception):
            return get_failure_handler().classify_error(error, status_code)
        
        # 如果是字符串錯誤訊息，根據狀態碼和關鍵字分類
        if status_code:
            if status_code == 429:
                return ErrorCategory.RATE_LIMIT
            elif status_code == 403:
                return ErrorCategory.PERMISSION
            elif 400 <= status_code < 500:
                return ErrorCategory.CLIENT
            elif 500 <= status_code < 600:
                return ErrorCategory.SERVER
        
        # 根據錯誤消息關鍵字判斷
        error_msg = str(error).lower()
        if any(net_err in error_msg for net_err in ["timeout", "connection", "network"]):
            return ErrorCategory.NETWORK
        elif any(parse_err in error_msg for parse_err in ["parse", "syntax", "json"]):
            return ErrorCategory.PARSING
        
        return ErrorCategory.UNKNOWN
    
    def _register_error(
        self, 
        url: str, 
        error: Union[Exception, str], 
        status_code: Optional[int] = None,
        extra_data: Optional[Dict[str, Any]] = None
    ) -> FailedTask:
        """
        註冊錯誤，並決定重試策略
        
        Args:
            url: 失敗的URL
            error: 錯誤對象或消息
            status_code: HTTP狀態碼
            extra_data: 額外數據
            
        Returns:
            FailedTask: 失敗任務對象
        """
        # 將錯誤註冊到失敗處理器
        task = register_failure(
            url=url,
            site_name=self.site_name,
            error=error,
            status_code=status_code,
            extra_data=extra_data
        )
        
        # 保存到失敗URL數據庫（舊有邏輯保留）
        error_message = task.error_message
        save_failed_url(url, error_message, str(status_code) if status_code else "Unknown")
        
        # 報告失敗給流量控制器
        self.rate_limiter.report_request_result(url, False, status_code)
        
        return task
    
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
            # 應用全局流量控制
            logger.info(f"應用流量控制，等待允許爬取 {self.site_name} 頁面")
            for url in self.start_urls:
                await self.rate_limiter.wait_for_rate_limit(url)
            
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                logger.info(f"開始從 {len(self.start_urls)} 個起始 URL 爬取連結")
                results = await crawler.arun_many(urls=self.start_urls, config=run_config, dispatcher=dispatcher)
                
                for result in results:
                    if not result.success:
                        # 使用新的錯誤處理機制
                        self._register_error(
                            url=result.url,
                            error=result.error_message,
                            status_code=getattr(result, "status_code", None),
                            extra_data={
                                "phase": "get_new_links",
                                "is_start_url": result.url in self.start_urls
                            }
                        )
                        continue
                    
                    # 報告成功結果給流量控制器
                    self.rate_limiter.report_request_result(result.url, True)
                    
                    # 處理任務成功
                    mark_task_success(result.url)
                    
                    # 取得內部連結
                    page_links = [item["href"] for item in result.links['internal']]
                    all_links.update(page_links)
                    
            # 過濾符合格式的連結
            filtered_links = []
            for link in all_links:
                if self._is_valid_url(link):
                    filtered_links.append(link)
                    
            filtered_links = list(set(filtered_links))  # 去重
            
            # 避免重複爬取已儲存的內容
            existing_urls = get_existing_urls()
            new_links = list(set(filtered_links) - existing_urls)
            logger.info(f"共找到 {len(filtered_links)} 個有效連結，其中 {len(new_links)} 個是新連結")
            
            # 限制初始爬取數量
            return new_links[:self.initial_urls]
            
        except Exception as e:
            logger.error(f"{self.site_name} 獲取連結失敗: {e}")
            
            # 使用新的錯誤處理機制
            self._register_error(
                url=self.start_urls[0] if self.start_urls else "unknown",
                error=e,
                extra_data={"phase": "get_new_links"}
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
        
        async with AsyncWebCrawler(config=self.browser_config) as crawler:
            for attempt in range(self.retry_config["max_retries"]):
                try:
                    # 應用智能流量控制，等待許可
                    wait_time = await self.rate_limiter.wait_for_rate_limit(url)
                    logger.info(f"爬取 URL: {url}，第 {attempt+1} 次嘗試 (等待了 {wait_time:.2f} 秒)")
                    
                    result = await crawler.arun(url=url, config=crawler_config)
                    
                    # 檢查爬取結果
                    if not result or not result.success:
                        error_message = getattr(result, "error_message", "未知錯誤") if result else "無結果返回"
                        status_code = getattr(result, "status_code", None) if result else None
                        logger.warning(f"爬取失敗: {url}，錯誤: {error_message}, 狀態碼: {status_code}")
                        
                        # 如果不是最後一次嘗試，繼續重試
                        if attempt < self.retry_config["max_retries"] - 1:
                            # 根據錯誤類型確定延遲時間
                            error_category = self._classify_error(error_message, status_code)
                            
                            # 根據錯誤類型調整延遲
                            base_delay = self.retry_config["min_delay"]
                            if error_category == ErrorCategory.RATE_LIMIT:
                                base_delay = max(30.0, base_delay * 3)
                            elif error_category == ErrorCategory.SERVER:
                                base_delay = max(10.0, base_delay * 2)
                            
                            delay = random.uniform(
                                base_delay,
                                base_delay * 2
                            )
                            
                            logger.info(f"將在 {delay:.2f} 秒後重試 (錯誤類型: {error_category.value})")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            # 最後一次嘗試也失敗，註冊失敗
                            self._register_error(
                                url=url,
                                error=error_message,
                                status_code=status_code,
                                extra_data={
                                    "phase": "extract_article",
                                    "attempt": attempt + 1
                                }
                            )
                            return None
                    
                    # 爬取成功
                    
                    # 報告成功給流量控制器，可能降低後續延遲
                    self.rate_limiter.report_request_result(url, True)
                    
                    # 處理任務成功
                    mark_task_success(url)
                    
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
                                logger.error(f"內容格式錯誤: {url}")
                                logger.error(f"類型: {type(extracted_content)}, 內容: {extracted_content}")
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
                            
                            # 註冊為解析錯誤，但任務本身仍然算作成功
                            self._register_error(
                                url=url,
                                error=e,
                                extra_data={
                                    "phase": "parse_content",
                                    "is_fatal": False  # 非致命錯誤
                                }
                            )
                    
                    # 從 result.links['internal'] 提取內部連結
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
                    
                    # 報告異常失敗給流量控制器
                    self.rate_limiter.report_request_result(url, False)
                    
                    # 如果不是最後一次嘗試，繼續重試
                    if attempt < self.retry_config["max_retries"] - 1:
                        # 根據錯誤類型確定延遲時間
                        error_category = self._classify_error(e, None)
                        
                        # 根據錯誤類型和失敗次數調整延遲
                        min_delay = self.retry_config["min_delay"]
                        max_delay = self.retry_config["max_delay"]
                        
                        # 根據錯誤類型調整基礎延遲
                        if error_category == ErrorCategory.NETWORK:
                            factor = (attempt + 1) * 1.2
                        elif error_category == ErrorCategory.RATE_LIMIT:
                            factor = (attempt + 1) * 2.0
                        else:
                            factor = (attempt + 1) * 1.5
                            
                        delay = random.uniform(
                            min_delay * factor,
                            max_delay * factor
                        )
                        
                        logger.info(f"將在 {delay:.2f} 秒後重試 (第 {attempt+1} 次失敗, 錯誤類型: {error_category.value})")
                        await asyncio.sleep(delay)
                    else:
                        # 最後一次嘗試也失敗，註冊失敗
                        self._register_error(
                            url=url,
                            error=e,
                            extra_data={
                                "phase": "extract_article",
                                "attempt": attempt + 1
                            }
                        )
            
            # 所有嘗試都失敗
            return None
    
    async def process_retry_queue(self, max_items: int = 5) -> int:
        """
        處理重試隊列中的任務
        
        Args:
            max_items: 一次處理的最大任務數
            
        Returns:
            int: 成功處理的任務數
        """
        # 獲取準備好可以重試的任務
        retry_tasks = get_failure_handler().get_ready_tasks(max_items)
        
        if not retry_tasks:
            logger.info("重試隊列中沒有準備好的任務")
            return 0
        
        logger.info(f"從重試隊列中取得 {len(retry_tasks)} 個任務準備處理")
        
        successful = 0
        for task in retry_tasks:
            logger.info(f"重試任務: {task.url}, 重試次數: {task.retry_count + 1}, 錯誤類別: {task.error_category.value}")
            
            # 重試任務
            result = await self.extract_article(task.url)
            
            if result:
                logger.info(f"重試成功: {task.url}")
                successful += 1
            else:
                logger.warning(f"重試失敗: {task.url}")
        
        return successful
    
    async def run_full_scraper(self) -> bool:
        """
        執行完整的爬蟲流程，進行多層爬取
        
        Returns:
            bool: 爬取是否成功完成
        """
        logger.info(f"開始 {self.site_name} 完整爬蟲")
        seen_urls = set()
        queue = await self.get_new_links()
        
        if not queue:
            logger.warning(f"{self.site_name} 沒有獲取到任何連結，爬蟲終止")
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
                
                # 索引到 Elasticsearch
                bulk_index_articles(df.to_dict(orient="records"))
                logger.info(f"已將 {len(articles)} 篇文章索引到 Elasticsearch")
            
            # 準備下一層爬取
            queue = list(set(next_queue) - seen_urls)
            depth += 1
            
            # 再次處理一些重試任務
            if total_scraped < self.max_pages:
                retry_processed = await self.process_retry_queue(max_items=min(5, self.max_pages - total_scraped))
                total_scraped += retry_processed
        
        # 爬蟲結束後，保存失敗任務狀態
        output_file = OUTPUT_DIR / f"{self.site_name}_failures.json"
        get_failure_handler().save_to_file(str(output_file))
        
        logger.info(f"{self.site_name} 爬蟲完成，共爬取 {total_scraped} 個頁面，深度 {depth}")
        return True

# 工廠函數，根據站點名稱創建對應的爬蟲
def create_crawler(site_name: str) -> BaseCrawler:
    """
    根據站點名稱創建對應的爬蟲實例
    
    Args:
        site_name: 站點名稱
        
    Returns:
        BaseCrawler: 對應的爬蟲實例
    """
    # 這裡可以根據站點名稱返回特定的子類，如果有特殊需求
    return BaseCrawler(site_name)