import asyncio
import json
import re
import pandas as pd
import random
from datetime import datetime
from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher
from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    CacheMode,
    RateLimiter,
    JsonCssExtractionStrategy
)
from config import BROWSER_CONFIG, SIMULATION_CONFIG, RETRY_CONFIG, get_random_delay
from database import save_failed_url, save_to_mysql, get_existing_urls
from kafka_manager import send_crawl_result
from es_manager import bulk_index_articles

# 設定最大爬取深度（最多 3 層）
MAX_DEPTH = 3  
# 設定最多爬取 100 篇新聞
MAX_PAGES = 100  


async def get_new_links() -> list | None:
    """
    爬取首頁連結，並過濾符合格式的 URL。

    Returns:
        list | None:
            - 成功獲取連結時，回傳符合格式的 URL 清單。
            - 爬取失敗時，回傳 None。
    """
    browser_config = BrowserConfig(**BROWSER_CONFIG)

    run_config = CrawlerRunConfig(cache_mode=CacheMode.BYPASS, **SIMULATION_CONFIG)
    
    dispatcher = MemoryAdaptiveDispatcher(
        memory_threshold_percent=90,
        check_interval=60.0,
        max_session_permit=5,
        rate_limiter=RateLimiter(
            base_delay=(1.0, 4.0),
            max_delay=10.0,
            max_retries=2
        )
    )

    start_urls = [
        "https://www.ettoday.net/",
        "https://www.ettoday.net/news/news-list.htm",
        "https://star.ettoday.net/",
        "https://house.ettoday.net/",
        "https://speed.ettoday.net/",
    ]
    
    all_links = set()

    try:
        async with AsyncWebCrawler(config=browser_config) as crawler:
            results = await crawler.arun_many(urls=start_urls, config=run_config, dispatcher=dispatcher)

            for result in results:
                if not result.success:
                    save_failed_url(result.url, result.error_message, getattr(result, "status_code", "Unknown"))
                    continue

                # 取得內部連結
                page_links = [item["href"] for item in result.links['internal']]
                all_links.update(page_links)

        # 過濾符合格式的連結
        filtered_links = list(set(line.split('?')[0] for line in all_links if "news/" in line))

        # 避免重複爬取已儲存的連結
        existing_urls = get_existing_urls()
        new_links = list(set(filtered_links) - set(existing_urls))
        print(f"共找到 {len(new_links)} 篇連結")

        return new_links[0:3]  # 初始最多抓 10 篇連結
    except Exception as e:
        save_failed_url("udn", str(e), "Unknown")
        return None


async def extract_article(url: str) -> dict | None:
    """
    解析網頁內容，並提取下一層的內部網頁連結。

    Args:
        url (str): 文章的 URL。

    Returns:
        dict | None:
            - 成功解析 -> 返回 `title`（來自 metadata）、`content`、`next_level_links` 的字典。
            - 解析失敗 -> 返回 `None`，並記錄錯誤。
    """
    browser_config = BrowserConfig(**BROWSER_CONFIG)

    crawler_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        simulate_user=False,
        **SIMULATION_CONFIG,
        extraction_strategy=JsonCssExtractionStrategy(
            schema={
                "name": "News Article",
                "baseSelector": "article",
                "fields": [
                    {"name": "content", "selector": "div > div.story", "type": "text", "multiple": True}
                ]
            }
        )
    )

    async with AsyncWebCrawler(config=browser_config) as crawler:
        for attempt in range(RETRY_CONFIG["max_retries"]):
            try:
                result = await crawler.arun(url=url, config=crawler_config)

                if result and result.success:
                    try:
                        article = json.loads(result.extracted_content)
                        if isinstance(article, list) and article:
                            article = article[0]
                        if not isinstance(article, dict):
                            print(f"內容格式錯誤: {url}")
                            print(f"article type: {type(article)}, content: {article}")
                    except json.JSONDecodeError:
                        continue
                    article["url"] = url
                    article["title"] = result.metadata.get("title", "")
                    article["description"] = result.metadata.get("description", "")
                    article["keywords"] = result.metadata.get("keywords", "")
                    article["scraped_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    # **從 result.links['internal'] 提取內部連結**
                    raw_links = [item["href"] for item in result.links['internal']]
                    valid_links = list(set(line.split('?')[0] for line in raw_links if "news/story/" in line))
                    article["next_level_links"] = valid_links

                    return article

            except Exception as e:
                print(f"failed_reason: {str(e)}")
                print(f"爬取失敗 (嘗試 {attempt+1}/{RETRY_CONFIG['max_retries']}): {e}")
                save_failed_url(url, str(e), "Unknown")

            await asyncio.sleep(random.uniform(RETRY_CONFIG["min_delay"], RETRY_CONFIG["max_delay"]))
            return None


async def run_full_scraper():
    """
    遞迴爬取連結，最多爬 3 層，每次最多 100 個連結。
    """
    seen_urls = set()
    queue = await get_new_links()
    depth = 0
    total_scraped = 0

    while queue and depth < MAX_DEPTH and total_scraped < MAX_PAGES:
        print(f"爬取第 {depth+1} 層，共 {len(queue)} 條連結")
        next_queue = []
        articles = []

        for link in queue:
            if link in seen_urls or total_scraped >= MAX_PAGES:
                continue
            seen_urls.add(link)

            article = await extract_article(link)
            if article:
                send_crawl_result(article)
                articles.append(article)
                total_scraped += 1
                next_queue.extend(article.get("next_level_links", []))

            if total_scraped >= MAX_PAGES:
                break

            await asyncio.sleep(get_random_delay())

        if articles:
            df = pd.DataFrame(articles)
            df = df.drop("next_level_links",axis=1)
            df["website_category"] = "news"
            df["site"] = "ettoday"
            df["site_id"] = 5
            df.to_csv(f"Desktop/work/crawl_project/output/ettoday_news_depth_{depth+1}.csv", index=False)
            save_to_mysql(df)
            print('結束將數據存在DB')
            bulk_index_articles(df.to_dict(orient="records"))

        queue = list(set(next_queue) - seen_urls)
        depth += 1

    print(f"爬取結束，最多爬取 {total_scraped} 個頁面")


