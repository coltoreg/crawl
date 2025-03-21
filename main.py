# main.py
import asyncio
import sys
import os
import logging
import time
import json
import urllib.parse
from typing import List, Set, Optional, Dict, Any, Tuple
from pathlib import Path
from datetime import datetime

from config import SITE_CONFIG, OUTPUT_DIR, KAFKA_CONFIG, RATE_LIMIT_CONFIG
from base_crawler import create_crawler
from kafka_manager import get_consumer, send_crawl_task, get_kafka_manager, close_kafka_connections
from es_manager import get_es_manager, close_es_client
from database import init_database, clear_url_cache, save_permanent_failure
from rate_limiter import get_rate_limiter
from failure_handler import (
    get_failure_handler, 
    ErrorCategory, 
    get_retry_tasks,
    FailedTask
)

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(OUTPUT_DIR / "crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("main")


async def produce_tasks(site: str) -> bool:
    """
    產生指定站台的任務
    
    Args:
        site: 站點名稱
        
    Returns:
        bool: 操作是否成功
    """
    logger.info(f"開始產生 {site} 任務...")
    
    if site not in SITE_CONFIG:
        logger.error(f"找不到站台設定：{site}")
        return False
    
    try:
        # 創建爬蟲實例
        crawler = create_crawler(site)
        
        # 獲取新連結
        links = await crawler.get_new_links()
        if not links:
            logger.warning(f"{site} 沒有取得任務可供發送")
            return False
        
        # 確保任務 topic 存在
        kafka_manager = get_kafka_manager()
        topic = f"{site}Tasks"
        kafka_manager.ensure_topics_exist([topic])
        
        # 發送任務
        success_count = 0
        for link in links:
            logger.info(f"發送任務: {link} 到 topic: {topic}")
            if send_crawl_task({"url": link}, topic):
                success_count += 1
            await asyncio.sleep(1)  # 間隔發送，避免過載
        
        logger.info(f"{site} 任務產生完成，成功發送 {success_count}/{len(links)} 個任務")
        return success_count > 0
        
    except Exception as e:
        logger.error(f"產生 {site} 任務時發生錯誤: {e}", exc_info=True)
        return False


async def kafka_consumer_loop(site: str) -> bool:
    """
    從指定站台的 Kafka 任務佇列中持續接收並處理任務
    
    Args:
        site: 站點名稱
        
    Returns:
        bool: 操作是否成功
    """
    if site not in SITE_CONFIG:
        logger.error(f"找不到站台設定：{site}")
        return False
    
    topic = f"{site}Tasks"
    group_id = KAFKA_CONFIG.get("group_id")
    consumer_timeout_ms = KAFKA_CONFIG.get("consumer_timeout_ms", 3000)
    
    try:
        # 創建爬蟲實例
        crawler = create_crawler(site)
        
        # 獲取消費者
        consumer = get_consumer(topic, group_id)
        logger.info(f"開始從 Kafka 任務佇列接收 {site} 任務 (topic: {topic}, group: {group_id})")
        
        no_message_count = 0  # 連續無訊息的次數
        max_empty_polls = 2   # 最大空輪詢次數 (2 * 3秒 = 6秒無消息則退出)
        
        # 消費消息循環
        while True:
            # 輪詢消息
            message_batch = consumer.poll(timeout_ms=consumer_timeout_ms)
            
            if not message_batch:
                no_message_count += 1
                logger.info(f"{consumer_timeout_ms/1000} 秒內 {site} 無新任務 ({no_message_count}/{max_empty_polls})")
                
                if no_message_count >= max_empty_polls:
                    logger.info(f"{site} 連續 {max_empty_polls * consumer_timeout_ms/1000} 秒無新任務，結束消費")
                    break
                    
                continue
            
            no_message_count = 0  # 收到訊息後重置計數器
            
            # 處理收到的消息
            for tp, messages in message_batch.items():
                for msg in messages:
                    task = msg.value
                    if not task:
                        continue
                        
                    logger.info(f"[{site}] 收到任務: {task}")
                    
                    # 執行完整爬蟲
                    try:
                        logger.info(f"開始執行 {site} 的完整爬蟲流程...")
                        await crawler.run_full_scraper()
                        logger.info(f"{site} 爬蟲完成")
                        # 執行一次完整爬蟲後退出
                        consumer.close()
                        return True
                    except Exception as e:
                        logger.error(f"執行 {site} 完整爬蟲時發生錯誤: {e}", exc_info=True)
        
        # 正常關閉消費者
        consumer.close()
        return True
        
    except Exception as e:
        logger.error(f"處理 {site} Kafka 任務時發生錯誤: {e}", exc_info=True)
        return False


def get_all_sites() -> List[str]:
    """
    獲取配置中的所有站點名稱
    
    Returns:
        List[str]: 站點名稱列表
    """
    return list(SITE_CONFIG.keys())


async def produce_tasks_all() -> bool:
    """
    同時產生所有站台的任務
    
    Returns:
        bool: 是否至少有一個站點成功
    """
    sites = get_all_sites()
    if not sites:
        logger.error("未發現任何站台設定")
        return False
        
    logger.info(f"開始產生所有站台任務: {sites}")
    tasks = [produce_tasks(site) for site in sites]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 檢查結果
    success_count = sum(1 for r in results if r is True)
    logger.info(f"任務產生完成，{success_count}/{len(sites)} 個站點成功")
    
    return success_count > 0


async def kafka_consumer_loop_all() -> bool:
    """
    同時從 Kafka 消費所有站台的任務
    
    Returns:
        bool: 是否至少有一個站點成功
    """
    sites = get_all_sites()
    if not sites:
        logger.error("未發現任何站台設定")
        return False
        
    logger.info(f"開始同時從 Kafka 消費所有站台任務: {sites}")
    tasks = [kafka_consumer_loop(site) for site in sites]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 檢查結果
    success_count = sum(1 for r in results if r is True)
    logger.info(f"消費任務完成，{success_count}/{len(sites)} 個站點成功")
    
    return success_count > 0


async def direct_crawl(site: str) -> bool:
    """
    直接執行指定站台的爬蟲，不通過 Kafka
    
    Args:
        site: 站點名稱
        
    Returns:
        bool: 操作是否成功
    """
    if site not in SITE_CONFIG:
        logger.error(f"找不到站台設定：{site}")
        return False
    
    try:
        logger.info(f"開始直接執行 {site} 爬蟲...")
        crawler = create_crawler(site)
        success = await crawler.run_full_scraper()
        
        if success:
            logger.info(f"{site} 爬蟲完成")
        else:
            logger.warning(f"{site} 爬蟲未完全成功")
            
        return success
    except Exception as e:
        logger.error(f"執行 {site} 爬蟲時發生錯誤: {e}", exc_info=True)
        return False


async def direct_crawl_all() -> bool:
    """
    直接執行所有站台的爬蟲，不通過 Kafka
    
    Returns:
        bool: 是否至少有一個站點成功
    """
    sites = get_all_sites()
    if not sites:
        logger.error("未發現任何站台設定")
        return False
        
    logger.info(f"開始直接爬取所有站台: {sites}")
    tasks = [direct_crawl(site) for site in sites]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 檢查結果
    success_count = sum(1 for r in results if r is True)
    logger.info(f"所有爬蟲完成，{success_count}/{len(sites)} 個站點成功")
    
    return success_count > 0


async def show_rate_limit_status() -> None:
    """
    顯示當前流量控制狀態
    
    Returns:
        None
    """
    try:
        rate_limiter = get_rate_limiter()
        stats = rate_limiter.get_domain_stats()
        
        if not stats:
            print("目前沒有任何網域的流量控制紀錄")
            return
        
        print("==== 流量控制狀態 ====")
        print(f"全局限制: {rate_limiter.global_rate_limit} 請求/分鐘")
        print(f"當前時間窗請求數: {len(rate_limiter.global_request_timestamps)}")
        print(f"默認域名延遲: {rate_limiter.default_domain_delay}秒")
        print("\n各域名狀態:")
        
        for domain, data in stats.items():
            if isinstance(data, dict):  # 確保是域名數據而非錯誤訊息
                print(f"\n域名: {domain}")
                print(f"  總請求數: {data['total_requests']}")
                print(f"  成功請求數: {data['total_success']}")
                print(f"  成功率: {data['success_rate']:.2f}%")
                print(f"  當前延遲: {data['current_delay']:.2f}秒")
                print(f"  是否被限流: {'是' if data['is_throttled'] else '否'}")
                if data['throttled_until']:
                    print(f"  限流解除時間: {data['throttled_until']}")
    
    except Exception as e:
        logger.error(f"顯示流量控制狀態時發生錯誤: {e}", exc_info=True)
        print(f"發生錯誤: {e}")


async def process_retry_tasks(site: Optional[str] = None, max_items: int = 10) -> bool:
    """
    處理指定站點的重試任務
    
    Args:
        site: 站點名稱，若為 None 則處理所有站點
        max_items: 最大處理數量
        
    Returns:
        bool: 是否有任務成功處理
    """
    try:
        # 獲取準備好可以重試的任務
        tasks = get_retry_tasks(max_items)
        
        if not tasks:
            logger.info("沒有準備好的重試任務")
            return False
            
        # 如果指定了站點，則過濾任務
        if site:
            tasks = [task for task in tasks if task.site_name == site]
            
            if not tasks:
                logger.info(f"{site} 沒有準備好的重試任務")
                return False
        
        logger.info(f"開始處理 {len(tasks)} 個重試任務")
        
        # 按站點分組任務
        tasks_by_site = {}
        for task in tasks:
            if task.site_name not in tasks_by_site:
                tasks_by_site[task.site_name] = []
            tasks_by_site[task.site_name].append(task)
        
        # 對每個站點的任務批量處理
        for site_name, site_tasks in tasks_by_site.items():
            if site_name not in SITE_CONFIG:
                logger.warning(f"重試任務中發現未知站點: {site_name}")
                continue
                
            # 創建站點爬蟲
            crawler = create_crawler(site_name)
            
            # 批量處理每個URL
            for task in site_tasks:
                logger.info(f"重試任務: {task.url} (站點: {site_name}, 重試次數: {task.retry_count + 1})")
                
                try:
                    # 使用爬蟲的擷取文章方法
                    result = await crawler.extract_article(task.url)
                    
                    if result:
                        logger.info(f"重試成功: {task.url}")
                    else:
                        logger.warning(f"重試仍然失敗: {task.url}")
                except Exception as e:
                    logger.error(f"重試任務 {task.url} 時發生錯誤: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"處理重試任務時發生錯誤: {e}", exc_info=True)
        return False


async def show_failure_stats() -> None:
    """
    顯示失敗任務統計
    
    Returns:
        None
    """
    try:
        failure_handler = get_failure_handler()
        stats = failure_handler.get_stats()
        
        print("\n==== 失敗任務統計 ====")
        print(f"等待重試任務數: {stats['total_pending']}")
        print(f"永久失敗任務數: {stats['total_permanent_failures']}")
        
        # 各錯誤類型統計
        print("\n錯誤類型統計:")
        for category, counts in stats['by_category'].items():
            if counts['pending'] > 0 or counts['permanent'] > 0:
                print(f"  {category}: 等待重試 {counts['pending']}，永久失敗 {counts['permanent']}")
        
        # 各站點統計
        print("\n站點統計:")
        for site, counts in stats['by_site'].items():
            if counts['pending'] > 0 or counts['permanent'] > 0:
                print(f"  {site}: 等待重試 {counts['pending']}，永久失敗 {counts['permanent']}")
        
    except Exception as e:
        logger.error(f"顯示失敗任務統計時發生錯誤: {e}", exc_info=True)
        print(f"發生錯誤: {e}")


def on_permanent_failure(task: FailedTask) -> None:
    """
    永久失敗任務的回調函數
    
    Args:
        task: 失敗任務對象
    """
    try:
        # 記錄永久失敗任務到數據庫
        save_permanent_failure(
            url=task.url,
            error_category=task.error_category.value,
            error_message=task.error_message,
            status_code=task.status_code,
            site_name=task.site_name,
            retry_count=task.retry_count,
            extra_data=task.extra_data
        )
        logger.warning(f"永久失敗任務已記錄: {task.url}, 站點: {task.site_name}, 類別: {task.error_category.value}")
    except Exception as e:
        logger.error(f"處理永久失敗任務時發生錯誤: {e}")


async def init_system() -> bool:
    """
    初始化系統：檢查並創建所需的資料庫、Kafka topics 等
    
    Returns:
        bool: 初始化是否成功
    """
    logger.info("開始系統初始化...")
    success = True
    
    # 初始化資料庫
    if not init_database():
        logger.error("資料庫初始化失敗")
        success = False
    
    # 初始化 Kafka topics
    kafka_manager = get_kafka_manager()
    topics_to_create = [KAFKA_CONFIG.get("result_topic", "CrawlResults")]
    
    # 為每個站點創建任務 topic
    for site in SITE_CONFIG:
        topics_to_create.append(f"{site}Tasks")
    
    if not kafka_manager.ensure_topics_exist(topics_to_create):
        logger.error("Kafka topics 初始化失敗")
        success = False
    
    # 初始化 Elasticsearch 索引
    es_manager = get_es_manager()
    if not es_manager.create_index_if_not_exists():
        logger.error("Elasticsearch 索引初始化失敗")
        success = False
    
    # 初始化流量控制系統
    try:
        rate_limiter = get_rate_limiter()
        # 配置全局流量控制參數
        for key, value in RATE_LIMIT_CONFIG.items():
            setattr(rate_limiter, key, value)
        
        # 從站點配置中載入域名級別延遲設定
        for site_name, site_config in SITE_CONFIG.items():
            if "start_urls" in site_config and site_config["start_urls"] and "domain_delay" in site_config:
                domain = urllib.parse.urlparse(site_config["start_urls"][0]).netloc
                rate_limiter.set_domain_delay(domain, site_config["domain_delay"])
                logger.info(f"為 {domain} 設定域名延遲: {site_config['domain_delay']}秒")
        
        logger.info("流量控制系統初始化完成")
    except Exception as e:
        logger.error(f"初始化流量控制系統時發生錯誤: {e}")
        success = False
    
    # 初始化失敗處理器
    try:
        failure_handler = get_failure_handler()
        # 設置永久失敗回調
        failure_handler.on_permanent_failure = on_permanent_failure
        
        # 嘗試從文件加載之前的失敗任務
        failure_file = OUTPUT_DIR / "failure_tasks.json"
        if failure_file.exists():
            if failure_handler.load_from_file(str(failure_file)):
                logger.info(f"從 {failure_file} 載入了失敗任務")
            else:
                logger.warning(f"無法從 {failure_file} 載入失敗任務")
        
        logger.info("失敗處理系統初始化完成")
    except Exception as e:
        logger.error(f"初始化失敗處理系統時發生錯誤: {e}")
        success = False
    
    # 清除 URL 快取
    clear_url_cache()
    
    if success:
        logger.info("系統初始化完成")
    else:
        logger.warning("系統初始化完成，但部分組件可能未成功初始化")
    
    return success


async def main():
    """
    主程式入口，支援多種模式
    
    使用方式:
      python main.py --init                # 初始化系統
      python main.py --produce site        # 產生指定站台任務
      python main.py --consumer site       # 消費並處理指定站台任務
      python main.py --produce-all         # 產生所有站台任務
      python main.py --consumer-all        # 消費並處理所有站台任務
      python main.py --crawl site          # 直接爬取指定站台
      python main.py --crawl-all           # 直接爬取所有站台
      python main.py --rate-status         # 顯示流量控制狀態
      python main.py --retry site          # 處理指定站台的重試任務
      python main.py --retry-all           # 處理所有站台的重試任務
      python main.py --failure-stats       # 顯示失敗任務統計
    """
    try:
        if len(sys.argv) < 2:
            print("使用方式:")
            print("  python main.py --init                # 初始化系統")
            print("  python main.py --produce site        # 產生指定站台任務")
            print("  python main.py --consumer site       # 消費並處理指定站台任務")
            print("  python main.py --produce-all         # 產生所有站台任務")
            print("  python main.py --consumer-all        # 消費並處理所有站台任務")
            print("  python main.py --crawl site          # 直接爬取指定站台")
            print("  python main.py --crawl-all           # 直接爬取所有站台")
            print("  python main.py --rate-status         # 顯示流量控制狀態")
            print("  python main.py --retry site          # 處理指定站台的重試任務")
            print("  python main.py --retry-all           # 處理所有站台的重試任務")
            print("  python main.py --failure-stats       # 顯示失敗任務統計")
            return

        mode = sys.argv[1].lower()

        # 初始化系統
        if mode == "--init":
            await init_system()
        # 產生指定站台任務
        elif mode == "--produce":
            if len(sys.argv) < 3:
                print("請指定站台名稱")
                return
            site = sys.argv[2].lower()
            await produce_tasks(site)
        # 消費並處理指定站台任務
        elif mode == "--consumer":
            if len(sys.argv) < 3:
                print("請指定站台名稱")
                return
            site = sys.argv[2].lower()
            await kafka_consumer_loop(site)
        # 產生所有站台任務
        elif mode == "--produce-all":
            await produce_tasks_all()
        # 消費並處理所有站台任務
        elif mode == "--consumer-all":
            await kafka_consumer_loop_all()
        # 直接爬取指定站台
        elif mode == "--crawl":
            if len(sys.argv) < 3:
                print("請指定站台名稱")
                return
            site = sys.argv[2].lower()
            await direct_crawl(site)
        # 直接爬取所有站台
        elif mode == "--crawl-all":
            await direct_crawl_all()
        # 顯示流量控制狀態
        elif mode == "--rate-status":
            await show_rate_limit_status()
        # 處理指定站台的重試任務
        elif mode == "--retry":
            if len(sys.argv) < 3:
                print("請指定站台名稱")
                return
            site = sys.argv[2].lower()
            await process_retry_tasks(site)
        # 處理所有站台的重試任務
        elif mode == "--retry-all":
            await process_retry_tasks()
        # 顯示失敗任務統計
        elif mode == "--failure-stats":
            await show_failure_stats()
        else:
            print("不支援的模式，請使用 --init, --produce, --consumer, --produce-all, --consumer-all, --crawl, --crawl-all, --rate-status, --retry, --retry-all 或 --failure-stats")
            
    except Exception as e:
        logger.error(f"執行過程中發生錯誤: {e}", exc_info=True)
    finally:
        # 確保資源正確釋放
        close_kafka_connections()
        close_es_client()
        
        # 保存失敗任務狀態
        try:
            failure_file = OUTPUT_DIR / "failure_tasks.json"
            get_failure_handler().save_to_file(str(failure_file))
            logger.info(f"失敗任務已保存到 {failure_file}")
        except Exception as e:
            logger.error(f"保存失敗任務時發生錯誤: {e}")
        
        
if __name__ == "__main__":
    asyncio.run(main())