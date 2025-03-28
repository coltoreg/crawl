"""
爬蟲主程式
提供爬蟲系統的主要執行入口和命令列介面
"""

import asyncio
import sys
import logging
import time
import json
import argparse
import os
import subprocess
from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime

# 本地模組
from config import OUTPUT_DIR
from base_crawler import create_crawler, batch_crawl
from database import init_database, clear_url_cache
from crawler_manager import get_crawler_manager
from es_manager import get_es_manager, close_es_client
from kafka_manager import get_kafka_manager, close_kafka_connections

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


async def initialize_system() -> bool:
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
    topics_to_create = [
        "CrawlTasks",
        "CrawlResults"
    ]
    
    # 為每個站點創建任務 topic
    manager = get_crawler_manager()
    for site in manager.site_names:
        topics_to_create.append(f"{site}Tasks")
    
    if not kafka_manager.ensure_topics_exist(topics_to_create):
        logger.error("Kafka topics 初始化失敗")
        success = False
    
    # 初始化 Elasticsearch 索引
    es_manager = get_es_manager()
    if not es_manager.create_index_if_not_exists():
        logger.error("Elasticsearch 索引初始化失敗")
        success = False
    
    # 清除 URL 快取
    clear_url_cache()
    
    # 檢查 Airflow 是否已安裝
    try:
        # 嘗試執行 airflow 命令
        result = subprocess.run(
            ["airflow", "version"], 
            capture_output=True, 
            text=True, 
            check=False
        )
        if result.returncode == 0:
            logger.info(f"已檢測到 Airflow: {result.stdout.strip()}")
            # 檢查 Airflow DAG 路徑
            airflow_dags_dir = os.environ.get("AIRFLOW_HOME", "airflow") + "/dags"
            if not os.path.exists(airflow_dags_dir):
                logger.warning(f"Airflow DAGs 目錄不存在: {airflow_dags_dir}")
                logger.info("請執行 scripts/setup-airflow.sh 來設置 Airflow 環境")
            else:
                logger.info(f"Airflow DAGs 目錄: {airflow_dags_dir}")
        else:
            logger.warning("未檢測到 Airflow，排程功能將無法使用")
            logger.info("請執行 scripts/setup-airflow.sh 來設置 Airflow 環境")
    except FileNotFoundError:
        logger.warning("未檢測到 Airflow，排程功能將無法使用")
        logger.info("請執行 scripts/setup-airflow.sh 來設置 Airflow 環境")
    
    if success:
        logger.info("系統初始化完成")
    else:
        logger.warning("系統初始化完成，但部分組件可能未成功初始化")
    
    return success


async def crawl_site(site_name: str) -> bool:
    """
    爬取指定站點的內容
    
    Args:
        site_name: 站點名稱
        
    Returns:
        bool: 爬取是否成功
    """
    manager = get_crawler_manager()
    return await manager.run_crawler(site_name)


async def crawl_all_sites() -> Dict[str, bool]:
    """
    爬取所有站點的內容
    
    Returns:
        Dict[str, bool]: 站點名稱到爬取結果的映射
    """
    manager = get_crawler_manager()
    return await manager.run_all_crawlers()


async def crawl_category(category: str) -> Dict[str, bool]:
    """
    爬取指定類別的站點內容
    
    Args:
        category: 站點類別
        
    Returns:
        Dict[str, bool]: 站點名稱到爬取結果的映射
    """
    manager = get_crawler_manager()
    return await manager.run_category_crawlers(category)


def list_sites() -> None:
    """
    列出所有支援的站點
    """
    manager = get_crawler_manager()
    sites = manager.list_sites()
    
    print("支援的站點列表:")
    print("===============")
    
    # 按類別分組
    sites_by_category = {}
    for site in sites:
        category = site["category"]
        if category not in sites_by_category:
            sites_by_category[category] = []
        sites_by_category[category].append(site)
    
    # 輸出分類列表
    for category, category_sites in sites_by_category.items():
        print(f"\n分類: {category}")
        print("-" * (8 + len(category)))
        for site in category_sites:
            print(f"  - {site['name']} (ID: {site['site_id']})")
            print(f"    起始 URL: {site['start_urls'][0] if site['start_urls'] else 'None'}")
            print(f"    請求延遲: {site['domain_delay']} 秒")
            print()


def show_stats() -> None:
    """
    顯示爬蟲統計資訊
    """
    manager = get_crawler_manager()
    stats = manager.get_stats()
    
    print("\n爬蟲系統統計")
    print("============")
    print(f"總文章數: {stats.get('total_articles', 0):,}")
    print(f"失敗 URL 數: {stats.get('total_failed', 0):,}")
    
    print("\n按站點統計:")
    for site, count in stats.get('by_site', {}).items():
        print(f"  - {site}: {count:,} 篇")
    
    print("\n按類別統計:")
    for category, count in stats.get('by_category', {}).items():
        print(f"  - {category}: {count:,} 篇")
    
    print("\n最近爬取統計:")
    for date, count in list(stats.get('by_date', {}).items())[:10]:
        print(f"  - {date}: {count:,} 篇")


def check_airflow_status() -> Dict[str, Any]:
    """
    檢查 Airflow 狀態
    
    Returns:
        Dict[str, Any]: Airflow 狀態信息
    """
    status = {
        "installed": False,
        "running": False,
        "webserver_url": None,
        "version": None,
        "dags": []
    }
    
    try:
        # 檢查 Airflow 是否已安裝
        version_result = subprocess.run(
            ["airflow", "version"], 
            capture_output=True, 
            text=True, 
            check=False
        )
        
        if version_result.returncode == 0:
            status["installed"] = True
            status["version"] = version_result.stdout.strip()
            
            # 檢查 Airflow Webserver 是否運行
            ps_result = subprocess.run(
                ["ps", "-ef"], 
                capture_output=True, 
                text=True, 
                check=False
            )
            
            if "airflow webserver" in ps_result.stdout:
                status["running"] = True
                status["webserver_url"] = "http://localhost:8080"
            
            # 獲取 DAG 列表
            dags_result = subprocess.run(
                ["airflow", "dags", "list", "--output", "json"], 
                capture_output=True, 
                text=True, 
                check=False
            )
            
            if dags_result.returncode == 0:
                try:
                    status["dags"] = json.loads(dags_result.stdout.strip())
                except json.JSONDecodeError:
                    status["dags"] = []
    except Exception as e:
        logger.error(f"檢查 Airflow 狀態時發生錯誤: {e}")
    
    return status


async def main():
    """
    主程式入口
    
    使用方法:
      python main.py --init                 # 初始化系統
      python main.py --site udn             # 爬取特定站點
      python main.py --all                  # 爬取所有站點
      python main.py --category news        # 爬取特定類別的站點
      python main.py --list                 # 列出所有站點
      python main.py --stats                # 顯示統計資訊
      python main.py --clear-cache          # 清除URL快取
      python main.py --airflow-status       # 檢查Airflow狀態
    """
    parser = argparse.ArgumentParser(description="爬蟲系統主程式")
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--init", action="store_true", help="初始化系統")
    group.add_argument("--site", type=str, help="爬取特定站點")
    group.add_argument("--all", action="store_true", help="爬取所有站點")
    group.add_argument("--category", type=str, help="爬取特定類別的站點")
    group.add_argument("--list", action="store_true", help="列出所有站點")
    group.add_argument("--stats", action="store_true", help="顯示統計資訊")
    group.add_argument("--clear-cache", action="store_true", help="清除URL快取")
    group.add_argument("--airflow-status", action="store_true", help="檢查Airflow狀態")
    
    args = parser.parse_args()
    
    try:
        start_time = time.time()
        
        if args.init:
            success = await initialize_system()
            print(f"系統初始化{'成功' if success else '失敗'}")
            
        elif args.site:
            success = await crawl_site(args.site)
            print(f"爬取 {args.site} {'成功' if success else '失敗'}")
            
        elif args.all:
            results = await crawl_all_sites()
            success_count = sum(1 for result in results.values() if result)
            print(f"爬取所有站點完成，成功: {success_count}/{len(results)}")
            
        elif args.category:
            results = await crawl_category(args.category)
            success_count = sum(1 for result in results.values() if result)
            print(f"爬取 {args.category} 類別完成，成功: {success_count}/{len(results)}")
            
        elif args.list:
            list_sites()
            
        elif args.stats:
            show_stats()
            
        elif args.clear_cache:
            clear_url_cache()
            print("URL 快取已清除")
            
        elif args.airflow_status:
            status = check_airflow_status()
            print("\nAirflow 狀態")
            print("============")
            print(f"已安裝: {'是' if status['installed'] else '否'}")
            
            if status['installed']:
                print(f"版本: {status['version']}")
                print(f"運行中: {'是' if status['running'] else '否'}")
                
                if status['running']:
                    print(f"Webserver URL: {status['webserver_url']}")
                
                if status['dags']:
                    print("\nDAG 列表:")
                    for dag in status['dags']:
                        dag_id = dag.get('dag_id', 'Unknown')
                        is_paused = dag.get('is_paused', True)
                        print(f"  - {dag_id} {'(已暫停)' if is_paused else '(運行中)'}")
                else:
                    print("\n沒有可用的 DAG 或無法獲取 DAG 列表")
                    
                print("\n使用以下命令管理 Airflow:")
                print("  啟動 Webserver: airflow webserver -D")
                print("  啟動 Scheduler: airflow scheduler -D")
                print("  啟用 DAG: airflow dags unpause [dag_id]")
            else:
                print("\n請執行以下命令來設置 Airflow:")
                print("  ./scripts/setup-airflow.sh")
        
        end_time = time.time()
        print(f"總執行時間: {end_time - start_time:.2f} 秒")
        
    except Exception as e:
        logger.error(f"執行過程中發生錯誤: {e}", exc_info=True)
        print(f"錯誤: {e}")
        
    finally:
        # 確保資源正確釋放
        close_kafka_connections()
        close_es_client()


if __name__ == "__main__":
    asyncio.run(main())