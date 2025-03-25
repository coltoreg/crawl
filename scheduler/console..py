#!/usr/bin/env python
"""
爬蟲系統控制台模組
提供統一的命令列介面，方便管理爬蟲系統
"""

import os
import sys
import time
import argparse
import logging
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime

# 設定 Python 路徑以便導入上層目錄的模組
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 避免循環引用
import importlib

# 設定日誌
output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(output_dir, "crawler_console.log"))
    ]
)
logger = logging.getLogger("crawler_console")

def get_crawler_manager():
    """
    獲取爬蟲管理器
    
    Returns:
        CrawlerManager: 爬蟲管理器實例
    """
    # 避免循環引用
    crawler_manager_module = importlib.import_module('crawler_manager')
    return crawler_manager_module.get_crawler_manager()

def get_scheduler():
    """
    獲取排程器
    
    Returns:
        CrawlerScheduler: 排程器實例
    """
    # 避免循環引用
    scheduler_module = importlib.import_module('scheduler.scheduler')
    return scheduler_module.get_scheduler()

def init_system():
    """
    初始化系統
    
    Returns:
        bool: 初始化是否成功
    """
    try:
        # 避免循環引用
        main_module = importlib.import_module('main')
        return asyncio.run(main_module.initialize_system())
    except Exception as e:
        logger.error(f"初始化系統失敗: {e}")
        return False

async def crawl_site(site_name: str) -> bool:
    """
    爬取特定站點
    
    Args:
        site_name: 站點名稱
        
    Returns:
        bool: 爬取是否成功
    """
    manager = get_crawler_manager()
    return await manager.run_crawler(site_name)

async def crawl_category(category: str) -> Dict[str, bool]:
    """
    爬取特定類別的所有站點
    
    Args:
        category: 站點類別
        
    Returns:
        Dict[str, bool]: 站點名稱到爬取結果的映射
    """
    manager = get_crawler_manager()
    return await manager.run_category_crawlers(category)

async def crawl_all_sites() -> Dict[str, bool]:
    """
    爬取所有站點
    
    Returns:
        Dict[str, bool]: 站點名稱到爬取結果的映射
    """
    manager = get_crawler_manager()
    return await manager.run_all_crawlers()

def list_sites():
    """列出所有可用站點"""
    # 避免循環引用
    config_module = importlib.import_module('config')
    
    # 按類別分組顯示站點
    sites_by_category = {}
    for site_name, site_config in config_module.SITE_CONFIG.items():
        category = site_config.get("website_category", "other")
        if category not in sites_by_category:
            sites_by_category[category] = []
        sites_by_category[category].append({
            "name": site_name,
            "id": site_config.get("site_id", "N/A")
        })
    
    for category, sites in sites_by_category.items():
        print(f"\n類別: {category}")
        print("=" * (len(category) + 4))
        
        for site in sites:
            print(f"  - {site['name']} (ID: {site['id']})")
    
    total_sites = sum(len(sites) for sites in sites_by_category.values())
    print(f"\n總計 {total_sites} 個站點")

def start_scheduler():
    """啟動排程器"""
    # 避免循環引用
    scheduler_module = importlib.import_module('scheduler.scheduler')
    
    logger.info("啟動排程器")
    scheduler_module.schedule_all_sites()
    logger.info("排程器已啟動")
    
    print("排程器已啟動並在後台運行")
    print("新聞類站點: 每10分鐘爬取一次")
    print("其他類別站點: 每小時爬取一次")
    print("\n使用 Ctrl+C 來停止排程器")
    
    try:
        # 保持主程式運行
        while True:
            time.sleep(60)  # 每分鐘檢查一次
    except KeyboardInterrupt:
        # 收到 Ctrl+C 信號，關閉排程器
        logger.info("接收到停止信號，關閉排程器")
        scheduler = get_scheduler()
        scheduler.shutdown()
        print("排程器已關閉")
        
def stop_scheduler():
    """停止排程器"""
    scheduler = get_scheduler()
    scheduler.shutdown()
    logger.info("排程器已關閉")
    print("排程器已關閉")

def scheduler_status():
    """顯示排程器狀態"""
    scheduler = get_scheduler()
    jobs = scheduler.list_jobs()
    
    if not jobs:
        print("沒有排程任務")
        return
    
    print("排程任務列表:")
    print("============")
    
    for job in jobs:
        print(f"站點: {job['site']}")
        print(f"排程規則: {job['trigger']}")
        print(f"下次執行時間: {job['next_run']}")
        print()
    
    print(f"總計 {len(jobs)} 個排程任務")

def show_stats():
    """顯示爬蟲統計資訊"""
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

def main():
    """主程式入口"""
    # 解析命令列參數
    parser = argparse.ArgumentParser(description="爬蟲系統控制台")
    
    subparsers = parser.add_subparsers(dest="command", help="可用命令")
    
    # init 命令
    init_parser = subparsers.add_parser("init", help="初始化系統")
    
    # crawl 命令
    crawl_parser = subparsers.add_parser("crawl", help="執行爬蟲")
    crawl_parser.add_argument("--site", type=str, help="爬取特定站點")
    crawl_parser.add_argument("--category", type=str, help="爬取特定類別的站點")
    crawl_parser.add_argument("--all", action="store_true", help="爬取所有站點")
    
    # scheduler 命令
    scheduler_parser = subparsers.add_parser("scheduler", help="排程器管理")
    scheduler_parser.add_argument("action", choices=["start", "stop", "status"], help="排程器操作")
    
    # list 命令
    list_parser = subparsers.add_parser("list", help="列出所有站點")
    
    # stats 命令
    stats_parser = subparsers.add_parser("stats", help="顯示爬蟲統計資訊")
    
    # 解析參數
    args = parser.parse_args()
    
    # 執行對應的命令
    if args.command == "init":
        success = init_system()
        print(f"系統初始化{'成功' if success else '失敗'}")
        
    elif args.command == "crawl":
        if args.site:
            # 爬取特定站點
            success = asyncio.run(crawl_site(args.site))
            print(f"爬取站點 {args.site} {'成功' if success else '失敗'}")
            
        elif args.category:
            # 爬取特定類別的站點
            results = asyncio.run(crawl_category(args.category))
            success_count = sum(1 for result in results.values() if result)
            print(f"爬取類別 {args.category} 完成，成功: {success_count}/{len(results)}")
            
        elif args.all:
            # 爬取所有站點
            results = asyncio.run(crawl_all_sites())
            success_count = sum(1 for result in results.values() if result)
            print(f"爬取所有站點完成，成功: {success_count}/{len(results)}")
            
        else:
            parser.error("crawl 命令需要指定 --site, --category 或 --all 選項之一")
    
    elif args.command == "scheduler":
        if args.action == "start":
            start_scheduler()
        elif args.action == "stop":
            stop_scheduler()
        elif args.action == "status":
            scheduler_status()
    
    elif args.command == "list":
        list_sites()
    
    elif args.command == "stats":
        show_stats()
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main()