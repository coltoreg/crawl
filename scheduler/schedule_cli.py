#!/usr/bin/env python
"""
爬蟲排程器命令列介面
提供管理排程任務的命令列工具
"""

import os
import sys
import time
import argparse
import logging
from typing import List, Dict, Any
from tabulate import tabulate

# 設定 Python 路徑以便導入上層目錄的模組
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 引入排程器模組
from scheduler.scheduler import get_scheduler, schedule_all_sites

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("scheduler_cli")

def start_command(args):
    """
    啟動排程器
    
    Args:
        args: 命令行參數
    """
    scheduler = get_scheduler()
    
    # 如果沒有任務，先排程所有站點
    if not scheduler.list_jobs():
        logger.info("未發現已排程的任務，自動排程所有站點")
        schedule_all_sites()
    else:
        scheduler.start()
    
    print("排程器已啟動")
    print("排程器將在後台運行，使用 'python scheduler/schedule_cli.py status' 查看狀態")

def stop_command(args):
    """
    停止排程器
    
    Args:
        args: 命令行參數
    """
    scheduler = get_scheduler()
    scheduler.shutdown()
    print("排程器已停止")

def status_command(args):
    """
    顯示排程器狀態
    
    Args:
        args: 命令行參數
    """
    scheduler = get_scheduler()
    jobs = scheduler.list_jobs()
    
    if not jobs:
        print("沒有排程任務")
        return
    
    # 格式化顯示任務列表
    table_data = []
    for job in jobs:
        table_data.append([
            job["site"],
            job["trigger"],
            job["next_run"]
        ])
    
    print(tabulate(
        table_data,
        headers=["站點", "排程規則", "下次執行時間"],
        tablefmt="pretty"
    ))
    
    print(f"\n總計 {len(jobs)} 個排程任務")

def list_sites_command(args):
    """
    列出所有可用的站點
    
    Args:
        args: 命令行參數
    """
    # 避免循環引用
    import importlib
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
        
        table_data = []
        for site in sites:
            table_data.append([site["name"], site["id"]])
        
        print(tabulate(
            table_data,
            headers=["站點名稱", "站點ID"],
            tablefmt="simple"
        ))
    
    total_sites = sum(len(sites) for sites in sites_by_category.values())
    print(f"\n總計 {total_sites} 個站點")

def schedule_site_command(args):
    """
    排程特定站點
    
    Args:
        args: 命令行參數
    """
    site_name = args.site
    cron_expr = args.cron
    
    scheduler = get_scheduler()
    success = scheduler.schedule_site(site_name, cron_expr)
    
    if success:
        print(f"已成功為站點 {site_name} 排程爬取任務")
        print(f"排程規則: {cron_expr}")
    else:
        print(f"為站點 {site_name} 排程任務失敗")

def schedule_category_command(args):
    """
    排程特定類別的所有站點
    
    Args:
        args: 命令行參數
    """
    category = args.category
    cron_expr = args.cron
    
    scheduler = get_scheduler()
    success_count, total = scheduler.schedule_category(category, cron_expr)
    
    if success_count > 0:
        print(f"已成功為類別 {category} 的 {success_count}/{total} 個站點排程爬取任務")
        print(f"排程規則: {cron_expr}")
    else:
        print(f"為類別 {category} 排程任務失敗")

def schedule_all_command(args):
    """
    排程所有站點
    
    Args:
        args: 命令行參數
    """
    logger.info("開始為所有站點排程爬取任務")
    schedule_all_sites()
    print("已完成所有站點的排程設置")
    print("新聞類站點: 每10分鐘爬取一次")
    print("其他類別站點: 每小時爬取一次")

def run_now_command(args):
    """
    立即執行指定站點的爬取任務
    
    Args:
        args: 命令行參數
    """
    site_name = args.site
    
    scheduler = get_scheduler()
    success = scheduler.run_now(site_name)
    
    if success:
        print(f"已啟動站點 {site_name} 的爬取任務")
        print("任務正在後台執行，可檢查日誌獲取詳情")
    else:
        print(f"啟動站點 {site_name} 的爬取任務失敗")

def remove_site_command(args):
    """
    移除特定站點的排程任務
    
    Args:
        args: 命令行參數
    """
    site_name = args.site
    
    scheduler = get_scheduler()
    success = scheduler.remove_site(site_name)
    
    if success:
        print(f"已成功移除站點 {site_name} 的排程任務")
    else:
        print(f"移除站點 {site_name} 的排程任務失敗")

def main():
    """命令列主函數"""
    # 創建命令列解析器
    parser = argparse.ArgumentParser(description="爬蟲排程器管理工具")
    subparsers = parser.add_subparsers(dest="command", help="可用命令")
    
    # start 命令
    start_parser = subparsers.add_parser("start", help="啟動排程器")
    start_parser.set_defaults(func=start_command)
    
    # stop 命令
    stop_parser = subparsers.add_parser("stop", help="停止排程器")
    stop_parser.set_defaults(func=stop_command)
    
    # status 命令
    status_parser = subparsers.add_parser("status", help="顯示排程器狀態")
    status_parser.set_defaults(func=status_command)
    
    # list-sites 命令
    list_sites_parser = subparsers.add_parser("list-sites", help="列出所有可用站點")
    list_sites_parser.set_defaults(func=list_sites_command)
    
    # schedule-site 命令
    schedule_site_parser = subparsers.add_parser("schedule-site", help="排程特定站點")
    schedule_site_parser.add_argument("site", help="站點名稱")
    schedule_site_parser.add_argument("cron", help="Cron 表達式（例如: '*/10 * * * *' 表示每10分鐘）")
    schedule_site_parser.set_defaults(func=schedule_site_command)
    
    # schedule-category 命令
    schedule_category_parser = subparsers.add_parser("schedule-category", help="排程特定類別的所有站點")
    schedule_category_parser.add_argument("category", help="站點類別")
    schedule_category_parser.add_argument("cron", help="Cron 表達式")
    schedule_category_parser.set_defaults(func=schedule_category_command)
    
    # schedule-all 命令
    schedule_all_parser = subparsers.add_parser("schedule-all", help="排程所有站點")
    schedule_all_parser.set_defaults(func=schedule_all_command)
    
    # run-now 命令
    run_now_parser = subparsers.add_parser("run-now", help="立即執行指定站點的爬取任務")
    run_now_parser.add_argument("site", help="站點名稱")
    run_now_parser.set_defaults(func=run_now_command)
    
    # remove-site 命令
    remove_site_parser = subparsers.add_parser("remove-site", help="移除特定站點的排程任務")
    remove_site_parser.add_argument("site", help="站點名稱")
    remove_site_parser.set_defaults(func=remove_site_command)
    
    # 解析命令行參數
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        return
    
    # 執行對應的命令
    args.func(args)

if __name__ == "__main__":
    main()