"""
爬蟲管理控制台
提供統一的爬蟲任務管理介面
"""

import os
import sys
import json
import asyncio
import logging
import argparse
from typing import List, Dict, Any, Optional, Set
from datetime import datetime, timedelta
from pathlib import Path

# 本地模組
from config import SITE_CONFIG, OUTPUT_DIR
from base_crawler import create_crawler, batch_crawl
from database import init_database, get_crawl_stats, clear_url_cache
from utils import CrawlerStats

# 設定日誌
logger = logging.getLogger("crawler_manager")

class CrawlerManager:
    """爬蟲管理器，提供統一的爬蟲管理介面"""
    
    def __init__(self):
        """初始化爬蟲管理器"""
        self.site_names = list(SITE_CONFIG.keys())
        logger.info(f"已載入 {len(self.site_names)} 個站點配置")
        
        # 確保輸出目錄存在
        OUTPUT_DIR.mkdir(exist_ok=True)
        
        # 初始化資料庫
        init_database()
    
    async def run_crawler(self, site_name: str) -> bool:
        """
        執行單個站點的爬蟲
        
        Args:
            site_name: 站點名稱
            
        Returns:
            bool: 爬取是否成功完成
        """
        if site_name not in self.site_names:
            logger.error(f"找不到站點配置: {site_name}")
            return False
        
        logger.info(f"開始執行 {site_name} 爬蟲")
        crawler = create_crawler(site_name)
        
        try:
            result = await crawler.run_full_scraper()
            
            # 匯出爬蟲報告
            report = crawler.stats.generate_report()
            report_path = OUTPUT_DIR / f"{site_name}_report.json"
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            
            logger.info(f"{site_name} 爬蟲執行完成，結果: {'成功' if result else '失敗'}")
            return result
        except Exception as e:
            logger.error(f"{site_name} 爬蟲執行異常: {e}")
            return False
    
    async def run_all_crawlers(self) -> Dict[str, bool]:
        """
        執行所有站點的爬蟲
        
        Returns:
            Dict[str, bool]: 站點名稱到爬取結果的映射
        """
        logger.info(f"開始執行所有站點 ({len(self.site_names)}) 的爬蟲")
        return await batch_crawl(self.site_names)
    
    async def run_category_crawlers(self, category: str) -> Dict[str, bool]:
        """
        執行指定類別的所有站點爬蟲
        
        Args:
            category: 網站類別
            
        Returns:
            Dict[str, bool]: 站點名稱到爬取結果的映射
        """
        sites = [
            site for site in self.site_names 
            if SITE_CONFIG[site].get("website_category") == category
        ]
        
        if not sites:
            logger.warning(f"找不到類別為 {category} 的站點")
            return {}
        
        logger.info(f"開始執行類別 {category} 的爬蟲，共 {len(sites)} 個站點")
        return await batch_crawl(sites)
    
    async def run_incremental_crawl(self, site_name: str, days: int = 1) -> bool:
        """
        執行增量爬取，只爬取最近指定天數的內容
        
        Args:
            site_name: 站點名稱
            days: 天數
            
        Returns:
            bool: 爬取是否成功完成
        """
        # TODO: 實現增量爬取邏輯
        logger.warning("增量爬取功能尚未實現")
        return False
    
    def get_stats(self) -> Dict[str, Any]:
        """
        獲取爬蟲統計數據
        
        Returns:
            Dict[str, Any]: 統計數據
        """
        return get_crawl_stats()
    
    def list_sites(self) -> List[Dict[str, Any]]:
        """
        列出所有站點資訊
        
        Returns:
            List[Dict[str, Any]]: 站點資訊列表
        """
        sites = []
        for name, config in SITE_CONFIG.items():
            sites.append({
                "name": name,
                "category": config.get("website_category", "unknown"),
                "site_id": config.get("site_id", 0),
                "start_urls": config.get("start_urls", []),
                "domain_delay": config.get("domain_delay", 3.0)
            })
        return sites
    
    def clear_cache(self) -> None:
        """清除快取"""
        clear_url_cache()
        logger.info("已清除 URL 快取")


# 單例模式
_crawler_manager = None

def get_crawler_manager() -> CrawlerManager:
    """
    獲取爬蟲管理器實例（單例模式）
    
    Returns:
        CrawlerManager: 爬蟲管理器實例
    """
    global _crawler_manager
    if _crawler_manager is None:
        _crawler_manager = CrawlerManager()
    return _crawler_manager


async def main():
    """
    主程式入口
    
    使用方式:
      python crawler_manager.py --site udn                   # 爬取特定站點
      python crawler_manager.py --all                        # 爬取所有站點
      python crawler_manager.py --category news              # 爬取特定類別的站點
      python crawler_manager.py --list                       # 列出所有站點
      python crawler_manager.py --stats                      # 顯示爬蟲統計
      python crawler_manager.py --clear-cache                # 清除快取
    """
    parser = argparse.ArgumentParser(description="爬蟲管理控制台")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--site", type=str, help="爬取特定站點")
    group.add_argument("--all", action="store_true", help="爬取所有站點")
    group.add_argument("--category", type=str, help="爬取特定類別的站點")
    group.add_argument("--list", action="store_true", help="列出所有站點")
    group.add_argument("--stats", action="store_true", help="顯示爬蟲統計")
    group.add_argument("--clear-cache", action="store_true", help="清除快取")
    
    args = parser.parse_args()
    manager = get_crawler_manager()
    
    if args.site:
        await manager.run_crawler(args.site)
    elif args.all:
        results = await manager.run_all_crawlers()
        print(f"爬蟲執行結果:\n" + "\n".join([f"{site}: {'成功' if result else '失敗'}" for site, result in results.items()]))
    elif args.category:
        results = await manager.run_category_crawlers(args.category)
        print(f"類別 {args.category} 爬蟲執行結果:\n" + "\n".join([f"{site}: {'成功' if result else '失敗'}" for site, result in results.items()]))
    elif args.list:
        sites = manager.list_sites()
        print("站點列表:")
        for site in sites:
            print(f"  - {site['name']} (ID: {site['site_id']}, 類別: {site['category']})")
    elif args.stats:
        stats = manager.get_stats()
        print(f"爬蟲統計:")
        print(f"  總文章數: {stats.get('total_articles', 0)}")
        print(f"  失敗 URL 數: {stats.get('total_failed', 0)}")
        print(f"  按站點統計:")
        for site, count in stats.get('by_site', {}).items():
            print(f"    - {site}: {count} 篇")
    elif args.clear_cache:
        manager.clear_cache()
        print("已清除 URL 快取")


if __name__ == "__main__":
    # 設定日誌
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(OUTPUT_DIR / "crawler_manager.log"),
            logging.StreamHandler()
        ]
    )
    
    asyncio.run(main())