"""
定時爬取排程器模組
負責管理不同類別網站的定時爬取任務
"""

import os
import sys
import time
import logging
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple, Any, Callable
from concurrent.futures import ThreadPoolExecutor

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

# 設定 Python 路徑以便導入上層目錄的模組
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 避免循環引用
import importlib

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("crawler_scheduler")

class CrawlerScheduler:
    """
    爬蟲排程器，管理定時爬取任務
    """
    
    def __init__(self):
        """初始化排程器"""
        self.scheduler = BackgroundScheduler()
        self.executor = ThreadPoolExecutor(max_workers=3)  # 最多3個同時執行的爬蟲
        self.site_jobs: Dict[str, str] = {}  # 儲存站點名稱與對應的任務ID
        
        # 註冊監聽器以處理任務執行狀態
        self.scheduler.add_listener(self._job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        
        logger.info("排程器初始化完成")
    
    def _job_listener(self, event):
        """
        任務執行狀態監聽器
        
        Args:
            event: 排程器事件
        """
        if hasattr(event, 'exception') and event.exception:
            job = self.scheduler.get_job(event.job_id)
            job_name = job.name if job else event.job_id
            logger.error(f"任務 {job_name} 執行失敗: {event.exception}")
        else:
            job = self.scheduler.get_job(event.job_id)
            job_name = job.name if job else event.job_id
            logger.info(f"任務 {job_name} 執行完成")
    
    def _get_crawler_manager(self):
        """
        動態引入 crawler_manager 以避免循環引用
        
        Returns:
            CrawlerManager: 爬蟲管理器實例
        """
        # 動態引入避免循環引用
        crawler_manager_module = importlib.import_module('crawler_manager')
        return crawler_manager_module.get_crawler_manager()
    
    def _run_crawler(self, site_name: str) -> bool:
        """
        執行特定站點的爬蟲（同步版本）
        
        Args:
            site_name: 站點名稱
            
        Returns:
            bool: 爬取是否成功
        """
        logger.info(f"開始排程爬取站點: {site_name}")
        
        # 使用非同步執行爬蟲
        try:
            manager = self._get_crawler_manager()
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(manager.run_crawler(site_name))
            loop.close()
            
            logger.info(f"站點 {site_name} 爬取{'成功' if result else '失敗'}")
            return result
        except Exception as e:
            logger.error(f"站點 {site_name} 爬取時發生異常: {e}")
            return False
    
    def schedule_site(self, site_name: str, cron_expression: str, replace_existing: bool = True) -> bool:
        """
        排程特定站點的爬取任務
        
        Args:
            site_name: 站點名稱
            cron_expression: Cron 表達式 (例如 "*/10 * * * *" 表示每10分鐘執行一次)
            replace_existing: 如果有現有任務，是否替換
            
        Returns:
            bool: 排程是否成功
        """
        # 檢查站點是否有效
        try:
            # 動態引入避免循環引用
            config_module = importlib.import_module('config')
            if site_name not in config_module.SITE_CONFIG:
                logger.error(f"站點 {site_name} 不存在於配置中")
                return False
        except Exception as e:
            logger.error(f"無法驗證站點 {site_name}: {e}")
            return False
        
        # 如果已存在該站點的任務，根據設定決定是否替換
        if site_name in self.site_jobs:
            if not replace_existing:
                logger.warning(f"站點 {site_name} 已有排程任務，跳過")
                return False
            # 移除現有任務
            old_job_id = self.site_jobs[site_name]
            self.scheduler.remove_job(old_job_id)
            logger.info(f"移除站點 {site_name} 的現有任務")
        
        # 新增任務
        try:
            job = self.scheduler.add_job(
                self._run_crawler,
                CronTrigger.from_crontab(cron_expression),
                args=[site_name],
                id=f"crawler_{site_name}_{int(time.time())}",
                name=f"Crawl {site_name}",
                replace_existing=True,
                max_instances=1,  # 同一個任務同時只能執行一個實例
                misfire_grace_time=600  # 錯過執行時間後的寬限期（秒）
            )
            
            self.site_jobs[job.id] = site_name
            logger.info(f"已為站點 {site_name} 排程爬取任務，使用 Cron 表達式: {cron_expression}")
            return True
            
        except Exception as e:
            logger.error(f"為站點 {site_name} 排程任務時發生錯誤: {e}")
            return False
    
    def schedule_category(self, category: str, cron_expression: str) -> Tuple[int, int]:
        """
        排程特定類別的所有站點
        
        Args:
            category: 站點類別
            cron_expression: Cron 表達式
            
        Returns:
            Tuple[int, int]: 成功排程的站點數和總站點數
        """
        # 動態引入避免循環引用
        config_module = importlib.import_module('config')
        
        # 找出該類別的所有站點
        sites = []
        for site_name, site_config in config_module.SITE_CONFIG.items():
            if site_config.get("website_category") == category:
                sites.append(site_name)
        
        if not sites:
            logger.warning(f"找不到類別為 {category} 的站點")
            return 0, 0
        
        # 為每個站點排程
        success_count = 0
        for site in sites:
            if self.schedule_site(site, cron_expression):
                success_count += 1
        
        logger.info(f"已為類別 {category} 的 {success_count}/{len(sites)} 個站點排程爬取任務")
        return success_count, len(sites)
    
    def remove_site(self, site_name: str) -> bool:
        """
        移除特定站點的排程任務
        
        Args:
            site_name: 站點名稱
            
        Returns:
            bool: 是否成功移除
        """
        # 尋找該站點對應的任務ID
        job_id = None
        for jid, site in self.site_jobs.items():
            if site == site_name:
                job_id = jid
                break
                
        if not job_id:
            logger.warning(f"站點 {site_name} 沒有排程任務")
            return False
        
        try:
            self.scheduler.remove_job(job_id)
            del self.site_jobs[job_id]
            logger.info(f"已移除站點 {site_name} 的排程任務")
            return True
        except Exception as e:
            logger.error(f"移除站點 {site_name} 的排程任務時發生錯誤: {e}")
            return False
    
    def start(self):
        """啟動排程器"""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("排程器已啟動")
        else:
            logger.warning("排程器已經在運行中")
    
    def shutdown(self):
        """關閉排程器"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("排程器已關閉")
        else:
            logger.warning("排程器未運行")
    
    def list_jobs(self) -> List[Dict[str, Any]]:
        """
        列出所有排程任務
        
        Returns:
            List[Dict[str, Any]]: 任務列表
        """
        jobs = []
        for job in self.scheduler.get_jobs():
            # 從任務名稱中提取站點名稱
            site_name = job.name.replace("Crawl ", "")
            
            next_run = job.next_run_time
            next_run_str = next_run.strftime("%Y-%m-%d %H:%M:%S") if next_run else "未排程"
            
            jobs.append({
                "id": job.id,
                "site": site_name,
                "trigger": str(job.trigger),
                "next_run": next_run_str
            })
        
        return jobs
    
    def run_now(self, site_name: str) -> bool:
        """
        立即執行特定站點的爬取任務
        
        Args:
            site_name: 站點名稱
            
        Returns:
            bool: 是否成功執行
        """
        # 檢查站點是否有效
        try:
            # 動態引入避免循環引用
            config_module = importlib.import_module('config')
            if site_name not in config_module.SITE_CONFIG:
                logger.error(f"站點 {site_name} 不存在於配置中")
                return False
        except Exception as e:
            logger.error(f"無法驗證站點 {site_name}: {e}")
            return False
        
        try:
            # 使用執行器來非同步執行爬蟲任務
            logger.info(f"立即執行站點 {site_name} 的爬取任務")
            future = self.executor.submit(self._run_crawler, site_name)
            # 不等待結果，立即返回
            return True
        except Exception as e:
            logger.error(f"立即執行站點 {site_name} 的爬取任務時發生錯誤: {e}")
            return False

# 創建排程器的單例
_scheduler = None

def get_scheduler() -> CrawlerScheduler:
    """
    獲取排程器實例（單例模式）
    
    Returns:
        CrawlerScheduler: 排程器實例
    """
    global _scheduler
    if _scheduler is None:
        _scheduler = CrawlerScheduler()
    return _scheduler

def schedule_all_sites():
    """
    依據類別排程所有站點的爬取任務
    新聞類別: 每10分鐘爬取一次
    其他類別: 每小時爬取一次
    """
    scheduler = get_scheduler()
    
    # 動態引入避免循環引用
    config_module = importlib.import_module('config')
    
    # 收集各類別站點
    categories = {}
    for site_name, site_config in config_module.SITE_CONFIG.items():
        category = site_config.get("website_category", "other")
        if category not in categories:
            categories[category] = []
        categories[category].append(site_name)
    
    # 新聞類別 (每10分鐘)
    if "news" in categories:
        news_sites = categories["news"]
        for site in news_sites:
            scheduler.schedule_site(site, "*/10 * * * *")  # 每10分鐘
        logger.info(f"已排程 {len(news_sites)} 個新聞類站點，每10分鐘爬取一次")
    
    # 其他類別 (每小時)
    for category, sites in categories.items():
        if category != "news":
            for site in sites:
                scheduler.schedule_site(site, "0 * * * *")  # 每小時整點
            logger.info(f"已排程 {len(sites)} 個 {category} 類站點，每小時爬取一次")
    
    # 啟動排程器
    scheduler.start()