"""
流量控制模組，提供域名級別和全局級別的爬取頻率限制
"""

import time
import logging
import asyncio
import urllib.parse
from typing import Dict, Optional, Tuple, List, Set, Any
import threading
from datetime import datetime, timedelta
from dataclasses import dataclass
from collections import defaultdict

# 設定日誌
logger = logging.getLogger("rate_limiter")

@dataclass
class DomainStatus:
    """存儲域名狀態的資料類"""
    last_access: float = 0.0  # 上次訪問時間戳
    failure_count: int = 0  # 連續失敗次數
    success_count: int = 0  # 連續成功次數
    current_delay: float = 0.0  # 當前延遲時間 (秒)
    total_requests: int = 0  # 總請求數
    total_success: int = 0  # 總成功數
    is_throttled: bool = False  # 是否被限流
    throttled_until: Optional[datetime] = None  # 限流解除時間


class RateLimiterManager:
    """
    流量控制管理器，提供域名級別和全局級別的爬取頻率限制
    """
    
    def __init__(
        self, 
        default_domain_delay: float = 3.0,
        min_domain_delay: float = 1.0,
        max_domain_delay: float = 20.0,
        global_rate_limit: int = 10,  # 每分鐘最大請求數
        global_time_window: int = 60,  # 全局限流時間窗口 (秒)
        failure_backoff_factor: float = 2.0,  # 失敗後增加延遲的倍數
        success_recovery_factor: float = 0.8,  # 成功後減少延遲的倍數
        max_failures_before_throttle: int = 5,  # 觸發限流的連續失敗次數
        throttle_duration_minutes: int = 5  # 限流持續時間 (分鐘)
    ):
        """
        初始化流量控制管理器
        
        Args:
            default_domain_delay: 默認域名延遲 (秒)
            min_domain_delay: 最小域名延遲 (秒)
            max_domain_delay: 最大域名延遲 (秒)
            global_rate_limit: 全局每分鐘最大請求數
            global_time_window: 全局限流時間窗口 (秒)
            failure_backoff_factor: 失敗後增加延遲的倍數
            success_recovery_factor: 成功後減少延遲的倍數
            max_failures_before_throttle: 觸發限流的連續失敗次數
            throttle_duration_minutes: 限流持續時間 (分鐘)
        """
        # 基本配置
        self.default_domain_delay = default_domain_delay
        self.min_domain_delay = min_domain_delay
        self.max_domain_delay = max_domain_delay
        self.global_rate_limit = global_rate_limit
        self.global_time_window = global_time_window
        self.failure_backoff_factor = failure_backoff_factor
        self.success_recovery_factor = success_recovery_factor
        self.max_failures_before_throttle = max_failures_before_throttle
        self.throttle_duration_minutes = throttle_duration_minutes
        
        # 域名狀態追蹤
        self.domain_status: Dict[str, DomainStatus] = defaultdict(DomainStatus)
        
        # 域名自定義延遲設定
        self.domain_delay_settings: Dict[str, float] = {}
        
        # 全局請求時間窗口
        self.global_request_timestamps: List[float] = []
        
        # 線程鎖，確保線程安全
        self.lock = threading.RLock()
        
        logger.info(f"流量控制管理器初始化: 默認延遲={default_domain_delay}秒, "
                   f"全局限制={global_rate_limit}請求/分鐘")
    
    def extract_domain(self, url: str) -> str:
        """
        從 URL 中提取域名
        
        Args:
            url: 完整的 URL
            
        Returns:
            str: 提取的域名
        """
        parsed = urllib.parse.urlparse(url)
        return parsed.netloc
    
    def set_domain_delay(self, domain: str, delay: float) -> None:
        """
        為特定域名設定延遲值
        
        Args:
            domain: 域名
            delay: 延遲秒數
        """
        with self.lock:
            self.domain_delay_settings[domain] = delay
            logger.info(f"已為 {domain} 設定自定義延遲: {delay}秒")
    
    def set_domain_delays_from_config(self, config: Dict[str, Dict[str, Any]]) -> None:
        """
        從配置中批量設定域名延遲
        
        Args:
            config: 站點配置字典，格式如 {site_name: {domain_delay: 5.0, ...}, ...}
        """
        for site_name, site_config in config.items():
            if "domain_delay" in site_config:
                # 從 start_urls 中提取域名
                if "start_urls" in site_config and site_config["start_urls"]:
                    domain = self.extract_domain(site_config["start_urls"][0])
                    self.set_domain_delay(domain, site_config["domain_delay"])
    
    def get_domain_delay(self, domain: str) -> float:
        """
        獲取域名的當前延遲設定
        
        Args:
            domain: 域名
            
        Returns:
            float: 當前延遲秒數
        """
        with self.lock:
            # 優先使用自定義設定
            if domain in self.domain_delay_settings:
                return self.domain_delay_settings[domain]
            
            # 返回默認延遲
            return self.default_domain_delay
    
    def is_domain_throttled(self, domain: str) -> bool:
        """
        檢查域名是否正在被限流
        
        Args:
            domain: 域名
            
        Returns:
            bool: 是否被限流
        """
        with self.lock:
            status = self.domain_status[domain]
            
            # 如果設置了限流且尚未到期
            if status.is_throttled and status.throttled_until:
                if datetime.now() < status.throttled_until:
                    time_left = (status.throttled_until - datetime.now()).total_seconds()
                    logger.warning(f"{domain} 正在限流中，還剩 {time_left:.1f} 秒")
                    return True
                else:
                    # 限流時間已過，重置狀態
                    logger.info(f"{domain} 限流已解除")
                    status.is_throttled = False
                    status.throttled_until = None
                    status.failure_count = 0
            
            return False
    
    def update_domain_status(self, domain: str, success: bool) -> None:
        """
        更新域名狀態，根據請求成功或失敗調整延遲
        
        Args:
            domain: 域名
            success: 請求是否成功
        """
        with self.lock:
            status = self.domain_status[domain]
            status.total_requests += 1
            
            if success:
                status.total_success += 1
                status.success_count += 1
                status.failure_count = 0
                
                # 成功則適當減少延遲，但不低於最小值
                if status.current_delay > 0:
                    status.current_delay = max(
                        self.min_domain_delay,
                        status.current_delay * self.success_recovery_factor
                    )
            else:
                status.failure_count += 1
                status.success_count = 0
                
                # 失敗則增加延遲，但不超過最大值
                if status.current_delay <= 0:
                    status.current_delay = self.get_domain_delay(domain)
                else:
                    status.current_delay = min(
                        self.max_domain_delay,
                        status.current_delay * self.failure_backoff_factor
                    )
                
                # 檢查是否需要進入限流狀態
                if status.failure_count >= self.max_failures_before_throttle:
                    throttle_until = datetime.now() + timedelta(minutes=self.throttle_duration_minutes)
                    status.is_throttled = True
                    status.throttled_until = throttle_until
                    logger.warning(f"{domain} 已連續失敗 {status.failure_count} 次，"
                                 f"限流至 {throttle_until.strftime('%Y-%m-%d %H:%M:%S')}")
    
    def check_global_rate_limit(self) -> Tuple[bool, Optional[float]]:
        """
        檢查全局速率限制，確定是否需要等待
        
        Returns:
            Tuple[bool, Optional[float]]: (是否需要等待, 需要等待的秒數)
        """
        with self.lock:
            current_time = time.time()
            
            # 移除時間窗口外的記錄
            cutoff_time = current_time - self.global_time_window
            self.global_request_timestamps = [t for t in self.global_request_timestamps if t > cutoff_time]
            
            # 檢查是否達到限流閾值
            if len(self.global_request_timestamps) >= self.global_rate_limit:
                # 計算需要等待的時間
                oldest_timestamp = min(self.global_request_timestamps)
                wait_time = (oldest_timestamp + self.global_time_window) - current_time
                if wait_time > 0:
                    logger.info(f"全局限流生效，需要等待 {wait_time:.2f} 秒")
                    return True, wait_time
            
            # 記錄本次請求時間戳
            self.global_request_timestamps.append(current_time)
            return False, None
    
    async def wait_for_rate_limit(self, url: str) -> float:
        """
        等待流量控制限制，結合域名級別和全局級別的限制
        
        Args:
            url: 請求的完整 URL
            
        Returns:
            float: 實際等待的秒數
        """
        domain = self.extract_domain(url)
        
        # 檢查域名是否被限流
        if self.is_domain_throttled(domain):
            status = self.domain_status[domain]
            # 如果被限流，等待限流解除
            if status.throttled_until:
                wait_time = (status.throttled_until - datetime.now()).total_seconds()
                if wait_time > 0:
                    logger.warning(f"{domain} 被限流，等待 {wait_time:.2f} 秒")
                    await asyncio.sleep(wait_time)
                    return wait_time
        
        with self.lock:
            # 計算域名級別限制
            current_time = time.time()
            status = self.domain_status[domain]
            domain_delay = max(status.current_delay, self.get_domain_delay(domain))
            
            # 計算上次訪問後的間隔
            elapsed = current_time - status.last_access
            domain_wait = max(0, domain_delay - elapsed)
            
            # 檢查全局限流
            global_limited, global_wait = self.check_global_rate_limit()
            
            # 取較長的等待時間
            wait_time = max(domain_wait, global_wait or 0)
            
            if wait_time > 0:
                logger.info(f"等待 {wait_time:.2f} 秒 (域名延遲: {domain_wait:.2f}秒, "
                           f"全局延遲: {global_wait or 0:.2f}秒)")
                await asyncio.sleep(wait_time)
            
            # 更新最後訪問時間
            status.last_access = time.time()
            
            return wait_time
    
    def report_request_result(self, url: str, success: bool, status_code: Optional[int] = None) -> None:
        """
        報告請求結果，用於調整流量控制參數
        
        Args:
            url: 請求的 URL
            success: 請求是否成功
            status_code: HTTP 狀態碼 (可選)
        """
        domain = self.extract_domain(url)
        self.update_domain_status(domain, success)
        
        # 如果收到特定狀態碼 (如 429, 403)，可能表示被反爬
        if status_code in {403, 429, 503}:
            logger.warning(f"{domain} 返回狀態碼 {status_code}，可能被反爬，增加延遲並標記失敗")
            self.update_domain_status(domain, False)  # 再標記一次失敗，加速限流
    
    def get_domain_stats(self, domain: Optional[str] = None) -> Dict[str, Any]:
        """
        獲取域名狀態統計
        
        Args:
            domain: 指定域名，若為 None 則返回所有域名統計
            
        Returns:
            Dict[str, Any]: 域名狀態統計
        """
        with self.lock:
            if domain:
                if domain in self.domain_status:
                    status = self.domain_status[domain]
                    return {
                        "total_requests": status.total_requests,
                        "total_success": status.total_success,
                        "success_rate": (status.total_success / status.total_requests * 100) if status.total_requests else 0,
                        "current_delay": status.current_delay,
                        "is_throttled": status.is_throttled,
                        "throttled_until": status.throttled_until.strftime('%Y-%m-%d %H:%M:%S') if status.throttled_until else None
                    }
                return {"error": f"域名 {domain} 未找到"}
            
            # 返回所有域名統計
            return {
                domain: {
                    "total_requests": status.total_requests,
                    "total_success": status.total_success,
                    "success_rate": (status.total_success / status.total_requests * 100) if status.total_requests else 0,
                    "current_delay": status.current_delay,
                    "is_throttled": status.is_throttled,
                    "throttled_until": status.throttled_until.strftime('%Y-%m-%d %H:%M:%S') if status.throttled_until else None
                }
                for domain, status in self.domain_status.items()
            }
    
    def clear_stats(self, domain: Optional[str] = None) -> None:
        """
        清除統計數據
        
        Args:
            domain: 指定域名，若為 None 則清除所有域名統計
        """
        with self.lock:
            if domain:
                if domain in self.domain_status:
                    self.domain_status[domain] = DomainStatus()
                    logger.info(f"已清除 {domain} 的統計數據")
            else:
                self.domain_status.clear()
                self.global_request_timestamps.clear()
                logger.info("已清除所有統計數據")


# 全局單例
_rate_limiter: Optional[RateLimiterManager] = None

def get_rate_limiter() -> RateLimiterManager:
    """
    獲取流量控制管理器實例 (單例模式)
    
    Returns:
        RateLimiterManager: 流量控制管理器實例
    """
    global _rate_limiter
    if not _rate_limiter:
        _rate_limiter = RateLimiterManager()
    return _rate_limiter