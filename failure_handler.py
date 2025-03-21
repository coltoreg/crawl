"""
失敗處理模塊，提供錯誤分類、差異化重試和永久失敗處理
"""

import asyncio
import enum
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple, Callable, Union, Set
from urllib.parse import urlparse
import threading
import random

# 設定日誌
logger = logging.getLogger("failure_handler")

class ErrorCategory(enum.Enum):
    """錯誤類別枚舉"""
    NETWORK = "network_error"       # 網絡錯誤 (連線問題、超時等)
    SERVER = "server_error"         # 服務器錯誤 (5xx 狀態碼)
    CLIENT = "client_error"         # 客戶端錯誤 (4xx 狀態碼)
    RATE_LIMIT = "rate_limit"       # 限流錯誤 (429 狀態碼)
    PARSING = "parsing_error"       # 解析錯誤 (網頁內容解析失敗)
    PERMISSION = "permission_error" # 權限錯誤 (403 狀態碼)
    UNKNOWN = "unknown_error"       # 未知錯誤

@dataclass
class FailedTask:
    """失敗任務數據結構"""
    url: str                                # 任務URL
    site_name: str                          # 站點名稱
    error_category: ErrorCategory           # 錯誤類別
    error_message: str                      # 錯誤訊息
    status_code: Optional[int] = None       # HTTP狀態碼
    retry_count: int = 0                    # 重試次數
    first_failed_at: float = field(default_factory=time.time)  # 首次失敗時間
    last_failed_at: float = field(default_factory=time.time)   # 最近失敗時間
    next_retry_at: Optional[float] = None   # 下次重試時間
    extra_data: Dict[str, Any] = field(default_factory=dict)   # 額外數據

    def to_dict(self) -> Dict[str, Any]:
        """轉換為字典格式"""
        return {
            "url": self.url,
            "site_name": self.site_name,
            "error_category": self.error_category.value,
            "error_message": self.error_message,
            "status_code": self.status_code,
            "retry_count": self.retry_count,
            "first_failed_at": datetime.fromtimestamp(self.first_failed_at).strftime("%Y-%m-%d %H:%M:%S"),
            "last_failed_at": datetime.fromtimestamp(self.last_failed_at).strftime("%Y-%m-%d %H:%M:%S"),
            "next_retry_at": datetime.fromtimestamp(self.next_retry_at).strftime("%Y-%m-%d %H:%M:%S") if self.next_retry_at else None,
            "extra_data": self.extra_data
        }

class RetryPolicy:
    """重試策略類，定義各錯誤類型的重試行為"""
    
    def __init__(
        self,
        max_retries: Dict[ErrorCategory, int] = None,
        base_delays: Dict[ErrorCategory, Tuple[float, float]] = None,
        backoff_factor: float = 2.0,
        jitter: float = 0.1,
        max_delay: float = 3600.0  # 最大延遲 1 小時
    ):
        """
        初始化重試策略
        
        Args:
            max_retries: 各錯誤類型的最大重試次數
            base_delays: 各錯誤類型的基本延遲範圍 (最小值, 最大值)
            backoff_factor: 重試延遲增長因子
            jitter: 隨機波動範圍 (0-1之間的小數)
            max_delay: 最大延遲上限 (秒)
        """
        # 默認最大重試次數配置
        self.max_retries = {
            ErrorCategory.NETWORK: 5,           # 網絡錯誤重試較多次
            ErrorCategory.SERVER: 3,            # 服務器錯誤適當重試
            ErrorCategory.CLIENT: 1,            # 客戶端錯誤少量重試
            ErrorCategory.RATE_LIMIT: 8,        # 限流錯誤多次重試
            ErrorCategory.PARSING: 2,           # 解析錯誤少量重試
            ErrorCategory.PERMISSION: 1,        # 權限錯誤少量重試
            ErrorCategory.UNKNOWN: 3            # 未知錯誤適當重試
        }
        
        # 更新用戶自定義配置
        if max_retries:
            self.max_retries.update(max_retries)
        
        # 默認基本延遲範圍配置 (最小秒數, 最大秒數)
        self.base_delays = {
            ErrorCategory.NETWORK: (5.0, 15.0),         # 網絡錯誤中等延遲
            ErrorCategory.SERVER: (10.0, 30.0),         # 服務器錯誤較長延遲
            ErrorCategory.CLIENT: (5.0, 10.0),          # 客戶端錯誤中等延遲
            ErrorCategory.RATE_LIMIT: (30.0, 60.0),     # 限流錯誤長延遲
            ErrorCategory.PARSING: (5.0, 15.0),         # 解析錯誤中等延遲
            ErrorCategory.PERMISSION: (60.0, 120.0),    # 權限錯誤長延遲
            ErrorCategory.UNKNOWN: (10.0, 20.0)         # 未知錯誤中等延遲
        }
        
        # 更新用戶自定義配置
        if base_delays:
            self.base_delays.update(base_delays)
            
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        self.max_delay = max_delay
    
    def should_retry(self, task: FailedTask) -> bool:
        """
        判斷任務是否應該重試
        
        Args:
            task: 失敗任務
            
        Returns:
            bool: 是否應該重試
        """
        # 檢查重試次數是否已達上限
        max_retry = self.max_retries.get(task.error_category, 3)
        return task.retry_count < max_retry
    
    def calculate_next_retry_time(self, task: FailedTask) -> float:
        """
        計算下次重試時間
        
        Args:
            task: 失敗任務
            
        Returns:
            float: 下次重試的時間戳
        """
        # 獲取基本延遲範圍
        min_delay, max_delay = self.base_delays.get(
            task.error_category, 
            (10.0, 30.0)  # 默認延遲範圍
        )
        
        # 指數退避: 基本延遲 * (退避因子 ^ 重試次數)
        base_delay = random.uniform(min_delay, max_delay)
        delay = base_delay * (self.backoff_factor ** task.retry_count)
        
        # 添加隨機抖動
        jitter_amount = delay * self.jitter
        delay = delay + random.uniform(-jitter_amount, jitter_amount)
        
        # 限制最大延遲
        delay = min(delay, self.max_delay)
        
        # 計算下次重試時間
        return time.time() + delay

class FailureHandler:
    """
    失敗處理器，管理失敗任務的重試和持久化
    """
    
    def __init__(
        self, 
        retry_policy: Optional[RetryPolicy] = None,
        on_permanent_failure: Optional[Callable[[FailedTask], None]] = None
    ):
        """
        初始化失敗處理器
        
        Args:
            retry_policy: 重試策略
            on_permanent_failure: 永久失敗回調函數
        """
        self.retry_policy = retry_policy or RetryPolicy()
        self.on_permanent_failure = on_permanent_failure
        
        # 失敗任務隊列
        self.pending_tasks: Dict[str, FailedTask] = {}  # 等待重試的任務
        self.permanent_failures: Dict[str, FailedTask] = {}  # 永久失敗的任務
        
        # 創建鎖，確保線程安全
        self.lock = threading.RLock()
        
        logger.info("失敗處理器初始化完成")
    
    def classify_error(
        self, 
        error: Exception, 
        status_code: Optional[int] = None
    ) -> ErrorCategory:
        """
        根據錯誤和狀態碼分類錯誤
        
        Args:
            error: 異常對象
            status_code: HTTP狀態碼
            
        Returns:
            ErrorCategory: 錯誤類別
        """
        # 如果有狀態碼，優先根據狀態碼分類
        if status_code is not None:
            if status_code == 429:
                return ErrorCategory.RATE_LIMIT
            elif status_code == 403:
                return ErrorCategory.PERMISSION
            elif 400 <= status_code < 500:
                return ErrorCategory.CLIENT
            elif 500 <= status_code < 600:
                return ErrorCategory.SERVER
        
        # 根據異常類型分類
        error_name = error.__class__.__name__.lower()
        
        # 網絡相關錯誤
        if any(net_err in error_name for net_err in [
            'timeout', 'connection', 'socket', 'ssl', 'dns', 'network'
        ]):
            return ErrorCategory.NETWORK
            
        # 解析相關錯誤
        if any(parse_err in error_name for parse_err in [
            'parse', 'json', 'decode', 'syntax', 'value', 'attribute', 'key', 'index'
        ]):
            return ErrorCategory.PARSING
            
        # 默認為未知錯誤
        return ErrorCategory.UNKNOWN
    
    def register_failure(
        self, 
        url: str,
        site_name: str,
        error: Union[Exception, str],
        status_code: Optional[int] = None,
        extra_data: Optional[Dict[str, Any]] = None
    ) -> FailedTask:
        """
        註冊失敗任務
        
        Args:
            url: 失敗的URL
            site_name: 站點名稱
            error: 錯誤對象或錯誤訊息
            status_code: HTTP狀態碼
            extra_data: 額外數據
            
        Returns:
            FailedTask: 創建的失敗任務對象
        """
        with self.lock:
            # 準備錯誤訊息和分類
            if isinstance(error, Exception):
                error_message = f"{error.__class__.__name__}: {str(error)}"
                error_category = self.classify_error(error, status_code)
            else:
                error_message = str(error)
                # 根據狀態碼分類
                if status_code == 429:
                    error_category = ErrorCategory.RATE_LIMIT
                elif status_code == 403:
                    error_category = ErrorCategory.PERMISSION
                elif status_code and 400 <= status_code < 500:
                    error_category = ErrorCategory.CLIENT
                elif status_code and 500 <= status_code < 600:
                    error_category = ErrorCategory.SERVER
                else:
                    error_category = ErrorCategory.UNKNOWN
            
            # 檢查是否已存在此URL的失敗任務
            if url in self.pending_tasks:
                task = self.pending_tasks[url]
                task.error_category = error_category
                task.error_message = error_message
                task.status_code = status_code
                task.retry_count += 1
                task.last_failed_at = time.time()
                
                if extra_data:
                    task.extra_data.update(extra_data)
                
                logger.info(f"更新失敗任務: {url}, 重試次數: {task.retry_count}, 類別: {error_category.value}")
            else:
                # 創建新的失敗任務
                task = FailedTask(
                    url=url,
                    site_name=site_name,
                    error_category=error_category,
                    error_message=error_message,
                    status_code=status_code,
                    retry_count=0,
                    extra_data=extra_data or {}
                )
                self.pending_tasks[url] = task
                logger.info(f"註冊新失敗任務: {url}, 類別: {error_category.value}")
            
            # 決定是否應該重試
            if self.retry_policy.should_retry(task):
                # 計算下次重試時間
                task.next_retry_at = self.retry_policy.calculate_next_retry_time(task)
                next_retry_time = datetime.fromtimestamp(task.next_retry_at).strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"任務 {url} 將在 {next_retry_time} 重試 (延遲 {task.next_retry_at - time.time():.1f} 秒)")
            else:
                # 永久失敗，從等待隊列移除
                self.pending_tasks.pop(url, None)
                self.permanent_failures[url] = task
                logger.warning(f"任務 {url} 已達最大重試次數 {task.retry_count}，標記為永久失敗")
                
                # 呼叫永久失敗回調
                if self.on_permanent_failure:
                    try:
                        self.on_permanent_failure(task)
                    except Exception as e:
                        logger.error(f"執行永久失敗回調時發生錯誤: {e}")
            
            return task
    
    def get_ready_tasks(self, max_count: int = 10) -> List[FailedTask]:
        """
        獲取準備好可以重試的任務
        
        Args:
            max_count: 最大返回數量
            
        Returns:
            List[FailedTask]: 準備好的任務列表
        """
        with self.lock:
            current_time = time.time()
            ready_tasks = [
                task for task in self.pending_tasks.values()
                if task.next_retry_at and task.next_retry_at <= current_time
            ]
            
            # 按重試時間排序
            ready_tasks.sort(key=lambda t: t.next_retry_at or 0)
            
            # 返回指定數量
            return ready_tasks[:max_count]
    
    def mark_task_success(self, url: str) -> bool:
        """
        標記任務成功完成，從失敗隊列中移除
        
        Args:
            url: 任務URL
            
        Returns:
            bool: 是否成功移除
        """
        with self.lock:
            if url in self.pending_tasks:
                self.pending_tasks.pop(url)
                logger.info(f"任務 {url} 已成功完成，從失敗隊列移除")
                return True
            return False
    
    def save_to_file(self, file_path: str) -> bool:
        """
        將失敗任務保存到文件
        
        Args:
            file_path: 文件路徑
            
        Returns:
            bool: 是否保存成功
        """
        try:
            with self.lock:
                data = {
                    "pending_tasks": {k: v.to_dict() for k, v in self.pending_tasks.items()},
                    "permanent_failures": {k: v.to_dict() for k, v in self.permanent_failures.items()},
                    "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                
            logger.info(f"失敗任務已保存到 {file_path}")
            return True
        except Exception as e:
            logger.error(f"保存失敗任務到文件時發生錯誤: {e}")
            return False
    
    def load_from_file(self, file_path: str) -> bool:
        """
        從文件加載失敗任務
        
        Args:
            file_path: 文件路徑
            
        Returns:
            bool: 是否加載成功
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            with self.lock:
                # 清空現有隊列
                self.pending_tasks.clear()
                self.permanent_failures.clear()
                
                # 載入等待任務
                for url, task_dict in data.get("pending_tasks", {}).items():
                    task = FailedTask(
                        url=task_dict["url"],
                        site_name=task_dict["site_name"],
                        error_category=ErrorCategory(task_dict["error_category"]),
                        error_message=task_dict["error_message"],
                        status_code=task_dict.get("status_code"),
                        retry_count=task_dict["retry_count"],
                        extra_data=task_dict.get("extra_data", {})
                    )
                    
                    # 轉換時間字符串為時間戳
                    if "first_failed_at" in task_dict:
                        dt = datetime.strptime(task_dict["first_failed_at"], "%Y-%m-%d %H:%M:%S")
                        task.first_failed_at = dt.timestamp()
                    
                    if "last_failed_at" in task_dict:
                        dt = datetime.strptime(task_dict["last_failed_at"], "%Y-%m-%d %H:%M:%S")
                        task.last_failed_at = dt.timestamp()
                    
                    if "next_retry_at" in task_dict and task_dict["next_retry_at"]:
                        dt = datetime.strptime(task_dict["next_retry_at"], "%Y-%m-%d %H:%M:%S")
                        task.next_retry_at = dt.timestamp()
                    
                    self.pending_tasks[url] = task
                
                # 載入永久失敗任務
                for url, task_dict in data.get("permanent_failures", {}).items():
                    task = FailedTask(
                        url=task_dict["url"],
                        site_name=task_dict["site_name"],
                        error_category=ErrorCategory(task_dict["error_category"]),
                        error_message=task_dict["error_message"],
                        status_code=task_dict.get("status_code"),
                        retry_count=task_dict["retry_count"],
                        extra_data=task_dict.get("extra_data", {})
                    )
                    
                    # 轉換時間字符串為時間戳
                    if "first_failed_at" in task_dict:
                        dt = datetime.strptime(task_dict["first_failed_at"], "%Y-%m-%d %H:%M:%S")
                        task.first_failed_at = dt.timestamp()
                    
                    if "last_failed_at" in task_dict:
                        dt = datetime.strptime(task_dict["last_failed_at"], "%Y-%m-%d %H:%M:%S")
                        task.last_failed_at = dt.timestamp()
                    
                    self.permanent_failures[url] = task
            
            logger.info(f"從 {file_path} 加載了 {len(self.pending_tasks)} 個等待任務和 {len(self.permanent_failures)} 個永久失敗任務")
            return True
        except Exception as e:
            logger.error(f"從文件加載失敗任務時發生錯誤: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """
        獲取失敗任務統計
        
        Returns:
            Dict[str, Any]: 統計數據
        """
        with self.lock:
            # 按錯誤類別統計
            category_stats = {}
            for cat in ErrorCategory:
                category_stats[cat.value] = {
                    "pending": sum(1 for t in self.pending_tasks.values() if t.error_category == cat),
                    "permanent": sum(1 for t in self.permanent_failures.values() if t.error_category == cat)
                }
            
            # 按站點名稱統計
            site_stats = {}
            all_tasks = list(self.pending_tasks.values()) + list(self.permanent_failures.values())
            for task in all_tasks:
                if task.site_name not in site_stats:
                    site_stats[task.site_name] = {
                        "pending": 0,
                        "permanent": 0
                    }
                
                if task.url in self.pending_tasks:
                    site_stats[task.site_name]["pending"] += 1
                else:
                    site_stats[task.site_name]["permanent"] += 1
            
            return {
                "total_pending": len(self.pending_tasks),
                "total_permanent_failures": len(self.permanent_failures),
                "by_category": category_stats,
                "by_site": site_stats
            }

# 全局單例
_failure_handler: Optional[FailureHandler] = None

def get_failure_handler() -> FailureHandler:
    """
    獲取失敗處理器的全局實例
    
    Returns:
        FailureHandler: 失敗處理器實例
    """
    global _failure_handler
    if _failure_handler is None:
        _failure_handler = FailureHandler()
    return _failure_handler

def register_failure(
    url: str,
    site_name: str,
    error: Union[Exception, str],
    status_code: Optional[int] = None,
    extra_data: Optional[Dict[str, Any]] = None
) -> FailedTask:
    """
    便捷函數：註冊失敗任務
    
    Args:
        url: 失敗的URL
        site_name: 站點名稱
        error: 錯誤對象或錯誤訊息
        status_code: HTTP狀態碼
        extra_data: 額外數據
        
    Returns:
        FailedTask: 創建的失敗任務對象
    """
    return get_failure_handler().register_failure(url, site_name, error, status_code, extra_data)

def mark_task_success(url: str) -> bool:
    """
    便捷函數：標記任務成功完成
    
    Args:
        url: 任務URL
        
    Returns:
        bool: 是否成功移除
    """
    return get_failure_handler().mark_task_success(url)

def get_retry_tasks(max_count: int = 10) -> List[FailedTask]:
    """
    便捷函數：獲取準備好重試的任務
    
    Args:
        max_count: 最大返回數量
        
    Returns:
        List[FailedTask]: 準備好的任務列表
    """
    return get_failure_handler().get_ready_tasks(max_count)