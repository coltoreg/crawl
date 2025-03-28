"""
自訂爬蟲運算子模組
提供 Airflow 運算子來執行爬蟲任務
"""

import os
import sys
import time
import asyncio
import logging
from typing import Dict, List, Optional, Set, Any, Union

# 將專案根目錄加入 Python 路徑
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# 設定日誌
logger = logging.getLogger("crawler_operator")

class CrawlerOperator(BaseOperator):
    """
    爬蟲運算子，用於在 Airflow 中執行爬蟲任務
    """
    
    @apply_defaults
    def __init__(
        self,
        site_name: str,
        site_config: Dict[str, Any],
        *args,
        **kwargs
    ) -> None:
        """
        初始化爬蟲運算子
        
        Args:
            site_name: 站點名稱
            site_config: 站點配置
            *args: 傳遞給基礎運算子的參數
            **kwargs: 傳遞給基礎運算子的關鍵字參數
        """
        super().__init__(*args, **kwargs)
        self.site_name = site_name
        self.site_config = site_config
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        執行爬蟲任務
        
        Args:
            context: Airflow任務上下文
            
        Returns:
            Dict[str, Any]: 任務執行結果
        """
        import importlib
        
        logger.info(f"開始執行站點 {self.site_name} 的爬蟲任務")
        
        # 1. 獲取爬蟲管理器
        manager_module = importlib.import_module('crawler_manager')
        manager = manager_module.get_crawler_manager()
        
        # 2. 執行爬蟲任務
        try:
            # 使用新的事件循環執行非同步任務
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # 執行爬蟲
            result = loop.run_until_complete(manager.run_crawler(self.site_name))
            loop.close()
            
            if result:
                logger.info(f"站點 {self.site_name} 爬蟲任務執行成功")
            else:
                logger.warning(f"站點 {self.site_name} 爬蟲任務執行失敗")
            
            # 3. 記錄執行結果和指標
            execution_time = context.get('ti').duration
            xcom_push_key = f"crawler_result_{self.site_name}"
            
            # 創建返回值
            return_value = {
                "site_name": self.site_name,
                "success": result,
                "execution_time": execution_time,
                "execution_date": context.get('execution_date').isoformat(),
                "task_id": context.get('task_instance').task_id,
                "dag_id": context.get('task_instance').dag_id,
            }
            
            # 將結果推送到 XCom 以便其他任務使用
            context.get('ti').xcom_push(key=xcom_push_key, value=return_value)
            
            return return_value
            
        except Exception as e:
            logger.error(f"站點 {self.site_name} 爬蟲任務執行時發生異常: {e}")
            # 重新拋出異常，讓 Airflow 可以捕獲和處理
            raise
