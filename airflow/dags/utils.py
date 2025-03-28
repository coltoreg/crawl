"""
Airflow DAG 輔助函數模組
提供創建和管理 DAG 的工具函數
"""

import os
import sys
import logging
from typing import Dict, List, Optional, Set, Any, Union, Callable

# 將專案根目錄加入 Python 路徑
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from airflow.models import Variable
from airflow.hooks.base import BaseHook

# 設定日誌
logger = logging.getLogger("airflow_utils")

def get_site_config_from_variable(site_name: str) -> Optional[Dict[str, Any]]:
    """
    從 Airflow Variable 獲取站點配置
    
    Args:
        site_name: 站點名稱
        
    Returns:
        Optional[Dict[str, Any]]: 站點配置，如果不存在則返回 None
    """
    try:
        # 嘗試從 Airflow Variable 獲取配置
        variable_name = f"site_config_{site_name}"
        site_config_json = Variable.get(variable_name, default_var=None)
        
        if site_config_json:
            import json
            return json.loads(site_config_json)
        
        # 如果 Variable 中沒有，則從專案配置中獲取
        import importlib
        config_module = importlib.import_module('config')
        
        if site_name in config_module.SITE_CONFIG:
            return config_module.SITE_CONFIG[site_name]
        
        return None
    except Exception as e:
        logger.error(f"獲取站點 {site_name} 配置時發生錯誤: {e}")
        return None

def update_site_config_to_variable(site_name: str, site_config: Dict[str, Any]) -> bool:
    """
    更新站點配置到 Airflow Variable
    
    Args:
        site_name: 站點名稱
        site_config: 站點配置
        
    Returns:
        bool: 更新是否成功
    """
    try:
        import json
        variable_name = f"site_config_{site_name}"
        Variable.set(variable_name, json.dumps(site_config))
        logger.info(f"站點 {site_name} 配置已更新到 Airflow Variable")
        return True
    except Exception as e:
        logger.error(f"更新站點 {site_name} 配置到 Airflow Variable 時發生錯誤: {e}")
        return False

def register_site_configs_to_variables() -> int:
    """
    將所有站點配置註冊到 Airflow Variable
    
    Returns:
        int: 成功註冊的站點數量
    """
    try:
        import importlib
        config_module = importlib.import_module('config')
        
        count = 0
        for site_name, site_config in config_module.SITE_CONFIG.items():
            if update_site_config_to_variable(site_name, site_config):
                count += 1
        
        logger.info(f"成功將 {count} 個站點配置註冊到 Airflow Variable")
        return count
    except Exception as e:
        logger.error(f"註冊站點配置到 Airflow Variable 時發生錯誤: {e}")
        return 0

def get_database_connection() -> Optional[Dict[str, Any]]:
    """
    獲取數據庫連接信息
    
    Returns:
        Optional[Dict[str, Any]]: 數據庫連接信息，如果不存在則返回 None
    """
    try:
        # 先嘗試從 Airflow Connection 獲取
        connection = BaseHook.get_connection('crawler_db')
        if connection:
            return {
                'host': connection.host,
                'user': connection.login,
                'password': connection.password,
                'database': connection.schema,
            }
        
        # 如果 Connection 中沒有，則從專案配置中獲取
        import importlib
        config_module = importlib.import_module('config')
        
        return config_module.DB_CONFIG
    except Exception as e:
        logger.error(f"獲取數據庫連接信息時發生錯誤: {e}")
        return None

def run_async_in_sync(async_func: Callable, *args, **kwargs) -> Any:
    """
    在同步環境中執行非同步函數
    
    Args:
        async_func: 要執行的非同步函數
        *args: 傳遞給非同步函數的參數
        **kwargs: 傳遞給非同步函數的關鍵字參數
        
    Returns:
        Any: 非同步函數的返回值
    """
    import asyncio
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        return loop.run_until_complete(async_func(*args, **kwargs))
    finally:
        loop.close()
