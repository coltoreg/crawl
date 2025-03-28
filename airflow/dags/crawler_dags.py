"""
爬蟲 Airflow DAG 定義
定義爬蟲的工作流程和排程
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Any, Union

# 將專案根目錄加入 Python 路徑
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# 導入自訂運算子
from airflow.plugins.crawler_operator import CrawlerOperator
from airflow.plugins.sensors import WebsiteMonitorSensor

# 從我們的專案導入模組
# 這裡使用動態導入避免循環引用
import importlib

def load_config() -> Dict[str, Any]:
    """
    載入爬蟲配置
    
    Returns:
        Dict[str, Any]: 站點配置字典
    """
    config_module = importlib.import_module('config')
    return config_module.SITE_CONFIG

def create_site_task(dag: DAG, site_name: str, site_config: Dict[str, Any]) -> PythonOperator:
    """
    為特定站點創建爬蟲任務
    
    Args:
        dag: Airflow DAG實例
        site_name: 站點名稱
        site_config: 站點配置
        
    Returns:
        PythonOperator: 創建的任務運算子
    """
    return CrawlerOperator(
        task_id=f'crawl_{site_name}',
        site_name=site_name,
        site_config=site_config,
        dag=dag,
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=1),
        pool='crawler_pool',
        priority_weight=10 if site_config.get('website_category') == 'news' else 5
    )

def create_monitor_sensor(dag: DAG, site_name: str, site_config: Dict[str, Any]) -> Optional[WebsiteMonitorSensor]:
    """
    為特定站點創建監控感測器
    
    Args:
        dag: Airflow DAG實例
        site_name: 站點名稱
        site_config: 站點配置
        
    Returns:
        Optional[WebsiteMonitorSensor]: 創建的感測器運算子，如果沒有起始URL則返回None
    """
    start_urls = site_config.get('start_urls', [])
    if not start_urls:
        return None
    
    return WebsiteMonitorSensor(
        task_id=f'monitor_{site_name}',
        website_url=start_urls[0],
        poke_interval=300,  # 5分鐘檢查一次
        timeout=1200,  # 20分鐘超時
        mode='reschedule',  # 在檢查間隔釋放工作槽
        soft_fail=True,  # 如果站點無法訪問，不要讓整個任務失敗
        dag=dag
    )

# 基本 DAG 默認參數
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

# 創建新聞類站點的 DAG - 每10分鐘執行
news_dag = DAG(
    'news_crawler',
    default_args=default_args,
    description='每10分鐘爬取新聞類站點',
    schedule_interval='*/10 * * * *',
    catchup=False,
    tags=['crawler', 'news'],
)

# 創建其他類別站點的 DAG - 每小時執行
others_dag = DAG(
    'others_crawler',
    default_args=default_args,
    description='每小時爬取非新聞類站點',
    schedule_interval='0 * * * *',
    catchup=False,
    tags=['crawler', 'others'],
)

# 根據站點類型將站點任務加入相應的DAG
site_configs = load_config()

# 初始化任務字典
news_tasks = {}
others_tasks = {}

# 為每個站點創建任務
for site_name, site_config in site_configs.items():
    category = site_config.get('website_category', 'other')
    
    if category == 'news':
        # 為新聞站點創建監控感測器和爬蟲任務
        monitor_sensor = create_monitor_sensor(news_dag, site_name, site_config)
        crawl_task = create_site_task(news_dag, site_name, site_config)
        
        news_tasks[site_name] = {
            'monitor': monitor_sensor,
            'crawl': crawl_task
        }
        
        # 設定任務依賴關係
        if monitor_sensor:
            monitor_sensor >> crawl_task
            
    else:
        # 為其他站點創建監控感測器和爬蟲任務
        monitor_sensor = create_monitor_sensor(others_dag, site_name, site_config)
        crawl_task = create_site_task(others_dag, site_name, site_config)
        
        others_tasks[site_name] = {
            'monitor': monitor_sensor,
            'crawl': crawl_task
        }
        
        # 設定任務依賴關係
        if monitor_sensor:
            monitor_sensor >> crawl_task

# 完成 DAG 設定，這些變數必須保留在全局作用域中
# 這樣 Airflow 才能發現和解析它們
globals()['news_dag'] = news_dag
globals()['others_dag'] = others_dag
