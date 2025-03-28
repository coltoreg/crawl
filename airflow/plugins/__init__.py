"""
Airflow 插件初始化模組
註冊自訂運算子和感測器
"""

from airflow.plugins_manager import AirflowPlugin
from crawler_operator import CrawlerOperator
from sensors import WebsiteMonitorSensor, ContentChangeSensor

class CrawlerPlugin(AirflowPlugin):
    """爬蟲插件，將自訂運算子和感測器註冊到 Airflow"""
    
    name = "crawler_plugin"
    operators = [CrawlerOperator]
    sensors = [WebsiteMonitorSensor, ContentChangeSensor]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []