"""
配置模組，集中管理爬蟲配置
"""
import os
from typing import Dict, List, Any, Optional, Union
from pathlib import Path
from dotenv import load_dotenv

# 載入環境變數
load_dotenv()

# 基本路徑設定
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)  # 確保輸出目錄存在

# 瀏覽器配置
BROWSER_CONFIG: Dict[str, Any] = {
    "headless": True,           # 是否使用無頭模式
    "user_agent_mode": "random", # 隨機 User-Agent
    #"text_mode": True,          # 是否使用純文字模式
}

# 爬取行為模擬
SIMULATION_CONFIG: Dict[str, Any] = {
    "override_navigator": True,   # 修改瀏覽器 Navigator 參數
    "delay_before_return_html": 3.0, # 爬取後延遲 3 秒，確保 JS 載入完成
    "exclude_external_links": True,  # 不爬取外部連結
    "exclude_social_media_links": True,  # 不爬取社群媒體連結
    "exclude_external_images": True,  # 不爬取外部圖片
}

# 重試機制
RETRY_CONFIG: Dict[str, int] = {
    "max_retries": 3,        # 最大重試次數
    "min_delay": 3,          # 重試間隔最短時間 (秒)
    "max_delay": 10          # 重試間隔最長時間 (秒)
}

# 爬蟲限制
CRAWLER_LIMITS: Dict[str, int] = {
    "max_depth": 1,          # 最大爬取深度
    "max_pages": 10,         # 每個站點最多爬取頁面數
    "initial_urls": 3,       # 初始爬取的 URL 數量
}

# 資料庫配置
"""
DB_CONFIG: Dict[str, Union[str, int]] = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", ""),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_DATABASE", "crawl")
}
"""

#MySQL 連接設定
DB_CONFIG = {
    "host": "localhost",
    "user": "",
    "password": "",
    "database": "crawl"
}


# Kafka 配置
KAFKA_CONFIG: Dict[str, Any] = {
    "bootstrap_servers": [os.getenv("KAFKA_SERVER", "localhost:9092")],
    "task_topic": os.getenv("KAFKA_TASK_TOPIC", "CrawlTasks"),
    "result_topic": os.getenv("KAFKA_RESULT_TOPIC", "CrawlResults"),
    "group_id": os.getenv("KAFKA_GROUP_ID", "CrawlerGroup"),
    "consumer_timeout_ms": int(os.getenv("KAFKA_CONSUMER_TIMEOUT", "3000")),
    "max_poll_interval_ms": int(os.getenv("KAFKA_MAX_POLL_INTERVAL", "300000")),  # 5分鐘
    "max_poll_records": int(os.getenv("KAFKA_MAX_POLL_RECORDS", "500")),
}

# Elasticsearch 配置
ES_CONFIG: Dict[str, Any] = {
    "hosts": [os.getenv("ES_HOST", "http://localhost:9200")],
    "index": os.getenv("ES_INDEX", "crawl_data"),
    "settings": {
        "number_of_shards": int(os.getenv("ES_SHARDS", "1")),
        "number_of_replicas": int(os.getenv("ES_REPLICAS", "0")),
        "refresh_interval": os.getenv("ES_REFRESH_INTERVAL", "30s")
    }
}

# 全局流量控制設定
RATE_LIMIT_CONFIG: Dict[str, Any] = {
    "default_domain_delay": float(os.getenv("RATE_DEFAULT_DELAY", "3.0")),
    "min_domain_delay": float(os.getenv("RATE_MIN_DELAY", "1.5")),
    "max_domain_delay": float(os.getenv("RATE_MAX_DELAY", "20.0")),
    "global_rate_limit": int(os.getenv("RATE_GLOBAL_LIMIT", "40")),
    "global_time_window": int(os.getenv("RATE_TIME_WINDOW", "60")),
    "failure_backoff_factor": float(os.getenv("RATE_BACKOFF_FACTOR", "2.0")),
    "success_recovery_factor": float(os.getenv("RATE_RECOVERY_FACTOR", "0.9")),
    "max_failures_before_throttle": int(os.getenv("RATE_MAX_FAILURES", "5")),
    "throttle_duration_minutes": int(os.getenv("RATE_THROTTLE_MINUTES", "2"))
}

# 站點配置
SITE_CONFIG: Dict[str, Dict[str, Any]] = {
    "udn": {
        "site_id": 1,
        "website_category": "news",
        "start_urls": [
            "https://udn.com/",
            "https://udn.com/news/breaknews/1",
            "https://stars.udn.com/star/index",
            "https://udn.com/news/cate/2/7227",
        ],
        "url_pattern": "news/story/",
        "domain_delay": 1.0,  # 單位：秒，每個請求間隔的時間
        "content_selector": {
            "name": "Article",
            "baseSelector": "body",
            "fields": [
                {"name": "content", "selector": "section.article-content__editor", "type": "text", "multiple": True}
            ]
        }
    },
    "tvbs": {
        "site_id": 2,
        "website_category": "news",
        "start_urls": [
            "https://news.tvbs.com.tw/",
            "https://news.tvbs.com.tw/realtime",
            "https://news.tvbs.com.tw/entertainment",
            "https://news.tvbs.com.tw/life",
        ],
        "url_pattern": r"^https://news\.tvbs\.com\.tw/[\w-]+/\d+$",
        "is_regex": True,
        "domain_delay": 3.5,  # 稍快的延遲
        "content_selector": {
            "name": "tvbs News Article",
            "baseSelector": "main",
            "fields": [
                {"name": "content", "selector": "div > article > div > div:nth-child(n)", "type": "text", "multiple": True}
            ]
        }
    },
    "setn": {
        "site_id": 3,
        "website_category": "news",
        "start_urls": [
            "https://www.setn.com/",
            "https://www.setn.com/viewall.aspx",
            "https://www.setn.com/viewall.aspx?pagegroupid=0",
            "https://star.setn.com/viewall",
        ],
        "url_pattern": "News.aspx?NewsID=",
        "domain_delay": 4.0,  # 適中的延遲
        "content_selector": {
            "name": "setn News Article",
            "baseSelector": "#ckuse",
            "fields": [
                {"name": "content", "selector": "article", "type": "text", "multiple": True}
            ]
        }
    },
    "pixnet": {
        "site_id": 4,
        "website_category": "blog",
        "start_urls": [
            "https://www.pixnet.net/channel/food",
            "https://www.pixnet.net/channel/travel",
            "https://www.pixnet.net/channel/family",
            "https://www.pixnet.net/channel/pet",
        ],
        "url_pattern": "blog/post/",
        "content_selector": {
            "name": "Pixnet Article",
            "baseSelector": "#article-box",
            "fields": [
                {"name": "content", "selector": "div > div.article-body > div.article-content", "type": "text", "multiple": True}
            ]
        }
    },
    "ettoday": {
        "site_id": 5,
        "website_category": "news",
        "start_urls": [
            "https://www.ettoday.net/",
            "https://www.ettoday.net/news/news-list.htm",
            "https://star.ettoday.net/",
            "https://house.ettoday.net/",
            "https://speed.ettoday.net/",
        ],
        "url_pattern": "news/",
        "content_selector": {
            "name": "News Article",
            "baseSelector": "article",
            "fields": [
                {"name": "content", "selector": "div > div.story", "type": "text", "multiple": True}
            ]
        }
    },
    "chinatimes": {
        "site_id": 6,
        "website_category": "news",
        "start_urls": [
            "https://www.chinatimes.com/",
            "https://www.chinatimes.com/life/",
            "https://www.chinatimes.com/realtimenews/",
            "https://www.chinatimes.com/star/",
        ],
        "url_pattern": "realtimenews/",
        "content_selector": {
            "name": "Article",
            "baseSelector": "article",
            "fields": [
                {"name": "content", "selector": "div > div:nth-child(n) > div.row", "type": "text", "multiple": True}
            ]
        }
    },
    "mobile01": {
        "site_id": 7,
        "website_category": "forum",
        "start_urls": [
            "https://www.mobile01.com/",
            "https://www.mobile01.com/category.php?id=6"
        ],
        "url_pattern": "topicdetail.php",
        "domain_delay": 8.0,  # 較長延遲，論壇通常限制更嚴格
        "content_selector": {
            "name": "Article",
            "baseSelector": "div.l-content__main",
            "fields": [
                {"name": "content", "selector": "div:nth-child(n) > div:nth-child(n) > div.l-articlePage__publish > div.u-gapNextV--lg > div > div > div:nth-child(n)", "type": "text", "multiple": True}
            ]
        }
    },
    "businessToday": {
        "site_id": 8,
        "website_category": "finance_news",
        "start_urls": [
            "https://www.businesstoday.com.tw/",
            "https://www.businesstoday.com.tw/catalog/183014",
            "https://www.businesstoday.com.tw/catalog/183007"
        ],
        "url_pattern": "article/category/",
        "content_selector": {
            "name": "Article",
            "baseSelector": "div.article__main",
            "fields": [
                {"name": "content", "selector": "div > div.Zi_ad_ar_iR > div:nth-child(n)", "type": "text", "multiple": True}
            ]
        }
    },
    "Newtalk": {
        "site_id": 9,
        "website_category": "news",
        "start_urls": [
            "https://newtalk.tw/",
            "https://newtalk.tw/news/subcategory/1/%E5%9C%8B%E9%9A%9B",
            "https://newtalk.tw/news/subcategory/5/%E7%94%9F%E6%B4%BB"
        ],
        "url_pattern": "news/view/",
        "content_selector": {
            "name": "Article",
            "baseSelector": "div.main_content.news_main",
            "fields": [
                {"name": "content", "selector": "div > div.left_column.sticky-main > div.news_content", "type": "text", "multiple": True}
            ]
        }
    },
    "ftvtv": {
        "site_id": 9,
        "website_category": "news",
        "start_urls": [
            "https://www.ftvnews.com.tw/",
            "https://www.ftvnews.com.tw/realtime/",
            "https://www.ftvnews.com.tw/popular/"
        ],
        "url_pattern": "news/detail/",
        "content_selector": {
            "name": "Article",
            "baseSelector": "div.col-article.position-relative",
            "fields": [
                {"name": "content", "selector": "div.news-article.fitVids > div > #contentarea", "type": "text", "multiple": True}
            ]
        }
    },
    "thenewslens": {
        "site_id": 10,
        "website_category": "news",
        "start_urls": [
            "https://www.thenewslens.com/",
            "https://www.thenewslens.com/latest-article",
            "https://www.thenewslens.com/author/BBC"
        ],
        "url_pattern": "article/",
        "content_selector": {
            "name": "Article",
            "baseSelector": "article",
            "fields": [
                {"name": "content", "selector": "section[id^='article-content-']", "type": "text", "multiple": True}
            ]
        }
    },
    "ctee": {
        "site_id": 11,
        "website_category": "finance_news",
        "start_urls": [
            "https://www.ctee.com.tw/",
            "https://www.ctee.com.tw/livenews",
        ],
        "url_pattern": "article/",
        "content_selector": {
            "name": "Article",
            "baseSelector": "#main",
            "fields": [
                {"name": "content", "selector": "div.content__body > div.article-wrap", "type": "text", "multiple": True}
            ]
        }
    },
    "tvbs_woman": {
        "site_id": 12,
        "website_category": "beauty_news",
        "start_urls": [
            "https://woman.tvbs.com.tw/",
            "https://woman.tvbs.com.tw/master",
            "https://woman.tvbs.com.tw/constellation/page/1"
        ],
        "url_pattern": "^https://woman\.tvbs\.com\.tw/[\w-]+/\d+$",
        "is_regex": True,
        "content_selector": {
            "name": "Article",
            "baseSelector": "div.woman_article.min-h-screen",
            "fields": [
                {"name": "content", "selector": "div.flex.gap-x-\[46px\].desktop-1512\:gap-x-\[60px\] > div", "type": "text", "multiple": True}
            ]
        }
    },
    "gonews": { #知識網
        "site_id": 13,
        "website_category": "news",
        "start_urls": [
            "https://gonews.work/",
            "https://gonews.work/news/tw",
        ],
        "url_pattern": "news/tw/",
        "content_selector": {
            "name": "Article",
            "baseSelector": "section.post-wrap.padding-small",
            "fields": [
                {"name": "content", "selector": "div > div:nth-child(n) > article > div", "type": "text", "multiple": True}
            ]
        }
    },
    "ftnn": { 
        "site_id": 14,
        "website_category": "news",
        "start_urls": [
            "https://www.ftnn.com.tw/",
            "https://www.ftnn.com.tw/category/0",
        ],
        "url_pattern": "news/",
        "content_selector": {
            "name": "Article",
            "baseSelector": "body",
            "fields": [
                {"name": "content", "selector": "div:nth-child(n) > div > div.news-show > div.news-box", "type": "text", "multiple": True}
            ]
        }
    },
    "ebc": { 
        "site_id": 15,
        "website_category": "news",
        "start_urls": [
            "https://news.ebc.net.tw/",
            "https://www.ftnn.com.tw/category/0",
        ],
        "url_pattern": "^https://news\.ebc\.net\.tw/news/\w+/[0-9]+",
        "is_regex": True,
        "content_selector": {
            "name": "Article",
            "baseSelector": "div.article_main",
            "fields": [
                {"name": "content", "selector": "div.article_content", "type": "text", "multiple": True}
            ]
        }
    },
    "businessweekly": { 
        "site_id": 16,
        "website_category": "news",
        "start_urls": [
            "https://www.businessweekly.com.tw/",
            "https://www.businessweekly.com.tw/channel/business/0000000319",
        ],
        "url_pattern": "^https://www\.businessweekly\.com\.tw/\w+/blog/\d+",
        "is_regex": True,
        "content_selector": {
            "name": "Article",
            "baseSelector": "section.row.no-gutters.position-relative",
            "fields": [
                {"name": "content", "selector": "div.Single-left-part.col-xs-12.col-md-7.col-lg-8 > section.Single-article", "type": "text", "multiple": True}
            ]
        }
    },
    "sportsv": { 
        "site_id": 17,
        "website_category": "sport_news",
        "start_urls": [
            "https://www.sportsv.net/",
            "https://www.sportsv.net/basketball",
            "https://www.sportsv.net/baseball",
            "https://www.sportsv.net/sportslife",
            "https://www.sportsv.net/tennis",
            "https://www.sportsv.net/football",
            "https://www.sportsv.net/racing"
        ],
        "url_pattern": "articles/",
        "content_selector": {
            "name": "Article",
            "baseSelector": "section.article",
            "fields": [
                {"name": "content", "selector": "div > div > div.col-md-10 > div > div.col-md-9 > div.article-content", "type": "text", "multiple": True}
            ]
        }
    },
    "metadata": {
        "site_id": 99,
        "website_category": "metadata",
        "start_urls": [
            "https://czbooks.net/",
            "https://www.baozimh.com/",
            "https://www.timotxt.com/",
            "https://www.xgcartoon.com/",
            "https://www.twbook.cc/shudan/",
            "https://18read.net/",
            "https://tw.manhuagui.com/list/view.html",
            "https://www.novel543.com/",
            "https://daydaynews.cc/zh-tw",
            "https://www.vivi01.com/",
            "https://www.idea-novel.work/",
            "https://forum.twbts.com/index.php",
            "https://memes.tw/",
            "https://comic.acgn.cc/"
        ],
        "url_pattern": "",  # 抓取所有內部連結
        "extract_only_metadata": True,
    }
}

# 獲取站點ID到站點名稱的映射
SITE_ID_TO_NAME: Dict[int, str] = {
    config["site_id"]: site_name 
    for site_name, config in SITE_CONFIG.items()
    if "site_id" in config
}

def get_site_name_by_id(site_id: int) -> Optional[str]:
    """
    根據站點ID獲取站點名稱
    
    Args:
        site_id: 站點ID
        
    Returns:
        Optional[str]: 站點名稱，若找不到則返回None
    """
    return SITE_ID_TO_NAME.get(site_id)

def get_site_config_by_id(site_id: int) -> Optional[Dict[str, Any]]:
    """
    根據站點ID獲取站點配置
    
    Args:
        site_id: 站點ID
        
    Returns:
        Optional[Dict[str, Any]]: 站點配置，若找不到則返回None
    """
    site_name = get_site_name_by_id(site_id)
    if site_name:
        return SITE_CONFIG.get(site_name)
    return None

# 簡單的隨機延遲函數
def get_random_delay(min_delay: float = 2.0, max_delay: float = 6.0) -> float:
    """
    生成隨機延遲時間
    
    Args:
        min_delay: 最小延遲時間 (秒)
        max_delay: 最大延遲時間 (秒)
        
    Returns:
        float: 隨機延遲時間 (秒)
    """
    import random
    return random.uniform(min_delay, max_delay)