import random
import os
from dotenv import load_dotenv
from pathlib import Path

# 載入環境變數
load_dotenv()

# 基本路徑設定
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)  # 確保輸出目錄存在

#爬蟲設定
BROWSER_CONFIG = {
    "headless": True,           # 是否使用無頭模式
    "user_agent_mode": "random", # 隨機 User-Agent
    #"text_mode": True,          # 是否使用純文字模式
    
}

#爬取行為模擬
SIMULATION_CONFIG = {
    "override_navigator": True,   # 修改瀏覽器 Navigator 參數
    "delay_before_return_html": 3.0, # 爬取後延遲 1 秒，確保 JS 載入完成
    "exclude_external_links": True,  # 不爬取外部連結
    "exclude_social_media_links": True,  # 不爬取社群媒體連結
    "exclude_external_images": True,  # 不爬取外部圖片

}

#重試機制
RETRY_CONFIG = {
    "max_retries": 3,        # 最大重試次數
    "min_delay": 3,          # 重試間隔最短時間 (秒)
    "max_delay": 10          # 重試間隔最長時間 (秒)
}

# 爬蟲限制
"""
CRAWLER_LIMITS = {
    "max_depth": 3,          # 最大爬取深度
    "max_pages": 100,        # 每個站點最多爬取頁面數
    "initial_urls": 3,        # 初始爬取的 URL 數量
}
"""

CRAWLER_LIMITS = {
    "max_depth": 1,          # 最大爬取深度
    "max_pages": 10,        # 每個站點最多爬取頁面數
    "initial_urls": 3,        # 初始爬取的 URL 數量
}


#MySQL 連接設定
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "!Colto10436",
    "database": "crawl"
}

# Kafka 配置
KAFKA_CONFIG = {
    "bootstrap_servers": ["localhost:9092"],
    "task_topic": "CrawlTasks",
    "result_topic": "CrawlResults",
    "group_id": "CrawlerGroup",
    "consumer_timeout_ms": 3000,  # 消費者超時時間
    "max_poll_interval_ms": 300000,  # 5分鐘
    "max_poll_records": 500,
}

# Elasticsearch 配置
ES_CONFIG = {
    "hosts": ["http://localhost:9200"],
    "index": "crawl_data"
}

KAFDROP_CONFIG = {
    "brokerconnect": "localhost:9092",  # Kafka broker 地址
    "port": 9000                        # Kafdrop UI 介面埠號
}

# 全局流量控制設定
RATE_LIMIT_CONFIG = {
    "default_domain_delay": 3.0,      # 默認域名延遲 (秒)
    "min_domain_delay": 1.5,          # 最小域名延遲 (秒)
    "max_domain_delay": 20.0,         # 最大域名延遲 (秒)
    "global_rate_limit": 40,          # 全局每分鐘最大請求數
    "global_time_window": 60,         # 全局限流時間窗口 (秒)
    "failure_backoff_factor": 2.0,    # 失敗後增加延遲的倍數
    "success_recovery_factor": 0.9,   # 成功後減少延遲的倍數
    "max_failures_before_throttle": 5, # 觸發限流的連續失敗次數
    "throttle_duration_minutes": 2   # 限流持續時間 (分鐘)
}

# 站點配置
SITE_CONFIG = {
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
        "content_selector": {
            "name": "Article",
            "baseSelector": "div.woman_article.min-h-screen",
            "fields": [
                {"name": "content", "selector": "div.flex.gap-x-\[46px\].desktop-1512\:gap-x-\[60px\] > div", "type": "text", "multiple": True}
            ]
        }
    },
    "gonews": { #知識網
        "site_id": 12,
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
        "site_id": 13,
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
        "site_id": 14,
        "website_category": "news",
        "start_urls": [
            "https://news.ebc.net.tw/",
            "https://www.ftnn.com.tw/category/0",
        ],
        "url_pattern": "^https://news\.ebc\.net\.tw/news/\w+/[0-9]+",
        "content_selector": {
            "name": "Article",
            "baseSelector": "div.article_main",
            "fields": [
                {"name": "content", "selector": "div.article_content", "type": "text", "multiple": True}
            ]
        }
    },
    "businessweekly": { 
        "site_id": 15,
        "website_category": "news",
        "start_urls": [
            "https://www.businessweekly.com.tw/",
            "https://www.businessweekly.com.tw/channel/business/0000000319",
        ],
        "url_pattern": "^https://www\.businessweekly\.com\.tw/\w+/blog/\d+",
        "content_selector": {
            "name": "Article",
            "baseSelector": "section.row.no-gutters.position-relative",
            "fields": [
                {"name": "content", "selector": "div.Single-left-part.col-xs-12.col-md-7.col-lg-8 > section.Single-article", "type": "text", "multiple": True}
            ]
        }
    },
    "sportsv": { 
        "site_id": 16,
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


#爬取間隔 (避免封鎖)
def get_random_delay():
    return random.uniform(2, 6)  # 隨機延遲 2~6 秒