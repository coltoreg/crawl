import logging
from typing import List, Dict, Any, Optional
from elasticsearch import Elasticsearch, helpers
from config import ES_CONFIG

# 設定日誌
logger = logging.getLogger("es_manager")

class ESManager:
    """Elasticsearch 管理類，處理文檔索引操作"""
    
    def __init__(self):
        """初始化 Elasticsearch 連接和索引設定"""
        self.hosts = ES_CONFIG.get("hosts", ["http://localhost:9200"])
        self.index_name = ES_CONFIG.get("index", "crawl_data")
        self._client = None
        
    @property
    def client(self) -> Elasticsearch:
        """獲取 Elasticsearch 客戶端，延遲初始化模式"""
        if self._client is None:
            try:
                self._client = Elasticsearch(self.hosts)
                logger.info(f"Elasticsearch 連接成功: {self.hosts}")
            except Exception as e:
                logger.error(f"Elasticsearch 連接失敗: {e}")
                # 返回一個空的客戶端避免報錯
                return Elasticsearch()
        return self._client
        
    def index_article(self, article: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        將單個文章數據索引到 Elasticsearch 中

        Args:
            article: 包含文章數據的字典（例如：{"url": "...", "title": "...", "content": "..."}）
            
        Returns:
            Optional[Dict[str, Any]]: Elasticsearch 返回的結果，失敗時返回 None
        """
        try:
            # 確保必要欄位存在
            required_fields = ["url", "title", "content"]
            missing_fields = [field for field in required_fields if field not in article or not article[field]]
            
            if missing_fields:
                logger.warning(f"文章缺少必要欄位 {missing_fields}, URL: {article.get('url', 'unknown')}")
                return None
                
            res = self.client.index(index=self.index_name, document=article)
            logger.info(f"文章已索引, ID: {res['_id']}, URL: {article.get('url')}")
            return res
        except Exception as e:
            logger.error(f"索引文章失敗: {e}, URL: {article.get('url', 'unknown')}")
            return None

    def bulk_index_articles(self, articles: List[Dict[str, Any]]) -> bool:
        """
        批量索引文章數據到 Elasticsearch 中

        Args:
            articles: 文章數據列表，每個元素為一個字典
            
        Returns:
            bool: 操作是否成功
        """
        if not articles:
            logger.warning("沒有文章需要索引")
            return False
            
        try:
            # 準備批量操作
            actions = [
                {
                    "_index": self.index_name,
                    "_source": article
                }
                for article in articles
                if "url" in article and "title" in article and "content" in article
            ]
            
            if not actions:
                logger.warning("所有文章都缺少必要欄位，無法索引")
                return False
                
            success, failed = helpers.bulk(
                self.client, 
                actions, 
                stats_only=False, 
                raise_on_error=False
            )
            
            if failed:
                logger.warning(f"批量索引部分失敗: 成功 {success} 篇, 失敗 {len(failed)} 篇")
                for item in failed:
                    logger.error(f"索引失敗項目: {item}")
            else:
                logger.info(f"批量索引完全成功: 共索引 {success} 篇文章")
                
            return True
            
        except Exception as e:
            logger.error(f"批量索引失敗: {e}")
            return False

    def create_index_if_not_exists(self) -> bool:
        """
        如果索引不存在，則創建一個新的索引
        
        Returns:
            bool: 操作是否成功
        """
        try:
            if not self.client.indices.exists(index=self.index_name):
                # 設定索引映射，優化文章搜索
                mappings = {
                    "mappings": {
                        "properties": {
                            "title": {"type": "text", "analyzer": "ik_max_word", "search_analyzer": "ik_smart"},
                            "content": {"type": "text", "analyzer": "ik_max_word", "search_analyzer": "ik_smart"},
                            "description": {"type": "text", "analyzer": "ik_max_word"},
                            "keywords": {"type": "text", "analyzer": "ik_max_word"},
                            "url": {"type": "keyword"},
                            "site": {"type": "keyword"},
                            "site_id": {"type": "integer"},
                            "website_category": {"type": "keyword"},
                            "scraped_time": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"}
                        }
                    },
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0
                    }
                }
                
                self.client.indices.create(index=self.index_name, body=mappings)
                logger.info(f"成功創建索引: {self.index_name}")
            return True
        except Exception as e:
            logger.error(f"創建索引失敗: {e}")
            return False
            
    def close(self) -> None:
        """關閉 Elasticsearch 客戶端連線"""
        if self._client:
            self._client.transport.close()
            self._client = None
            logger.info("Elasticsearch 連接已關閉")


# 單例模式
_es_manager = None

def get_es_manager() -> ESManager:
    """獲取 ESManager 實例（單例模式）"""
    global _es_manager
    if _es_manager is None:
        _es_manager = ESManager()
    return _es_manager

def index_article(article: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """將單個文章索引到 Elasticsearch (簡易接口)"""
    return get_es_manager().index_article(article)

def bulk_index_articles(articles: List[Dict[str, Any]]) -> bool:
    """批量索引文章到 Elasticsearch (簡易接口)"""
    return get_es_manager().bulk_index_articles(articles)

def close_es_client() -> None:
    """關閉 Elasticsearch 客戶端連線 (簡易接口)"""
    get_es_manager().close()