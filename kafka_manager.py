import json
import logging
from typing import Any, Dict, List, Optional, Set
from kafka import KafkaProducer, KafkaConsumer, errors
from kafka.admin import KafkaAdminClient, NewTopic
from config import KAFKA_CONFIG

# 設定日誌
logger = logging.getLogger("kafka_manager")

class KafkaManager:
    """Kafka 管理類，處理消息生產和消費"""
    
    def __init__(self):
        """初始化 Kafka 配置"""
        # 從 config 讀取 Kafka 配置參數
        self.servers = KAFKA_CONFIG.get("bootstrap_servers", ["localhost:9092"])
        self.default_task_topic = KAFKA_CONFIG.get("task_topic", "CrawlTasks")
        self.result_topic = KAFKA_CONFIG.get("result_topic", "CrawlResults")
        self.group_id = KAFKA_CONFIG.get("group_id", "CrawlerGroup")
        self.consumer_timeout_ms = KAFKA_CONFIG.get("consumer_timeout_ms", 3000)
        
        # 保存生產者和消費者實例
        self._producer = None
        self._consumers = {}
        
    def get_producer(self) -> KafkaProducer:
        """
        創建並返回 KafkaProducer 實例，使用延遲初始化模式
        
        Returns:
            KafkaProducer: 已配置的 KafkaProducer 實例
        """
        if self._producer is None:
            try:
                self._producer = KafkaProducer(
                    bootstrap_servers=self.servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',  # 等待所有副本確認
                    retries=3,   # 重試次數
                    retry_backoff_ms=500  # 重試間隔
                )
                logger.info(f"Kafka Producer 連接成功: {self.servers}")
            except errors.KafkaError as e:
                logger.error(f"Kafka Producer 初始化失敗: {e}")
                raise
        return self._producer
        
    def get_consumer(self, topic: str, group_id: Optional[str] = None) -> KafkaConsumer:
        """
        創建或獲取 KafkaConsumer 實例，用於消費指定 topic 的消息
        
        Args:
            topic: 要消費的 Kafka topic 名稱
            group_id: 消費者群組 ID，若未指定則使用預設的群組 ID
            
        Returns:
            KafkaConsumer: 已配置的 KafkaConsumer 實例
        """
        if not group_id:
            group_id = self.group_id
            
        # 檢查是否已經有這個 topic 的消費者
        consumer_key = f"{topic}_{group_id}"
        if consumer_key in self._consumers:
            return self._consumers[consumer_key]
            
        try:
            logger.info(f"創建 Consumer，Topic: {topic}, Group ID: {group_id}")
            
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.servers,
                group_id=group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                consumer_timeout_ms=self.consumer_timeout_ms,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m and m.strip() else None,
                # 重要的 group 管理參數
                max_poll_interval_ms=KAFKA_CONFIG.get("max_poll_interval_ms", 300000),  # 5分鐘
                max_poll_records=KAFKA_CONFIG.get("max_poll_records", 500),
            )
            
            logger.info(f"Consumer 已創建，參數檢查：")
            logger.info(f"- Bootstrap Servers: {consumer.config['bootstrap_servers']}")
            logger.info(f"- Group ID: {consumer.config['group_id']}")
            logger.info(f"- Auto Offset Reset: {consumer.config['auto_offset_reset']}")
            
            # 保存消費者實例以便重用
            self._consumers[consumer_key] = consumer
            return consumer
            
        except errors.KafkaError as e:
            logger.error(f"Kafka Consumer 初始化失敗: {e}")
            raise
    
    def send_message(self, message: Dict[str, Any], topic: str) -> bool:
        """
        發送消息到指定的 Kafka topic
        
        Args:
            message: 要發送的消息內容
            topic: 要發送到的 topic 名稱
            
        Returns:
            bool: 發送是否成功
        """
        try:
            producer = self.get_producer()
            future = producer.send(topic, message)
            # 等待發送結果
            record_metadata = future.get(timeout=10)
            logger.info(f"消息已發送到 {topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            return True
        except Exception as e:
            logger.error(f"發送消息到 {topic} 失敗: {e}")
            return False
            
    def send_crawl_task(self, url: str, topic: Optional[str] = None) -> bool:
        """
        發送爬蟲任務
        
        Args:
            url: 要爬取的 URL
            topic: 要使用的 topic，若未指定則使用預設任務 topic
            
        Returns:
            bool: 發送是否成功
        """
        if not topic:
            topic = self.default_task_topic
            
        message = {"url": url}
        logger.info(f"發送任務: {url} 到 topic: {topic}")
        return self.send_message(message, topic)
        
    def send_crawl_result(self, article: Dict[str, Any]) -> bool:
        """
        發送爬蟲結果
        
        Args:
            article: 包含文章數據的字典
            
        Returns:
            bool: 發送是否成功
        """
        logger.info(f"發送結果: {article.get('url', '無URL')} 到 topic: {self.result_topic}")
        return self.send_message(article, self.result_topic)
        
    def ensure_topics_exist(self, topics: List[str]) -> bool:
        """
        確保指定的 topics 存在，不存在則創建
        
        Args:
            topics: 要確保存在的 topics 列表
            
        Returns:
            bool: 操作是否成功
        """
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.servers,
                client_id='crawler-admin'
            )
            
            # 獲取現有 topics
            existing_topics = admin_client.list_topics()
            topics_to_create = [topic for topic in topics if topic not in existing_topics]
            
            if not topics_to_create:
                logger.info("所有需要的 topics 已存在")
                admin_client.close()
                return True
                
            # 創建新 topics
            new_topics = [
                NewTopic(
                    name=topic,
                    num_partitions=1,
                    replication_factor=1
                ) for topic in topics_to_create
            ]
            
            admin_client.create_topics(new_topics)
            logger.info(f"成功創建 topics: {topics_to_create}")
            admin_client.close()
            return True
            
        except Exception as e:
            logger.error(f"確保 topics 存在失敗: {e}")
            return False
            
    def close(self) -> None:
        """關閉所有 Kafka 連接"""
        if self._producer:
            self._producer.close()
            self._producer = None
            logger.info("Kafka Producer 已關閉")
            
        for key, consumer in self._consumers.items():
            consumer.close()
            logger.info(f"Kafka Consumer {key} 已關閉")
        self._consumers = {}


# 單例模式
_kafka_manager = None

def get_kafka_manager() -> KafkaManager:
    """獲取 KafkaManager 實例（單例模式）"""
    global _kafka_manager
    if _kafka_manager is None:
        _kafka_manager = KafkaManager()
    return _kafka_manager

def get_producer() -> KafkaProducer:
    """簡易接口：獲取 KafkaProducer 實例"""
    return get_kafka_manager().get_producer()

def get_consumer(topic: str, group_id: Optional[str] = None) -> KafkaConsumer:
    """簡易接口：獲取 KafkaConsumer 實例"""
    return get_kafka_manager().get_consumer(topic, group_id)

def send_crawl_task(message: Dict[str, Any], topic: Optional[str] = None) -> bool:
    """簡易接口：發送爬蟲任務"""
    url = message.get("url")
    if not url:
        logger.error("任務消息缺少 URL 欄位")
        return False
    return get_kafka_manager().send_crawl_task(url, topic)

def send_crawl_result(article: Dict[str, Any]) -> bool:
    """簡易接口：發送爬蟲結果"""
    return get_kafka_manager().send_crawl_result(article)

def close_kafka_connections() -> None:
    """簡易接口：關閉所有 Kafka 連接"""
    if _kafka_manager:
        _kafka_manager.close()