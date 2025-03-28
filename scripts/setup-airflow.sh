#!/bin/bash
# Airflow 設置腳本
# 安裝和配置 Airflow 以用於爬蟲排程
# 使用方法: ./scripts/setup-airflow.sh [安裝路徑]

set -e

# 檢查參數
AIRFLOW_HOME=${1:-"$(pwd)/airflow"}
echo "Airflow 將安裝到: $AIRFLOW_HOME"

# 確認繼續
read -p "確認安裝? (y/n): " confirm
if [ "$confirm" != "y" ]; then
    echo "安裝已取消"
    exit 1
fi

# 建立 Airflow 目錄
mkdir -p $AIRFLOW_HOME/dags
mkdir -p $AIRFLOW_HOME/plugins
mkdir -p $AIRFLOW_HOME/logs
mkdir -p $AIRFLOW_HOME/config

# 安裝 Airflow 和相關依賴
echo "正在安裝 Airflow 及依賴套件..."
pip install apache-airflow==2.6.3 \
    apache-airflow-providers-http \
    apache-airflow-providers-mysql \
    apache-airflow-providers-elasticsearch \
    requests \
    lxml

# 設定 Airflow 環境變數
export AIRFLOW_HOME=$AIRFLOW_HOME
echo "AIRFLOW_HOME=$AIRFLOW_HOME" > .env

# 初始化 Airflow 資料庫
echo "初始化 Airflow 資料庫..."
airflow db init

# 創建 Airflow 管理員帳號
echo "創建 Airflow 管理員帳號..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# 複製 DAG 和插件到 Airflow 目錄
echo "複製爬蟲 DAG 和插件到 Airflow 目錄..."
cp -r airflow/dags/* $AIRFLOW_HOME/dags/
cp -r airflow/plugins/* $AIRFLOW_HOME/plugins/

# 設定 Airflow 配置
echo "配置 Airflow..."
cat > $AIRFLOW_HOME/airflow.cfg << EOF
[core]
dags_folder = $AIRFLOW_HOME/dags
base_log_folder = $AIRFLOW_HOME/logs
executor = LocalExecutor
load_examples = False
default_timezone = Asia/Taipei

[database]
sql_alchemy_conn = sqlite:///$AIRFLOW_HOME/airflow.db
sql_alchemy_pool_size = 5
sql_alchemy_pool_recycle = 3600

[scheduler]
dag_dir_list_interval = 30
job_heartbeat_sec = 5
scheduler_heartbeat_sec = 5
min_file_process_interval = 30
dag_discovery_safe_mode = True
max_active_runs_per_dag = 3

[webserver]
web_server_host = 0.0.0.0
web_server_port = 8080
web_server_worker_timeout = 120

[resource]
pool_size = 8

[smtp]
smtp_host = localhost
smtp_starttls = True
smtp_ssl = False
smtp_user = airflow
smtp_password = airflow
smtp_port = 25
smtp_mail_from = airflow@example.com
EOF

# 創建資源池
echo "創建爬蟲資源池..."
airflow pools set crawler_pool 3 "爬蟲任務使用的資源池，限制同時執行的爬蟲數量"

# 註冊站點配置到變數
echo "將站點配置註冊到 Airflow 變數..."
python - << EOF
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname('$0'))))
from airflow.dags.utils import register_site_configs_to_variables
register_site_configs_to_variables()
EOF

# 啟動 Airflow 服務
echo "啟動 Airflow 服務..."
echo "開始 Airflow webserver..."
nohup airflow webserver -D > $AIRFLOW_HOME/logs/webserver.log 2>&1 &
echo "開始 Airflow scheduler..."
nohup airflow scheduler -D > $AIRFLOW_HOME/logs/scheduler.log 2>&1 &

echo "Airflow 安裝和配置完成！"
echo "請訪問 http://localhost:8080 使用使用者名稱 'admin' 和密碼 'admin' 登入"
echo "您可以使用以下命令啟停 Airflow 服務:"
echo "  啟動: $AIRFLOW_HOME/start-airflow.sh"
echo "  停止: $AIRFLOW_HOME/stop-airflow.sh"

# 創建啟停腳本
cat > $AIRFLOW_HOME/start-airflow.sh << EOF
#!/bin/bash
export AIRFLOW_HOME=$AIRFLOW_HOME
echo "啟動 Airflow webserver..."
nohup airflow webserver -D > $AIRFLOW_HOME/logs/webserver.log 2>&1 &
echo "啟動 Airflow scheduler..."
nohup airflow scheduler -D > $AIRFLOW_HOME/logs/scheduler.log 2>&1 &
echo "Airflow 服務已啟動"
EOF

cat > $AIRFLOW_HOME/stop-airflow.sh << EOF
#!/bin/bash
echo "停止 Airflow 服務..."
for pid in \$(ps -ef | grep "airflow" | grep -v grep | awk '{print \$2}'); do
    kill \$pid
done
echo "Airflow 服務已停止"
EOF

chmod +x $AIRFLOW_HOME/start-airflow.sh
chmod +x $AIRFLOW_HOME/stop-airflow.sh

# 創建 Docker Compose 文件以支援容器化部署
cat > docker-compose-airflow.yml << EOF
version: '3'
services:
  postgres:
    image: postgres:13
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    restart: always

  webserver:
    image: apache/airflow:2.6.3
    depends_on:
      - postgres
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
      - AIRFLOW__CORE__FERNET_KEY=''
      - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=30
      - AIRFLOW_UID=50000
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/logs:/opt/airflow/logs
      - ./airflow/plugins:/opt/airflow/plugins
      - ./airflow/config:/opt/airflow/config
    ports:
      - "8080:8080"
    command: webserver
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always

  scheduler:
    image: apache/airflow:2.6.3
    depends_on:
      - postgres
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
      - AIRFLOW__CORE__FERNET_KEY=''
      - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=30
      - AIRFLOW_UID=50000
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/logs:/opt/airflow/logs
      - ./airflow/plugins:/opt/airflow/plugins
      - ./airflow/config:/opt/airflow/config
    command: scheduler
    restart: always

volumes:
  postgres-db-volume:
EOF

echo "已創建 Docker Compose 檔案 'docker-compose-airflow.yml' 以支援容器化部署"
echo "使用以下命令啟動容器化 Airflow:"
echo "  docker-compose -f docker-compose-airflow.yml up -d"
