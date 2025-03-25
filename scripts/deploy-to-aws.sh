#!/bin/bash
# 爬蟲系統 AWS EC2 部署腳本
# 使用方法: ./scripts/deploy-to-aws.sh [環境] [區域]
# 例如: ./scripts/deploy-to-aws.sh production ap-northeast-1

set -e

# 檢查參數
if [ "$#" -lt 2 ]; then
    echo "用法: $0 [環境] [區域]"
    echo "例如: $0 production ap-northeast-1"
    exit 1
fi

# 設定參數
ENV=$1
REGION=$2
TIMESTAMP=$(date +%Y%m%d%H%M%S)
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_REPO_NAME="crawler-app-${ENV}"
ECR_REPO_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${ECR_REPO_NAME}"
EC2_INSTANCE_NAME="crawler-${ENV}"

echo "開始部署爬蟲系統到 AWS..."
echo "環境: ${ENV}"
echo "區域: ${REGION}"
echo "時間戳: ${TIMESTAMP}"

# 建立輸出目錄
mkdir -p output

# 步驟 1: 登入 AWS ECR
echo "步驟 1: 登入 AWS ECR..."
aws ecr get-login-password --region ${REGION} | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com

# 步驟 2: 檢查並創建 ECR 儲存庫
echo "步驟 2: 檢查並創建 ECR 儲存庫..."
if ! aws ecr describe-repositories --repository-names ${ECR_REPO_NAME} --region ${REGION} > /dev/null 2>&1; then
    echo "創建 ECR 儲存庫: ${ECR_REPO_NAME}"
    aws ecr create-repository --repository-name ${ECR_REPO_NAME} --region ${REGION}
fi

# 步驟 3: 建構 Docker 映像
echo "步驟 3: 建構 Docker 映像..."
export DOCKER_BUILDKIT=1
docker build -t ${ECR_REPO_NAME}:latest --platform=linux/amd64 .
docker tag ${ECR_REPO_NAME}:latest ${ECR_REPO_URI}:latest
docker tag ${ECR_REPO_NAME}:latest ${ECR_REPO_URI}:${TIMESTAMP}

# 步驟 4: 推送映像至 ECR
echo "步驟 4: 推送映像至 ECR..."
docker push ${ECR_REPO_URI}:latest
docker push ${ECR_REPO_URI}:${TIMESTAMP}
echo "映像已推送至: ${ECR_REPO_URI}"

# 步驟 5: 檢查 EC2 實例是否存在
echo "步驟 5: 檢查 EC2 實例..."
INSTANCE_ID=$(aws ec2 describe-instances \
    --region ${REGION} \
    --filters "Name=tag:Name,Values=${EC2_INSTANCE_NAME}" "Name=instance-state-name,Values=running" \
    --query "Reservations[0].Instances[0].InstanceId" \
    --output text)

if [ "${INSTANCE_ID}" = "None" ] || [ -z "${INSTANCE_ID}" ]; then
    echo "找不到運行中的 EC2 實例 ${EC2_INSTANCE_NAME}"
    echo "請先創建 EC2 實例並標記名稱為 ${EC2_INSTANCE_NAME}"
    echo "確保實例已安裝 Docker 和 Docker Compose"
    exit 1
fi

echo "找到 EC2 實例: ${INSTANCE_ID}"

# 步驟 6: 取得 EC2 實例的公開 DNS
EC2_PUBLIC_DNS=$(aws ec2 describe-instances \
    --region ${REGION} \
    --instance-ids ${INSTANCE_ID} \
    --query "Reservations[0].Instances[0].PublicDnsName" \
    --output text)

echo "EC2 公開 DNS: ${EC2_PUBLIC_DNS}"

# 步驟 7: 生成 docker-compose 和 .env 文件
echo "步驟 7: 生成部署文件..."
cat > docker-compose.deploy.yml <<EOF
version: '3'

services:
  crawler:
    image: ${ECR_REPO_URI}:latest
    container_name: crawler
    restart: unless-stopped
    depends_on:
      - kafka
      - elasticsearch
    env_file:
      - .env
    volumes:
      - /data/crawler/output:/app/output

  kafka:
    image: bitnami/kafka:latest
    container_name: kafka
    restart: unless-stopped
    ports:
      - "9092:9092"
    environment:
      - KAFKA_CFG_NODE_ID=1
      - KAFKA_CFG_PROCESS_ROLES=broker,controller
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@kafka:9093
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT
      - KAFKA_KRAFT_CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk
      - ALLOW_PLAINTEXT_LISTENER=yes
    volumes:
      - /data/crawler/kafka:/bitnami/kafka

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    restart: unless-stopped
    ports:
      - "8080:8080"
    environment:
      - KAFKA_CLUSTERS_0_NAME=local
      - KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:9092
    depends_on:
      - kafka

  elasticsearch:
    build:
      context: ./elasticsearch-docker
      dockerfile: Dockerfile
    container_name: elasticsearch
    restart: unless-stopped
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
      - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
    ports:
      - "9200:9200"
    volumes:
      - /data/crawler/elasticsearch:/bitnami/elasticsearch/data
EOF

# 步驟 8: 準備遠程部署命令
echo "步驟 8: 準備遠程部署命令..."
SSH_KEY_PATH=~/.ssh/aws-crawler-key.pem
DEPLOY_SCRIPT=$(cat <<EOF
#!/bin/bash
set -e

# 安裝或更新 Docker (如果需要)
if ! command -v docker &> /dev/null; then
    echo "安裝 Docker..."
    sudo yum update -y
    sudo amazon-linux-extras install docker -y
    sudo service docker start
    sudo usermod -a -G docker ec2-user
    sudo systemctl enable docker
fi

# 安裝或更新 Docker Compose (如果需要)
if ! command -v docker-compose &> /dev/null; then
    echo "安裝 Docker Compose..."
    sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-\$(uname -s)-\$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
fi

# 創建必要的目錄
sudo mkdir -p /data/crawler/output /data/crawler/kafka /data/crawler/elasticsearch
sudo chmod -R 777 /data/crawler

# 登入 ECR
aws ecr get-login-password --region ${REGION} | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com

# 停止並移除舊容器
docker-compose -f ~/docker-compose.yml down || true

# 部署新版本
cp ~/docker-compose.deploy.yml ~/docker-compose.yml
cp ~/.env ~/docker-compose.yml

# 拉取最新映像
docker pull ${ECR_REPO_URI}:latest

# 啟動服務
docker-compose -f ~/docker-compose.yml up -d

echo "部署完成！"
EOF
)

echo "步驟 9: 複製文件到 EC2 實例..."
echo "請確認您擁有 EC2 實例的 SSH 密鑰，路徑為: ${SSH_KEY_PATH}"
read -p "確認繼續？ (y/n): " confirm
if [ "$confirm" != "y" ]; then
    echo "部署已取消"
    exit 1
fi

# 複製文件到 EC2 實例
scp -i ${SSH_KEY_PATH} docker-compose.deploy.yml ec2-user@${EC2_PUBLIC_DNS}:~/docker-compose.deploy.yml
scp -i ${SSH_KEY_PATH} .env ec2-user@${EC2_PUBLIC_DNS}:~/.env

# 複製並執行部署腳本
echo "${DEPLOY_SCRIPT}" > deploy_remote.sh
chmod +x deploy_remote.sh
scp -i ${SSH_KEY_PATH} deploy_remote.sh ec2-user@${EC2_PUBLIC_DNS}:~/deploy.sh
ssh -i ${SSH_KEY_PATH} ec2-user@${EC2_PUBLIC_DNS} "chmod +x ~/deploy.sh && ~/deploy.sh"

# 清理
rm deploy_remote.sh
rm docker-compose.deploy.yml

echo "部署已完成！您的爬蟲系統現在運行在 EC2 實例上: ${EC2_PUBLIC_DNS}"
echo "Kafka UI: http://${EC2_PUBLIC_DNS}:8080"
echo "Elasticsearch: http://${EC2_PUBLIC_DNS}:9200"