#!/bin/bash
# 爬蟲系統資料備份腳本
# 使用方法: ./scripts/backup-data.sh [EC2公開DNS] [SSH密鑰路徑] [S3儲存桶名稱] [備份保留天數]
# 例如: ./scripts/backup-data.sh ec2-xx-xx-xx-xx.compute-1.amazonaws.com ~/.ssh/aws-crawler-key.pem crawler-backups 30

set -e

# 檢查參數
if [ "$#" -lt 3 ]; then
    echo "用法: $0 [EC2公開DNS] [SSH密鑰路徑] [S3儲存桶名稱] [備份保留天數(選填)]"
    echo "例如: $0 ec2-xx-xx-xx-xx.compute-1.amazonaws.com ~/.ssh/aws-crawler-key.pem crawler-backups 30"
    exit 1
fi

# 設定參數
EC2_PUBLIC_DNS=$1
SSH_KEY_PATH=$2
S3_BUCKET=$3
RETENTION_DAYS=${4:-30}  # 預設保留30天
TIMESTAMP=$(date +%Y%m%d%H%M%S)
BACKUP_PREFIX="crawler-backup-${TIMESTAMP}"
LOCAL_BACKUP_DIR="./backup-${TIMESTAMP}"
REGION=$(aws configure get region || echo "ap-northeast-1")

echo "開始備份爬蟲系統資料..."
echo "EC2 公開 DNS: ${EC2_PUBLIC_DNS}"
echo "S3 儲存桶: ${S3_BUCKET}"
echo "備份時間戳: ${TIMESTAMP}"
echo "備份檔案將保留 ${RETENTION_DAYS} 天"

# 檢查 S3 儲存桶是否存在，如果不存在則創建
if ! aws s3api head-bucket --bucket ${S3_BUCKET} 2>/dev/null; then
    echo "S3 儲存桶 ${S3_BUCKET} 不存在，正在創建..."
    aws s3 mb s3://${S3_BUCKET} --region ${REGION}
    
    # 設定 S3 儲存桶生命週期規則
    cat > lifecycle.json <<EOF
{
    "Rules": [
        {
            "ID": "Delete old backups",
            "Status": "Enabled",
            "Prefix": "crawler-backup-",
            "Expiration": {
                "Days": ${RETENTION_DAYS}
            }
        }
    ]
}
EOF
    aws s3api put-bucket-lifecycle-configuration --bucket ${S3_BUCKET} --lifecycle-configuration file://lifecycle.json
    rm lifecycle.json
fi

# 創建本地臨時目錄
mkdir -p ${LOCAL_BACKUP_DIR}

# 步驟 1: 在 EC2 上準備備份數據
echo "步驟 1: 在 EC2 上準備備份資料..."
REMOTE_SCRIPT=$(cat <<EOF
#!/bin/bash
set -e

# 創建臨時備份目錄
BACKUP_DIR="/tmp/crawler-backup-${TIMESTAMP}"
mkdir -p \${BACKUP_DIR}

# 備份 Elasticsearch 數據
echo "正在備份 Elasticsearch 數據..."
mkdir -p \${BACKUP_DIR}/elasticsearch
if docker ps | grep -q elasticsearch; then
    # 使用 Elasticsearch API 創建快照
    docker exec elasticsearch curl -X PUT "localhost:9200/_snapshot/my_backup/snapshot_${TIMESTAMP}?wait_for_completion=true" -H 'Content-Type: application/json' -d'
    {
      "indices": "*",
      "ignore_unavailable": true,
      "include_global_state": true
    }' || echo "Elasticsearch API 備份失敗，嘗試直接複製檔案..."
    
    # 如果 API 失敗，直接複製資料
    sudo cp -R /data/crawler/elasticsearch/* \${BACKUP_DIR}/elasticsearch/ || echo "Elasticsearch 檔案複製失敗"
fi

# 備份 Kafka 數據
echo "正在備份 Kafka 數據..."
mkdir -p \${BACKUP_DIR}/kafka
if docker ps | grep -q kafka; then
    # 只能在 Kafka 容器停止時安全備份，所以我們只複製配置文件
    sudo cp -R /data/crawler/kafka/config \${BACKUP_DIR}/kafka/ || echo "Kafka 配置檔案複製失敗"
fi

# 備份爬蟲輸出數據
echo "正在備份爬蟲輸出數據..."
mkdir -p \${BACKUP_DIR}/output
sudo cp -R /data/crawler/output/* \${BACKUP_DIR}/output/ || echo "爬蟲輸出複製失敗"

# 備份 docker-compose 配置
echo "正在備份配置文件..."
mkdir -p \${BACKUP_DIR}/config
cp ~/docker-compose.yml \${BACKUP_DIR}/config/ || echo "docker-compose.yml 複製失敗"
cp ~/.env \${BACKUP_DIR}/config/ || echo ".env 複製失敗"

# 創建數據庫備份
echo "正在備份數據庫..."
DB_HOST=\$(grep DB_HOST ~/.env | cut -d '=' -f2)
DB_USER=\$(grep DB_USER ~/.env | cut -d '=' -f2)
DB_PASSWORD=\$(grep DB_PASSWORD ~/.env | cut -d '=' -f2)
DB_DATABASE=\$(grep DB_DATABASE ~/.env | cut -d '=' -f2)

if [ -n "\${DB_HOST}" ] && [ -n "\${DB_USER}" ] && [ -n "\${DB_PASSWORD}" ] && [ -n "\${DB_DATABASE}" ]; then
    # 檢查是否安裝了 MySQL 客戶端
    if ! command -v mysql &> /dev/null; then
        sudo yum install -y mysql
    fi
    
    # 導出數據庫
    mysqldump -h \${DB_HOST} -u \${DB_USER} -p\${DB_PASSWORD} \${DB_DATABASE} > \${BACKUP_DIR}/db_backup.sql || echo "數據庫備份失敗"
else
    echo "找不到完整的數據庫配置信息，跳過數據庫備份"
fi

# 壓縮備份目錄
echo "正在壓縮備份資料..."
cd /tmp
tar -czf crawler-backup-${TIMESTAMP}.tar.gz crawler-backup-${TIMESTAMP}

# 清理臨時目錄
rm -rf \${BACKUP_DIR}

echo "備份準備完成: /tmp/crawler-backup-${TIMESTAMP}.tar.gz"
EOF
)

# 將備份腳本發送到 EC2 並執行
echo "${REMOTE_SCRIPT}" > ${LOCAL_BACKUP_DIR}/ec2_backup.sh
chmod +x ${LOCAL_BACKUP_DIR}/ec2_backup.sh
scp -i ${SSH_KEY_PATH} ${LOCAL_BACKUP_DIR}/ec2_backup.sh ec2-user@${EC2_PUBLIC_DNS}:~/ec2_backup.sh
ssh -i ${SSH_KEY_PATH} ec2-user@${EC2_PUBLIC_DNS} "chmod +x ~/ec2_backup.sh && ~/ec2_backup.sh"

# 步驟 2: 從 EC2 下載備份文件
echo "步驟 2: 從 EC2 下載備份文件..."
scp -i ${SSH_KEY_PATH} ec2-user@${EC2_PUBLIC_DNS}:/tmp/crawler-backup-${TIMESTAMP}.tar.gz ${LOCAL_BACKUP_DIR}/

# 步驟 3: 上傳備份文件到 S3
echo "步驟 3: 上傳備份文件到 S3..."
aws s3 cp ${LOCAL_BACKUP_DIR}/crawler-backup-${TIMESTAMP}.tar.gz s3://${S3_BUCKET}/${BACKUP_PREFIX}/crawler-backup-${TIMESTAMP}.tar.gz

# 步驟 4: 清理 EC2 上的臨時文件
echo "步驟 4: 清理 EC2 上的臨時文件..."
ssh -i ${SSH_KEY_PATH} ec2-user@${EC2_PUBLIC_DNS} "rm ~/ec2_backup.sh /tmp/crawler-backup-${TIMESTAMP}.tar.gz"

# 步驟 5: 列出備份信息
echo "步驟 5: 備份完成，顯示備份信息..."
echo "本地備份: ${LOCAL_BACKUP_DIR}/crawler-backup-${TIMESTAMP}.tar.gz"
echo "S3 備份: s3://${S3_BUCKET}/${BACKUP_PREFIX}/crawler-backup-${TIMESTAMP}.tar.gz"
aws s3 ls s3://${S3_BUCKET}/${BACKUP_PREFIX}/

echo "是否要保留本地備份副本？"
read -p "保留本地備份 (y/n): " keep_local
if [ "$keep_local" != "y" ]; then
    echo "刪除本地備份..."
    rm -rf ${LOCAL_BACKUP_DIR}
fi

# 列出 S3 上的所有備份
echo "S3 儲存桶中的所有備份:"
aws s3 ls s3://${S3_BUCKET}/ --recursive

echo "備份過程完成！"
echo "備份檔案將在 S3 保留 ${RETENTION_DAYS} 天"
echo "要還原備份，請使用以下命令:"
echo "aws s3 cp s3://${S3_BUCKET}/${BACKUP_PREFIX}/crawler-backup-${TIMESTAMP}.tar.gz ."
echo "tar -xzf crawler-backup-${TIMESTAMP}.tar.gz"