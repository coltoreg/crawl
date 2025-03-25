#!/bin/bash
# 爬蟲系統健康檢查腳本
# 使用方法: ./scripts/healthcheck.sh [EC2公開DNS] [SSH密鑰路徑] [通知郵箱] [警報阈值]
# 例如: ./scripts/healthcheck.sh ec2-xx-xx-xx-xx.compute-1.amazonaws.com ~/.ssh/aws-crawler-key.pem admin@example.com 3

set -e

# 檢查參數
if [ "$#" -lt 2 ]; then
    echo "用法: $0 [EC2公開DNS] [SSH密鑰路徑] [通知郵箱(選填)] [警報阈值(選填)]"
    echo "例如: $0 ec2-xx-xx-xx-xx.compute-1.amazonaws.com ~/.ssh/aws-crawler-key.pem admin@example.com 3"
    exit 1
fi

# 設定參數
EC2_PUBLIC_DNS=$1
SSH_KEY_PATH=$2
NOTIFICATION_EMAIL=${3:-""}  # 若不提供則不發送通知
ALARM_THRESHOLD=${4:-3}      # 預設連續失敗 3 次觸發警報
LOG_DIR="./logs"
LOG_FILE="${LOG_DIR}/healthcheck-$(date +%Y%m%d).log"
STATUS_FILE="${LOG_DIR}/healthcheck-status.json"
TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")

# 建立日誌目錄
mkdir -p ${LOG_DIR}

# 載入狀態文件
if [ -f "${STATUS_FILE}" ]; then
    FAILURE_COUNT=$(jq -r '.failure_count' "${STATUS_FILE}")
    LAST_CHECK=$(jq -r '.last_check' "${STATUS_FILE}")
    LAST_STATUS=$(jq -r '.last_status' "${STATUS_FILE}")
else
    FAILURE_COUNT=0
    LAST_CHECK=""
    LAST_STATUS="unknown"
    echo '{"failure_count": 0, "last_check": "", "last_status": "unknown", "issues": []}' > "${STATUS_FILE}"
fi

log() {
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] $1" | tee -a "${LOG_FILE}"
}

log "開始健康檢查..."
log "EC2 公開 DNS: ${EC2_PUBLIC_DNS}"
log "上次檢查時間: ${LAST_CHECK:-"首次檢查"}"
log "上次狀態: ${LAST_STATUS}"
log "當前連續失敗次數: ${FAILURE_COUNT}"

# 檢查 SSH 連線
check_ssh_connection() {
    log "檢查 SSH 連線..."
    if ssh -i ${SSH_KEY_PATH} -o ConnectTimeout=10 -o BatchMode=yes -o StrictHostKeyChecking=no ec2-user@${EC2_PUBLIC_DNS} "echo 'SSH connection successful'" &> /dev/null; then
        log "✅ SSH 連線正常"
        return 0
    else
        log "❌ SSH 連線失敗"
        return 1
    fi
}

# 檢查 Docker 服務
check_docker_service() {
    log "檢查 Docker 服務..."
    if ssh -i ${SSH_KEY_PATH} ec2-user@${EC2_PUBLIC_DNS} "sudo systemctl is-active docker &> /dev/null"; then
        log "✅ Docker 服務運行中"
        return 0
    else
        log "❌ Docker 服務未運行"
        return 1
    fi
}

# 檢查容器狀態
check_containers() {
    log "檢查容器狀態..."
    
    # 獲取所有容器狀態
    CONTAINER_STATUS=$(ssh -i ${SSH_KEY_PATH} ec2-user@${EC2_PUBLIC_DNS} "docker ps -a --format '{{.Names}},{{.Status}}' | grep -v 'Exited (0)'")
    
    # 檢查關鍵容器是否運行中
    local all_running=true
    local issues=()
    
    for container in "crawler" "kafka" "elasticsearch"; do
        local container_info=$(echo "${CONTAINER_STATUS}" | grep "^${container},")
        
        if [ -z "${container_info}" ]; then
            log "❌ ${container} 容器不存在或已停止"
            all_running=false
            issues+=("${container} 容器不存在或已停止")
        elif ! echo "${container_info}" | grep -q "Up"; then
            local status=$(echo "${container_info}" | cut -d',' -f2)
            log "❌ ${container} 容器狀態異常: ${status}"
            all_running=false
            issues+=("${container} 容器狀態異常: ${status}")
        else
            log "✅ ${container} 容器運行中"
        fi
    done
    
    # 更新狀態文件中的問題列表
    if [ ${#issues[@]} -gt 0 ]; then
        local issues_json=$(printf '%s\n' "${issues[@]}" | jq -R . | jq -s .)
        jq --argjson issues "$issues_json" '.issues = $issues' "${STATUS_FILE}" > "${STATUS_FILE}.tmp"
        mv "${STATUS_FILE}.tmp" "${STATUS_FILE}"
        return 1
    else
        jq '.issues = []' "${STATUS_FILE}" > "${STATUS_FILE}.tmp"
        mv "${STATUS_FILE}.tmp" "${STATUS_FILE}"
        return 0
    fi
}

# 檢查系統資源
check_system_resources() {
    log "檢查系統資源..."
    
    # 獲取系統資源使用情況
    RESOURCE_INFO=$(ssh -i ${SSH_KEY_PATH} ec2-user@${EC2_PUBLIC_DNS} "
        echo '--- CPU 使用率 ---'
        top -bn1 | grep 'Cpu(s)' | awk '{print \$2 + \$4}' | awk '{print \"CPU 使用率: \" \$1 \"%\"}'
        
        echo '--- 記憶體使用率 ---'
        free -m | grep Mem | awk '{print \"記憶體使用率: \" \$3/\$2*100 \"%\"}' | awk '{printf(\"%.2f\\n\", \$2)}'
        
        echo '--- 磁碟使用率 ---'
        df -h / | grep -v Filesystem | awk '{print \"磁碟使用率: \" \$5}'
        
        echo '--- Docker 磁碟使用 ---'
        docker system df
    ")
    
    log "${RESOURCE_INFO}"
    
    # 檢查關鍵資源閾值
    local cpu_usage=$(echo "${RESOURCE_INFO}" | grep "CPU 使用率" | awk '{print $3}' | sed 's/%//')
    local memory_usage=$(echo "${RESOURCE_INFO}" | grep "記憶體使用率" | awk '{print $2}' | sed 's/%//')
    local disk_usage=$(echo "${RESOURCE_INFO}" | grep "磁碟使用率" | awk '{print $2}' | sed 's/%//')
    
    local resource_issues=()
    
    if (( $(echo "${cpu_usage} > 90" | bc -l) )); then
        log "⚠️ CPU 使用率過高: ${cpu_usage}%"
        resource_issues+=("CPU 使用率過高: ${cpu_usage}%")
    fi
    
    if (( $(echo "${memory_usage} > 90" | bc -l) )); then
        log "⚠️ 記憶體使用率過高: ${memory_usage}%"
        resource_issues+=("記憶體使用率過高: ${memory_usage}%")
    fi
    
    if (( $(echo "${disk_usage} > 90" | bc -l) )); then
        log "⚠️ 磁碟使用率過高: ${disk_usage}"
        resource_issues+=("磁碟使用率過高: ${disk_usage}")
    fi
    
    # 更新狀態文件中的資源問題
    if [ ${#resource_issues[@]} -gt 0 ]; then
        local issues_json=$(printf '%s\n' "${resource_issues[@]}" | jq -R . | jq -s .)
        jq --argjson resources "$issues_json" '.resource_issues = $resources' "${STATUS_FILE}" > "${STATUS_FILE}.tmp"
        mv "${STATUS_FILE}.tmp" "${STATUS_FILE}"
        return 1
    else
        jq '.resource_issues = []' "${STATUS_FILE}" > "${STATUS_FILE}.tmp"
        mv "${STATUS_FILE}.tmp" "${STATUS_FILE}"
        log "✅ 系統資源使用正常"
        return 0
    fi
}

# 檢查網站連線
check_web_services() {
    log "檢查網站服務..."
    local web_issues=()
    
    # 檢查 Kafka UI
    if curl -s -m 10 "http://${EC2_PUBLIC_DNS}:8080" | grep -q "Kafka"; then
        log "✅ Kafka UI 可訪問"
    else
        log "❌ Kafka UI 無法訪問"
        web_issues+=("Kafka UI 無法訪問")
    fi
    
    # 檢查 Elasticsearch
    if curl -s -m 10 "http://${EC2_PUBLIC_DNS}:9200" | grep -q "name"; then
        log "✅ Elasticsearch 可訪問"
    else
        log "❌ Elasticsearch 無法訪問"
        web_issues+=("Elasticsearch 無法訪問")
    fi
    
    # 更新狀態文件中的網站問題
    if [ ${#web_issues[@]} -gt 0 ]; then
        local issues_json=$(printf '%s\n' "${web_issues[@]}" | jq -R . | jq -s .)
        jq --argjson web "$issues_json" '.web_issues = $web' "${STATUS_FILE}" > "${STATUS_FILE}.tmp"
        mv "${STATUS_FILE}.tmp" "${STATUS_FILE}"
        return 1
    else
        jq '.web_issues = []' "${STATUS_FILE}" > "${STATUS_FILE}.tmp"
        mv "${STATUS_FILE}.tmp" "${STATUS_FILE}"
        log "✅ 網站服務正常"
        return 0
    fi
}

# 檢查爬蟲日誌
check_crawler_logs() {
    log "檢查爬蟲日誌..."
    
    # 獲取最近的爬蟲日誌
    RECENT_LOGS=$(ssh -i ${SSH_KEY_PATH} ec2-user@${EC2_PUBLIC_DNS} "
        sudo tail -n 50 /data/crawler/output/crawler.log 2>/dev/null || echo 'No log file found'
    ")
    
    # 檢查錯誤關鍵字
    if echo "${RECENT_LOGS}" | grep -i -E "error|exception|fatal|failed|crash" | grep -v "retry"; then
        log "⚠️ 在爬蟲日誌中發現錯誤"
        ERROR_SAMPLES=$(echo "${RECENT_LOGS}" | grep -i -E "error|exception|fatal|failed|crash" | grep -v "retry" | head -5)
        log "錯誤樣本:"
        log "${ERROR_SAMPLES}"
        
        # 更新狀態文件中的日誌問題
        local log_issues=("爬蟲日誌中發現錯誤")
        local issues_json=$(printf '%s\n' "${log_issues[@]}" | jq -R . | jq -s .)
        jq --argjson logs "$issues_json" '.log_issues = $logs' "${STATUS_FILE}" > "${STATUS_FILE}.tmp"
        mv "${STATUS_FILE}.tmp" "${STATUS_FILE}"
        return 1
    else
        jq '.log_issues = []' "${STATUS_FILE}" > "${STATUS_FILE}.tmp"
        mv "${STATUS_FILE}.tmp" "${STATUS_FILE}"
        log "✅ 爬蟲日誌正常"
        return 0
    fi
}

# 嘗試修復問題
fix_issues() {
    log "嘗試修復問題..."
    
    # 重啟 Docker 服務
    if ! check_docker_service; then
        log "正在重啟 Docker 服務..."
        ssh -i ${SSH_KEY_PATH} ec2-user@${EC2_PUBLIC_DNS} "sudo systemctl restart docker"
        sleep 10  # 等待 Docker 重啟
    fi
    
    # 檢查並重啟有問題的容器
    local container_issues=$(jq -r '.issues[]' "${STATUS_FILE}" 2>/dev/null | grep "容器" || echo "")
    if [ -n "${container_issues}" ]; then
        log "正在重啟服務..."
        ssh -i ${SSH_KEY_PATH} ec2-user@${EC2_PUBLIC_DNS} "
            cd ~
            docker-compose -f docker-compose.yml down
            sleep 5
            docker-compose -f docker-compose.yml up -d
        "
        log "服務已重啟"
    fi
    
    # 檢查磁碟使用率是否過高
    local disk_issue=$(jq -r '.resource_issues[]' "${STATUS_FILE}" 2>/dev/null | grep "磁碟使用率" || echo "")
    if [ -n "${disk_issue}" ]; then
        log "正在清理磁碟空間..."
        ssh -i ${SSH_KEY_PATH} ec2-user@${EC2_PUBLIC_DNS} "
            # 清理 Docker 垃圾
            docker system prune -f
            
            # 刪除舊日誌
            sudo find /data/crawler/output -name '*.log.*' -type f -mtime +7 -delete
            
            # 壓縮舊日誌
            sudo find /data/crawler/output -name '*.log' -type f -mtime +3 -exec gzip {} \;
        "
        log "磁碟清理完成"
    fi
}

# 發送通知
send_notification() {
    if [ -z "${NOTIFICATION_EMAIL}" ]; then
        log "未設置通知郵箱，跳過通知"
        return
    fi
    
    log "發送通知郵件到 ${NOTIFICATION_EMAIL}..."
    
    # 獲取問題摘要
    ALL_ISSUES=$(jq -r '.issues + .resource_issues + .web_issues + .log_issues | .[]' "${STATUS_FILE}" 2>/dev/null)
    
    if [ -z "${ALL_ISSUES}" ]; then
        ALL_ISSUES="未知問題"
    fi
    
    # 檢查是否安裝 mail 命令
    if ! command -v mail &> /dev/null; then
        log "未找到 mail 命令，將使用 AWS SES 發送郵件"
        
        # 使用 AWS SES 發送郵件
        SUBJECT="爬蟲系統警報: ${EC2_PUBLIC_DNS} 健康檢查失敗"
        BODY="EC2 實例 ${EC2_PUBLIC_DNS} 健康檢查失敗。\n\n問題摘要:\n${ALL_ISSUES}\n\n請檢查系統狀態。\n\n時間: ${TIMESTAMP}"
        
        aws ses send-email \
            --from "crawler-monitor@example.com" \
            --destination "ToAddresses=${NOTIFICATION_EMAIL}" \
            --message "Subject={Data=${SUBJECT},Charset=UTF-8},Body={Text={Data=${BODY},Charset=UTF-8}}" \
            --region us-east-1 || log "AWS SES 郵件發送失敗"
    else
        # 使用本地 mail 命令
        echo -e "EC2 實例 ${EC2_PUBLIC_DNS} 健康檢查失敗。\n\n問題摘要:\n${ALL_ISSUES}\n\n請檢查系統狀態。\n\n時間: ${TIMESTAMP}" | \
        mail -s "爬蟲系統警報: ${EC2_PUBLIC_DNS} 健康檢查失敗" ${NOTIFICATION_EMAIL} || log "本地郵件發送失敗"
    fi
    
    log "通知已發送"
}

# 執行各項檢查
run_checks() {
    local all_checks_passed=true
    
    # 檢查 SSH 連線
    if ! check_ssh_connection; then
        all_checks_passed=false
    else
        # 只有 SSH 連線成功才執行其他檢查
        if ! check_docker_service; then
            all_checks_passed=false
        fi
        
        if ! check_containers; then
            all_checks_passed=false
        fi
        
        if ! check_system_resources; then
            all_checks_passed=false
        fi
        
        if ! check_web_services; then
            all_checks_passed=false
        fi
        
        if ! check_crawler_logs; then
            all_checks_passed=false
        fi
    fi
    
    return $((all_checks_passed == true ? 0 : 1))
}

main() {
    # 執行檢查
    if run_checks; then
        log "✅ 所有檢查均通過"
        CURRENT_STATUS="healthy"
        FAILURE_COUNT=0
    else
        log "❌ 檢查失敗"
        CURRENT_STATUS="unhealthy"
        FAILURE_COUNT=$((FAILURE_COUNT + 1))
        
        log "連續失敗次數: ${FAILURE_COUNT}/${ALARM_THRESHOLD}"
        
        # 如果連續失敗達到閾值，嘗試修復並發送通知
        if [ ${FAILURE_COUNT} -ge ${ALARM_THRESHOLD} ]; then
            log "⚠️ 警報: 連續失敗次數達到閾值"
            fix_issues
            send_notification
        fi
    fi
    
    # 更新狀態文件
    jq --arg status "${CURRENT_STATUS}" \
       --arg check "${TIMESTAMP}" \
       --argjson count ${FAILURE_COUNT} \
       '.last_status = $status | .last_check = $check | .failure_count = $count' \
       "${STATUS_FILE}" > "${STATUS_FILE}.tmp"
    mv "${STATUS_FILE}.tmp" "${STATUS_FILE}"
    
    log "健康檢查完成"
    
    # 返回狀態
    if [ "${CURRENT_STATUS}" = "healthy" ]; then
        return 0
    else
        return 1
    fi
}

# 執行主函數
main
exit $?