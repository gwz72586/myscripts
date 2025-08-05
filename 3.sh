#!/bin/bash
# Google Drive Expander v7 - 谷歌网盘扩充器 (稳定简化版)
# Author: DX

###################### 基本配置 ######################
WARC_LIST_URL="https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-30/warc.paths.gz"
DEST_PATH="/dx"
MAX_TRANSFER="700G"

# 网络优化配置
THREADS=16
CHUNK_SIZE="256M"
BUFFER_SIZE="2G"
MULTI_THREAD_STREAMS=8

# 监控配置
LOW_SPEED_MB=5
LOW_SPEED_SECONDS=60

TMP_DIR="/tmp/warc_uploader"
LOG_DIR="./logs"
mkdir -p "$TMP_DIR" "$LOG_DIR"

export LANG=zh_CN.UTF-8
export LC_ALL=zh_CN.UTF-8

DEBUG_MODE=${DEBUG_MODE:-0}

###################### 工具函数 ######################
debug_log() {
    if [ "$DEBUG_MODE" = "1" ]; then
        echo "[DEBUG $(date '+%H:%M:%S')] $1" >&2
    fi
}

error_log() {
    echo "[ERROR $(date '+%H:%M:%S')] $1" >&2
}

###################### 网络监控函数 ######################
get_main_interface() {
    ip route | grep '^default' | awk '{print $5}' | head -1 | cut -d'@' -f1
}

get_network_speed() {
    local interface="$1"
    local speed_file="$TMP_DIR/net_${interface}.tmp"
    
    # 获取网卡统计
    local stats_line
    stats_line=$(grep "^[ ]*${interface}[:@]" /proc/net/dev 2>/dev/null | head -1)
    
    if [ -z "$stats_line" ]; then
        echo "0"
        return
    fi
    
    local current_bytes
    current_bytes=$(echo "$stats_line" | awk '{print $10}')
    local current_time
    current_time=$(date +%s)
    
    if ! echo "$current_bytes" | grep -q '^[0-9]*$'; then
        echo "0"
        return
    fi
    
    if [ -f "$speed_file" ]; then
        local prev_bytes prev_time
        if read -r prev_bytes prev_time < "$speed_file" 2>/dev/null; then
            if echo "$prev_bytes" | grep -q '^[0-9]*$' && echo "$prev_time" | grep -q '^[0-9]*$'; then
                local time_diff=$((current_time - prev_time))
                local bytes_diff=$((current_bytes - prev_bytes))
                
                if [ "$time_diff" -ge 5 ] && [ "$bytes_diff" -ge 0 ]; then
                    local speed_mb=$((bytes_diff / time_diff / 1048576))
                    echo "$speed_mb"
                else
                    echo "0"
                fi
            else
                echo "0"
            fi
        else
            echo "0"
        fi
    else
        echo "0"
    fi
    
    echo "$current_bytes $current_time" > "$speed_file" 2>/dev/null || true
}

reset_network_monitor() {
    local interface="$1"
    rm -f "$TMP_DIR/net_${interface}.tmp"
    get_network_speed "$interface" >/dev/null 2>&1
}

###################### 进程管理函数 ######################
wait_processes() {
    local wait_count=0
    local max_wait=120
    
    while [ "$wait_count" -lt "$max_wait" ]; do
        local alive=0
        
        for pid in "$@"; do
            if ps -p "$pid" > /dev/null 2>&1; then
                alive=$((alive + 1))
            fi
        done
        
        if [ "$alive" -eq 0 ]; then
            echo -e "\n├─ ✅ 所有进程完成"
            return 0
        fi
        
        if [ $((wait_count % 20)) -eq 0 ] && [ "$wait_count" -gt 0 ]; then
            echo -e "\n├─ ⏳ 等待中... 剩余 $alive 个进程"
        fi
        
        wait_count=$((wait_count + 1))
        sleep 3
    done
    
    echo -e "\n├─ ⚠️ 等待超时，强制终止进程"
    for pid in "$@"; do
        kill -9 "$pid" 2>/dev/null || true
    done
    sleep 2
    return 1
}

cleanup_processes() {
    local remote="$1"
    echo "├─ 🧹 清理进程..."
    
    local pids
    pids=$(pgrep -f "rclone.*$remote:" 2>/dev/null || true)
    
    if [ -n "$pids" ]; then
        echo "$pids" | xargs -r kill -9 2>/dev/null || true
        sleep 2
        echo "├─ ✅ 清理完成"
    else
        echo "├─ ✅ 无需清理"
    fi
}

###################### 存储管理函数 ######################
get_node_storage() {
    local remote="$1"
    timeout 15 rclone size "$remote:$DEST_PATH" --json 2>/dev/null | \
        jq -r '.bytes // 0' 2>/dev/null || echo "0"
}

verify_batch_completion() {
    local remote="$1"
    local last_bytes="$2"
    local attempts=0
    
    while [ "$attempts" -lt 6 ]; do
        local current_bytes
        current_bytes=$(get_node_storage "$remote")
        if echo "$current_bytes" | grep -q '^[0-9]*$' && [ "$current_bytes" -gt "$last_bytes" ]; then
            echo "$current_bytes"
            return 0
        fi
        attempts=$((attempts + 1))
        sleep 10
    done
    
    echo "$last_bytes"
    return 1
}

###################### 启动界面 ######################
show_banner() {
    clear
    echo -e "\033[38;5;39m"
    cat << 'EOF'
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║   ██████╗  ██████╗  ██████╗  ██████╗ ██╗     ███████╗    ██████╗ ██╗  ██╗   ║
║  ██╔════╝ ██╔═══██╗██╔═══██╗██╔════╝ ██║     ██╔════╝    ██╔══██╗╚██╗██╔╝   ║
║  ██║  ███╗██║   ██║██║   ██║██║  ███╗██║     █████╗      ██║  ██║ ╚███╔╝    ║
║  ██║   ██║██║   ██║██║   ██║██║   ██║██║     ██╔══╝      ██║  ██║ ██╔██╗    ║
║  ╚██████╔╝╚██████╔╝╚██████╔╝╚██████╔╝███████╗███████╗    ██████╔╝██╔╝ ██╗   ║
║   ╚═════╝  ╚═════╝  ╚═════╝  ╚═════╝ ╚══════╝╚══════╝    ╚═════╝ ╚═╝  ╚═╝   ║
║                                                                              ║
║              🌍 GOOGLE DRIVE EXPANDER v7.0 - DX 🌍                          ║
║                       谷歌网盘扩充器 (稳定版)                                   ║
║                                                                              ║
║                     🚀 稳定可靠 • 智能监控 • 防卡死                              ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
EOF
    echo -e "\033[0m"
    
    echo -e "\033[38;5;46m🔧 系统配置\033[0m"
    echo "   ├─ 并发线程: $THREADS"
    echo "   ├─ 分块大小: $CHUNK_SIZE" 
    echo "   ├─ 缓存大小: $BUFFER_SIZE"
    echo "   └─ 低速阈值: ${LOW_SPEED_MB}MB/s"
    echo
}

###################### 主程序 ######################
show_banner

# 检查依赖
for cmd in bc jq curl rclone; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        error_log "缺少必要工具: $cmd"
        exit 1
    fi
done

# 获取网卡信息
MAIN_INTERFACE=$(get_main_interface)
if [ -z "$MAIN_INTERFACE" ]; then
    MAIN_INTERFACE="eth0"
fi

INTERFACE_IP=$(ip addr show "$MAIN_INTERFACE" 2>/dev/null | grep 'inet ' | awk '{print $2}' | head -1 || echo "N/A")
echo -e "\033[38;5;226m🌐 网络接口\033[0m: $MAIN_INTERFACE ($INTERFACE_IP)"

# 初始化网卡监控
if grep -q "^[ ]*${MAIN_INTERFACE}[:@]" /proc/net/dev 2>/dev/null; then
    echo "   └─ 网卡监控就绪"
    reset_network_monitor "$MAIN_INTERFACE"
else
    echo "   └─ ⚠️ 网卡监控可能不准确"
fi

# 用户交互
DEFAULT_REPEAT=25
echo -e "\033[38;5;51m⏰ 循环间隔\033[0m"
read -p "   循环间隔小时数 (默认${DEFAULT_REPEAT}小时，0=仅执行一次): " REPEAT_INTERVAL_HOURS
REPEAT_INTERVAL_HOURS=${REPEAT_INTERVAL_HOURS:-$DEFAULT_REPEAT}

# 节点选择
ALL_REMOTES=($(rclone listremotes | sed 's/:$//'))
if [ ${#ALL_REMOTES[@]} -eq 0 ]; then
    error_log "未检测到rclone存储节点"
    exit 1
fi

echo -e "\n\033[38;5;51m🟢 存储节点\033[0m (共${#ALL_REMOTES[@]}个)"
for i in "${!ALL_REMOTES[@]}"; do
    echo "   ├─ [$((i+1))] ${ALL_REMOTES[i]}"
done

echo -e "\n\033[38;5;196m🎯 节点选择\033[0m"
read -p "   选择节点 (默认全选并随机排序，数字用空格分隔): " NODE_SELECTION

if [ -z "$NODE_SELECTION" ]; then
    SELECTED_REMOTES=($(printf '%s\n' "${ALL_REMOTES[@]}" | shuf))
    echo "   ✅ 已选择全部节点并随机排序"
else
    SELECTED_REMOTES=()
    for num in $NODE_SELECTION; do
        if echo "$num" | grep -q '^[0-9]*$' && [ "$num" -ge 1 ] && [ "$num" -le ${#ALL_REMOTES[@]} ]; then
            SELECTED_REMOTES+=("${ALL_REMOTES[$((num-1))]}")
        fi
    done
fi

if [ ${#SELECTED_REMOTES[@]} -eq 0 ]; then
    error_log "未选择有效节点"
    exit 1
fi

echo "   已选择: ${SELECTED_REMOTES[*]}"

read -p "📍 起始文件行号 (默认1): " START_LINE
START_LINE=${START_LINE:-1}

# 下载文件列表
WARC_FILE="$TMP_DIR/warc.paths"
echo -e "\n\033[38;5;226m📥 正在获取文件列表...\033[0m"
if curl -sL "$WARC_LIST_URL" | gunzip -c > "$WARC_FILE"; then
    TOTAL_LINES=$(wc -l < "$WARC_FILE")
    echo "✅ 成功获取 $TOTAL_LINES 个文件"
else
    error_log "获取文件列表失败"
    exit 1
fi

###################### 主循环 ######################
echo -e "\n\033[38;5;51m🚀 开始上传任务...\033[0m"

while true; do
    echo -e "\n\033[48;5;21m========== 新一轮上传 $(date '+%F %T') ==========\033[0m"
    
    for REMOTE in "${SELECTED_REMOTES[@]}"; do
        echo -e "\n┌─ \033[38;5;82m🚀 节点: $REMOTE\033[0m"
        
        # 文件和状态管理
        PROGRESS_FILE="$TMP_DIR/${REMOTE}.progress"
        LOGFILE="$LOG_DIR/${REMOTE}_$(date +%F_%H-%M-%S).log"
        
        if [ ! -f "$PROGRESS_FILE" ]; then
            echo "$START_LINE" > "$PROGRESS_FILE"
        fi
        CURRENT_LINE=$(cat "$PROGRESS_FILE")
        
        # 重新初始化网卡监控
        reset_network_monitor "$MAIN_INTERFACE"
        
        # 获取初始存储量
        LAST_STORAGE=$(get_node_storage "$REMOTE")
        debug_log "节点 $REMOTE 初始存储: $LAST_STORAGE bytes"
        
        NO_PROGRESS=0
        
        while [ "$CURRENT_LINE" -le "$TOTAL_LINES" ]; do
            BATCH_END=$((CURRENT_LINE + THREADS - 1))
            if [ "$BATCH_END" -gt "$TOTAL_LINES" ]; then
                BATCH_END=$TOTAL_LINES
            fi
            
            echo -e "\n├─ 🚀 批次 $CURRENT_LINE-$BATCH_END 共 $((BATCH_END-CURRENT_LINE+1)) 文件"
            
            # 准备批次文件
            BATCH_URLS="$TMP_DIR/${REMOTE}_urls_${CURRENT_LINE}.txt"
            sed -n "${CURRENT_LINE},${BATCH_END}p" "$WARC_FILE" | \
                sed "s|^|https://data.commoncrawl.org/|" > "$BATCH_URLS"
            
            # 启动上传进程
            UPLOAD_PIDS=()
            idx=0
            
            while IFS= read -r url && [ "$idx" -lt "$THREADS" ]; do
                filename=$(basename "$url")
                
                rclone copyurl "$url" "$REMOTE:$DEST_PATH" \
                    --auto-filename \
                    --drive-chunk-size "$CHUNK_SIZE" \
                    --buffer-size "$BUFFER_SIZE" \
                    --multi-thread-streams "$MULTI_THREAD_STREAMS" \
                    --checkers 4 \
                    --disable-http2 \
                    --max-transfer "$MAX_TRANSFER" \
                    --timeout 30m \
                    --retries 2 \
                    --low-level-retries 5 \
                    --stats 5s \
                    --stats-one-line \
                    >> "$LOGFILE" 2>&1 &
                
                UPLOAD_PIDS+=("$!")
                idx=$((idx + 1))
                echo "├─ 🔗 线程 $idx: ${filename:0:35}..."
                sleep 0.1
            done < "$BATCH_URLS"
            
            echo "├─ ⚡ 启动 ${#UPLOAD_PIDS[@]} 个上传线程"
            
            # 监控循环
            monitor_count=0
            slow_count=0
            low_speed_triggered=false
            
            while true; do
                # 检查进程状态
                alive=0
                for pid in "${UPLOAD_PIDS[@]}"; do
                    if ps -p "$pid" > /dev/null 2>&1; then
                        alive=$((alive + 1))
                    fi
                done
                
                # 所有进程完成则退出
                if [ "$alive" -eq 0 ]; then
                    echo -e "\n├─ ✅ 批次进程完成"
                    break
                fi
                
                # 获取监控数据  
                speed=$(get_network_speed "$MAIN_INTERFACE")
                storage_bytes=$(get_node_storage "$REMOTE")
                
                # 安全计算存储GB
                if echo "$storage_bytes" | grep -q '^[0-9]*$' && [ "$storage_bytes" -gt 0 ]; then
                    storage_gb=$((storage_bytes / 1073741824))
                else
                    storage_gb=0
                fi
                
                # 确保speed是数字
                if ! echo "$speed" | grep -q '^[0-9]*$'; then
                    speed=0
                fi
                
                printf "\r├─ 📊 网速: %dMB/s | 活跃: %d | 存储: %dGB" "$speed" "$alive" "$storage_gb"
                
                monitor_count=$((monitor_count + 1))
                
                # 低速检测（跳过前12次，即前60秒）
                if [ "$monitor_count" -gt 12 ]; then
                    if [ "$speed" -lt "$LOW_SPEED_MB" ]; then
                        slow_count=$((slow_count + 5))
                    else
                        slow_count=0
                    fi
                    
                    # 触发低速处理
                    if [ "$slow_count" -ge "$LOW_SPEED_SECONDS" ]; then
                        echo -e "\n├─ 🐌 检测到低速: ${speed}MB/s < ${LOW_SPEED_MB}MB/s"
                        low_speed_triggered=true
                        break
                    fi
                fi
                
                # 超时检测（10分钟）
                if [ "$monitor_count" -gt 120 ]; then
                    echo -e "\n├─ ⏰ 批次监控超时"
                    break
                fi
                
                sleep 5
            done
            
            # 处理异常情况
            if [ "$low_speed_triggered" = true ] || [ "$monitor_count" -gt 120 ]; then
                cleanup_processes "$REMOTE"
            fi
            
            # 验证批次结果
            NEW_STORAGE=$(verify_batch_completion "$REMOTE" "$LAST_STORAGE")
            
            if [ "$NEW_STORAGE" -gt "$LAST_STORAGE" ]; then
                size_diff_gb=$(echo "scale=2; ($NEW_STORAGE - $LAST_STORAGE) / 1073741824" | bc -l)
                echo -e "\n├─ ✅ 批次成功 | 新增 ${size_diff_gb}GB"
                LAST_STORAGE=$NEW_STORAGE
                NO_PROGRESS=0
            else
                echo -e "\n├─ ⚠️ 无进展 (${NO_PROGRESS}/3)"
                NO_PROGRESS=$((NO_PROGRESS + 1))
            fi
            
            # 连续失败则切换节点
            if [ "$NO_PROGRESS" -ge 3 ]; then
                echo "└─ 🚫 连续失败，切换下一个节点"
                break
            fi
            
            # 更新进度
            CURRENT_LINE=$((BATCH_END + 1))
            echo "$CURRENT_LINE" > "$PROGRESS_FILE"
            rm -f "$BATCH_URLS"
        done
        
        echo -e "└─ ✅ 节点 \033[38;5;82m$REMOTE\033[0m 处理完成"
    done
    
    # 检查是否循环
    if [ "$REPEAT_INTERVAL_HOURS" -eq 0 ]; then
        echo -e "\n\033[38;5;46m🎉 所有任务完成！\033[0m"
        break
    fi
    
    echo -e "\n\033[38;5;226m💤 休眠 ${REPEAT_INTERVAL_HOURS}小时后继续...\033[0m"
    sleep $((REPEAT_INTERVAL_HOURS * 3600))
done

echo -e "\n\033[38;5;46m🎉 脚本执行完毕！\033[0m"
