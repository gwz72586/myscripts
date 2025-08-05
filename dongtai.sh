#!/bin/bash
# Google Drive Expander v7 - 7G内存优化版
# Author: DX
# ✨ 针对7G内存VM优化：智能并发控制、内存安全、零等待

###################### 7G内存优化配置 ######################
WARC_LIST_URL="https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-30/warc.paths.gz"
DEST_PATH="/dx"
MAX_TRANSFER="700G"

# 7G内存安全配置
MAX_CONCURRENT=16      # 最大并发（适配7G内存）
BUFFER_SIZE="128M"     # 安全缓存大小（128M×16=2G总缓存）
CHUNK_SIZE="256M"      # 分块大小
MULTI_THREAD_STREAMS=4 # 每进程流数（减少内存使用）
CHECKERS=16            # 检查器数量

# 简化的动态管理
MIN_CONCURRENT=8       # 最小保持并发
REFILL_THRESHOLD=4     # 补充阈值

# 监控配置
LOW_SPEED_MB=8
LOW_SPEED_SECONDS=45
SPEED_CHECK_INTERVAL=3

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
    
    local stats_line
    stats_line=$(grep "^[ ]*${interface}[:@]" /proc/net/dev 2>/dev/null | head -1)
    
    if [ -z "$stats_line" ]; then
        echo "0"
        return
    fi
    
    local current_bytes current_time
    current_bytes=$(echo "$stats_line" | awk '{print $10}')
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
                
                if [ "$time_diff" -ge 3 ] && [ "$bytes_diff" -ge 0 ]; then
                    echo $((bytes_diff / time_diff / 1048576))
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

###################### 简化的进程管理 ######################
# 获取活跃进程数
get_active_count() {
    local remote="$1"
    pgrep -c -f "rclone.*$remote:" 2>/dev/null || echo "0"
}

# 获取活跃进程PID列表
get_active_pids() {
    local remote="$1"
    pgrep -f "rclone.*$remote:" 2>/dev/null || true
}

# 启动单个上传进程
start_upload_process() {
    local remote="$1"
    local url="$2"
    local logfile="$3"
    
    rclone copyurl "$url" "$remote:$DEST_PATH" \
        --auto-filename \
        --drive-chunk-size "$CHUNK_SIZE" \
        --buffer-size "$BUFFER_SIZE" \
        --multi-thread-streams "$MULTI_THREAD_STREAMS" \
        --checkers "$CHECKERS" \
        --disable-http2 \
        --max-transfer "$MAX_TRANSFER" \
        --timeout 30m \
        --retries 2 \
        --low-level-retries 5 \
        --stats 3s \
        --stats-one-line \
        --transfers 1 \
        >> "$logfile" 2>&1 &
    
    echo "$!"
}

# 清理所有进程
cleanup_all_processes() {
    local remote="$1"
    local pids
    pids=$(get_active_pids "$remote")
    
    if [ -n "$pids" ]; then
        echo "$pids" | xargs -r kill -9 2>/dev/null || true
        sleep 1
    fi
}

###################### 存储管理函数 ######################
get_node_storage() {
    local remote="$1"
    timeout 15 rclone size "$remote:$DEST_PATH" --json 2>/dev/null | \
        jq -r '.bytes // 0' 2>/dev/null || echo "0"
}

###################### 启动界面 ######################
show_banner() {
    clear
    echo -e "\033[38;5;39m"
    cat << 'EOF'
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║   ██████╗  ██████╗  ██████╗  ██████╗ ██╗     ███████╗    ██████╗ ██╗  ██╗   ║
║  ██╔════╝ ██╔═══██╗██╔═══██╗██╔════╝ ████╗   ██╔════╝    ██╔══██╗╚██╗██╔╝   ║
║  ██║  ███╗██║   ██║██║   ██║██║  ███╗██╔██╗  █████╗      ██║  ██║ ╚███╔╝    ║
║  ██║   ██║██║   ██║██║   ██║██║   ██║██║╚██╗ ██╔══╝      ██║  ██║ ██╔██╗    ║
║  ╚██████╔╝╚██████╔╝╚██████╔╝╚██████╔╝██║ ╚██╗███████╗    ██████╔╝██╔╝ ██╗   ║
║   ╚═════╝  ╚═════╝  ╚═════╝  ╚═════╝ ╚═╝  ╚═╝╚══════╝    ╚═════╝ ╚═╝  ╚═╝   ║
║                                                                              ║
║              🚀 GOOGLE DRIVE EXPANDER v7.0 - DX 🚀                          ║
║                   谷歌网盘扩充器 (7G内存优化版)                                 ║
║                                                                              ║
║               ⚡ 内存安全 • 智能并发 • 零等待上传                                 ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
EOF
    echo -e "\033[0m"
    
    echo -e "\033[38;5;46m🔧 7G内存优化配置\033[0m"
    echo "   ├─ 最大并发: $MAX_CONCURRENT 进程"
    echo "   ├─ 缓存大小: $BUFFER_SIZE (总计 $(( ${BUFFER_SIZE%M} * MAX_CONCURRENT / 1024 ))G)"
    echo "   ├─ 分块大小: $CHUNK_SIZE"
    echo "   ├─ 预计内存: ~2.5G (安全边际充足)"
    echo "   └─ 策略: 动态补充，保持满载"
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

# 内存检查
total_mem=$(free -g | awk '/^Mem:/{print $2}')
if [ "$total_mem" -lt 6 ]; then
    echo -e "\033[38;5;196m⚠️ 警告: 检测到内存不足 ${total_mem}GB，建议至少6GB\033[0m"
    read -p "   是否继续? (y/N): " continue_choice
    if [ "$continue_choice" != "y" ] && [ "$continue_choice" != "Y" ]; then
        exit 1
    fi
fi

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

###################### 简化的动态上传循环 ######################
echo -e "\n\033[38;5;51m🚀 启动智能并发上传...\033[0m"

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
        
        # 清理旧进程
        cleanup_all_processes "$REMOTE"
        
        # 重新初始化网卡监控
        reset_network_monitor "$MAIN_INTERFACE"
        
        # 获取初始存储量
        LAST_STORAGE=$(get_node_storage "$REMOTE")
        LAST_CHECK_TIME=$(date +%s)
        
        echo "├─ 📊 初始存储: $((LAST_STORAGE / 1073741824))GB"
        echo "├─ 🔄 启动智能并发池 (目标: $MAX_CONCURRENT 进程)"
        
        # 进程队列管理
        UPLOAD_PIDS=()
        slow_count=0
        monitor_count=0
        no_progress_count=0
        
        # 主上传循环
        while [ "$CURRENT_LINE" -le "$TOTAL_LINES" ]; do
            # 清理完成的进程
            NEW_PIDS=()
            for pid in "${UPLOAD_PIDS[@]}"; do
                if ps -p "$pid" > /dev/null 2>&1; then
                    NEW_PIDS+=("$pid")
                fi
            done
            UPLOAD_PIDS=("${NEW_PIDS[@]}")
            
            active_count=${#UPLOAD_PIDS[@]}
            
            # 补充进程到最大并发
            while [ "$active_count" -lt "$MAX_CONCURRENT" ] && [ "$CURRENT_LINE" -le "$TOTAL_LINES" ]; do
                url=$(sed -n "${CURRENT_LINE}p" "$WARC_FILE" | sed 's|^|https://data.commoncrawl.org/|')
                if [ -n "$url" ]; then
                    new_pid=$(start_upload_process "$REMOTE" "$url" "$LOGFILE")
                    if [ -n "$new_pid" ]; then
                        UPLOAD_PIDS+=("$new_pid")
                        active_count=$((active_count + 1))
                        debug_log "启动进程 $new_pid: $(basename "$url")"
                    fi
                fi
                CURRENT_LINE=$((CURRENT_LINE + 1))
                echo "$CURRENT_LINE" > "$PROGRESS_FILE"
                sleep 0.1
            done
            
            # 如果没有活跃进程了，说明都完成了
            if [ "$active_count" -eq 0 ] && [ "$CURRENT_LINE" -gt "$TOTAL_LINES" ]; then
                break
            fi
            
            # 获取监控数据
            speed=$(get_network_speed "$MAIN_INTERFACE")
            storage_bytes=$(get_node_storage "$REMOTE")
            
            # 计算存储GB
            if echo "$storage_bytes" | grep -q '^[0-9]*$' && [ "$storage_bytes" -gt 0 ]; then
                storage_gb=$((storage_bytes / 1073741824))
            else
                storage_gb=0
            fi
            
            if ! echo "$speed" | grep -q '^[0-9]*$'; then
                speed=0
            fi
            
            # 显示实时状态
            printf "\r├─ 📊 速度: %dMB/s | 活跃: %d/%d | 存储: %dGB | 进度: %d/%d (%.1f%%)" \
                "$speed" "$active_count" "$MAX_CONCURRENT" "$storage_gb" "$CURRENT_LINE" "$TOTAL_LINES" \
                "$(echo "scale=1; $CURRENT_LINE * 100 / $TOTAL_LINES" | bc -l)"
            
            monitor_count=$((monitor_count + 1))
            
            # 低速检测 (45秒后开始)
            if [ "$monitor_count" -gt 15 ]; then
                if [ "$speed" -lt "$LOW_SPEED_MB" ]; then
                    slow_count=$((slow_count + SPEED_CHECK_INTERVAL))
                else
                    slow_count=0
                fi
                
                if [ "$slow_count" -ge "$LOW_SPEED_SECONDS" ]; then
                    echo -e "\n├─ 🐌 检测到持续低速 (${speed}MB/s < ${LOW_SPEED_MB}MB/s)，切换节点"
                    break
                fi
            fi
            
            # 进度检测 (每分钟)
            if [ $((monitor_count % 20)) -eq 0 ] && [ "$monitor_count" -gt 0 ]; then
                current_time=$(date +%s)
                if [ "$storage_bytes" -gt "$LAST_STORAGE" ]; then
                    size_diff_gb=$(echo "scale=2; ($storage_bytes - $LAST_STORAGE) / 1073741824" | bc -l)
                    time_diff=$((current_time - LAST_CHECK_TIME))
                    if [ "$time_diff" -gt 0 ]; then
                        speed_gb_min=$(echo "scale=2; $size_diff_gb * 60 / $time_diff" | bc -l)
                        echo -e "\n├─ 📈 进展: +${size_diff_gb}GB (${speed_gb_min}GB/min)"
                    fi
                    LAST_STORAGE=$storage_bytes
                    LAST_CHECK_TIME=$current_time
                    no_progress_count=0
                else
                    no_progress_count=$((no_progress_count + 1))
                    if [ "$no_progress_count" -ge 3 ]; then
                        echo -e "\n├─ ⚠️ 连续3分钟无进展，切换节点"
                        break
                    fi
                fi
            fi
            
            sleep "$SPEED_CHECK_INTERVAL"
        done
        
        # 等待剩余进程完成
        if [ ${#UPLOAD_PIDS[@]} -gt 0 ]; then
            echo -e "\n├─ ⏳ 等待剩余 ${#UPLOAD_PIDS[@]} 个进程完成..."
            wait_count=0
            while [ ${#UPLOAD_PIDS[@]} -gt 0 ] && [ "$wait_count" -lt 60 ]; do
                NEW_PIDS=()
                for pid in "${UPLOAD_PIDS[@]}"; do
                    if ps -p "$pid" > /dev/null 2>&1; then
                        NEW_PIDS+=("$pid")
                    fi
                done
                UPLOAD_PIDS=("${NEW_PIDS[@]}")
                
                if [ ${#UPLOAD_PIDS[@]} -gt 0 ]; then
                    printf "\r├─ ⏳ 剩余: %d 个进程" "${#UPLOAD_PIDS[@]}"
                    sleep 3
                    wait_count=$((wait_count + 1))
                fi
            done
            
            # 强制清理超时进程
            if [ ${#UPLOAD_PIDS[@]} -gt 0 ]; then
                echo -e "\n├─ ⏰ 等待超时，强制清理"
                for pid in "${UPLOAD_PIDS[@]}"; do
                    kill -9 "$pid" 2>/dev/null || true
                done
            fi
        fi
        
        # 最终清理
        cleanup_all_processes "$REMOTE"
        
        echo -e "\n└─ ✅ 节点 \033[38;5;82m$REMOTE\033[0m 处理完成"
    done
    
    # 检查是否循环
    if [ "$REPEAT_INTERVAL_HOURS" -eq 0 ]; then
        echo -e "\n\033[38;5;46m🎉 所有任务完成！\033[0m"
        break
    fi
    
    echo -e "\n\033[38;5;226m💤 休眠 ${REPEAT_INTERVAL_HOURS}小时后继续...\033[0m"
    sleep $((REPEAT_INTERVAL_HOURS * 3600))
done

echo -e "\n\033[38;5;46m🎉 智能并发上传执行完毕！\033[0m"
