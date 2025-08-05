#!/bin/bash
# Google Drive Expander v7 - 谷歌网盘扩充器
# Author: DX
# ✨ 核心特性：
#   - 稳定可靠的网卡速度监控
#   - 智能节点切换和错误恢复
#   - 防卡死机制和完善错误处理

set -euo pipefail

###################### 基本配置 ######################
WARC_LIST_URL="https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-30/warc.paths.gz"
DEST_PATH="/dx"
MAX_TRANSFER="700G"

# 网络优化配置
THREADS=16
CHUNK_SIZE="256M"
BUFFER_SIZE="2G"
MULTI_THREAD_STREAMS=8
CHECKERS=32

# 监控配置
LOW_SPEED_MB=5         # 低于 5 MB/s
LOW_SPEED_SECONDS=60   # 持续 60 秒判为低速
BATCH_TIMEOUT=600      # 批次超时时间（秒）
MONITOR_INTERVAL=5     # 监控间隔（秒）

TMP_DIR="/tmp/warc_uploader"
LOG_DIR="./logs"
mkdir -p "$TMP_DIR" "$LOG_DIR"

export LANG=zh_CN.UTF-8
export LC_ALL=zh_CN.UTF-8

###################### 调试和日志函数 ######################
debug_log() {
    local msg="$1"
    echo "[DEBUG $(date '+%H:%M:%S')] $msg" >&2
}

error_log() {
    local msg="$1"
    echo "[ERROR $(date '+%H:%M:%S')] $msg" >&2
}

###################### 网卡监控函数（简化重写） ######################
# 使用 vnstat 或 iftop 风格的简单监控
get_network_speed_simple() {
    local interface="$1"
    local speed_file="$TMP_DIR/net_${interface}.tmp"
    
    # 使用 cat /proc/net/dev 获取网络统计
    local stats_line=$(grep "$interface:" /proc/net/dev 2>/dev/null || echo "")
    if [[ -z "$stats_line" ]]; then
        echo "0"
        return
    fi
    
    # 提取发送字节数（第10列）
    local current_bytes=$(echo "$stats_line" | awk '{print $10}')
    local current_time=$(date +%s)
    
    if [[ ! "$current_bytes" =~ ^[0-9]+$ ]]; then
        echo "0"
        return
    fi
    
    # 检查是否有历史数据
    if [[ -f "$speed_file" ]]; then
        local prev_data=$(<"$speed_file")
        local prev_bytes prev_time
        IFS=' ' read -r prev_bytes prev_time <<< "$prev_data"
        
        if [[ "$prev_bytes" =~ ^[0-9]+$ ]] && [[ "$prev_time" =~ ^[0-9]+$ ]]; then
            local time_diff=$((current_time - prev_time))
            local bytes_diff=$((current_bytes - prev_bytes))
            
            if (( time_diff >= 5 && bytes_diff >= 0 )); then
                # 计算MB/s
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
    
    # 保存当前数据
    echo "$current_bytes $current_time" > "$speed_file"
}

# 重置网卡监控
reset_network_monitor() {
    local interface="$1"
    rm -f "$TMP_DIR/net_${interface}.tmp"
    # 等待一个周期后初始化
    sleep 6
    get_network_speed_simple "$interface" > /dev/null
}

# 获取主要网卡接口
get_main_interface() {
    local interface=$(ip route | grep '^default' | awk '{print $5}' | head -1)
    interface=${interface%%@*}  # 处理容器环境
    
    if [[ -d "/sys/class/net/$interface" ]]; then
        echo "$interface"
    else
        # 备用方案
        for iface in /sys/class/net/*; do
            local name=$(basename "$iface")
            [[ "$name" != "lo" ]] && [[ -f "$iface/statistics/tx_bytes" ]] && {
                echo "$name"
                return
            }
        done
    fi
}

###################### 进程管理函数 ######################
# 获取活跃rclone进程数
get_active_threads() {
    local remote="$1"
    pgrep -cf "rclone.*$remote:" 2>/dev/null || echo "0"
}

# 强制终止所有相关进程
force_kill_processes() {
    local remote="$1"
    local pids=($(pgrep -f "rclone.*$remote:" 2>/dev/null || true))
    
    if [[ ${#pids[@]} -gt 0 ]]; then
        debug_log "终止 ${#pids[@]} 个rclone进程"
        for pid in "${pids[@]}"; do
            kill -TERM "$pid" 2>/dev/null || true
        done
        sleep 3
        
        # 强制杀死顽固进程
        for pid in "${pids[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                kill -KILL "$pid" 2>/dev/null || true
            fi
        done
    fi
}

# 安全的进程等待函数
wait_for_processes() {
    local -a pids=("$@")
    local timeout_count=0
    local max_timeout=200  # 10分钟
    
    while (( timeout_count < max_timeout )); do
        local alive=0
        for pid in "${pids[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                alive=$((alive+1))
            fi
        done
        
        if (( alive == 0 )); then
            debug_log "所有进程已完成"
            return 0
        fi
        
        # 每30秒输出调试信息
        if (( timeout_count % 10 == 0 && timeout_count > 0 )); then
            debug_log "等待进程完成... 剩余: $alive 个"
        fi
        
        timeout_count=$((timeout_count+1))
        sleep 3
    done
    
    error_log "进程等待超时，强制终止"
    for pid in "${pids[@]}"; do
        kill -KILL "$pid" 2>/dev/null || true
    done
    return 1
}

###################### 存储管理函数 ######################
# 获取节点存储量
get_node_storage() {
    local remote="$1"
    timeout 15 rclone size "$remote:$DEST_PATH" --json 2>/dev/null | \
        jq -r '.bytes // 0' 2>/dev/null || echo "0"
}

# 验证批次完成
verify_batch_completion() {
    local remote="$1"
    local last_bytes="$2"
    local attempts=0
    local max_attempts=6
    
    while (( attempts < max_attempts )); do
        local current_bytes=$(get_node_storage "$remote")
        if [[ "$current_bytes" =~ ^[0-9]+$ ]] && (( current_bytes > last_bytes )); then
            echo "$current_bytes"
            return 0
        fi
        attempts=$((attempts+1))
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
║                       谷歌网盘扩充器                                            ║
║                                                                              ║
║                     🚀 稳定可靠 • 智能监控 • 防卡死                              ║
║                     ⚡ 2.5Gbps优化 • 全自动化部署                              ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
EOF
    echo -e "\033[0m"
    
    echo -e "\033[38;5;46m🔧 系统配置\033[0m"
    echo "   ├─ 并发线程: $THREADS"
    echo "   ├─ 分块大小: $CHUNK_SIZE" 
    echo "   ├─ 缓存大小: $BUFFER_SIZE"
    echo "   ├─ 流数量: $MULTI_THREAD_STREAMS"
    echo "   └─ 低速阈值: ${LOW_SPEED_MB}MB/s (${LOW_SPEED_SECONDS}秒)"
    echo
}

###################### 主程序开始 ######################
show_banner

# 检查依赖
for cmd in bc jq curl rclone; do
    if ! command -v "$cmd" &> /dev/null; then
        error_log "缺少必要工具: $cmd"
        exit 1
    fi
done

# 获取网卡信息
MAIN_INTERFACE=$(get_main_interface)
if [[ -z "$MAIN_INTERFACE" ]]; then
    error_log "无法检测主网卡接口"
    echo "   可用接口: $(ls /sys/class/net/ | grep -v lo | tr '\n' ' ')"
    exit 1
fi

INTERFACE_IP=$(ip addr show "$MAIN_INTERFACE" | grep 'inet ' | awk '{print $2}' | head -1)
echo -e "\033[38;5;226m🌐 网络接口\033[0m: $MAIN_INTERFACE ($INTERFACE_IP)"

# 初始化网卡监控
if ! init_network_monitor "$MAIN_INTERFACE"; then
    error_log "网卡监控初始化失败"
    exit 1
fi

# 测试网卡速度
sleep 2  # 等待一下再测试
TEST_SPEED=$(get_network_speed "$MAIN_INTERFACE")
echo "   └─ 初始上传速度: ${TEST_SPEED}MB/s"

# 用户交互
DEFAULT_REPEAT=25
echo -e "\033[38;5;51m⏰ 循环间隔\033[0m"
read -rp "   循环间隔小时数 (默认${DEFAULT_REPEAT}小时，0=仅执行一次): " REPEAT_INTERVAL_HOURS
REPEAT_INTERVAL_HOURS=${REPEAT_INTERVAL_HOURS:-$DEFAULT_REPEAT}

# 节点选择
ALL_REMOTES=($(rclone listremotes | sed 's/:$//'))
if [[ ${#ALL_REMOTES[@]} -eq 0 ]]; then
    error_log "未检测到rclone存储节点"
    exit 1
fi

echo -e "\n\033[38;5;51m🟢 存储节点\033[0m (共${#ALL_REMOTES[@]}个)"
for i in "${!ALL_REMOTES[@]}"; do
    echo "   ├─ [$((i+1))] ${ALL_REMOTES[i]}"
done

echo -e "\n\033[38;5;196m🎯 节点选择\033[0m"
read -rp "   选择节点 (默认全选并随机排序，数字用空格分隔): " NODE_SELECTION

if [[ -z "$NODE_SELECTION" ]]; then
    SELECTED_REMOTES=($(printf '%s\n' "${ALL_REMOTES[@]}" | shuf))
    echo "   ✅ 已选择全部节点并随机排序"
else
    SELECTED_REMOTES=()
    for num in $NODE_SELECTION; do
        if [[ $num =~ ^[0-9]+$ ]] && (( num >= 1 && num <= ${#ALL_REMOTES[@]} )); then
            SELECTED_REMOTES+=("${ALL_REMOTES[$((num-1))]}")
        fi
    done
fi

if [[ ${#SELECTED_REMOTES[@]} -eq 0 ]]; then
    error_log "未选择有效节点"
    exit 1
fi

echo "   已选择: ${SELECTED_REMOTES[*]}"

read -rp "📍 起始文件行号 (默认1): " START_LINE
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
        FLAG_FILE="$TMP_DIR/${REMOTE}_flag"
        
        [[ -f "$PROGRESS_FILE" ]] || echo "$START_LINE" > "$PROGRESS_FILE"
        CURRENT_LINE=$(<"$PROGRESS_FILE")
        
        # 重新初始化网卡监控
        init_network_monitor "$MAIN_INTERFACE"
        
        # 获取初始存储量
        LAST_STORAGE=$(get_node_storage "$REMOTE")
        debug_log "节点 $REMOTE 初始存储: $LAST_STORAGE bytes"
        
        NO_PROGRESS=0
        
        while (( CURRENT_LINE <= TOTAL_LINES )); do
            BATCH_END=$(( CURRENT_LINE + THREADS - 1 ))
            (( BATCH_END > TOTAL_LINES )) && BATCH_END=$TOTAL_LINES
            
            echo -e "\n├─ 🚀 批次 $CURRENT_LINE-$BATCH_END 共 $((BATCH_END-CURRENT_LINE+1)) 文件"
            
            # 准备批次文件
            BATCH_URLS="$TMP_DIR/${REMOTE}_urls_${CURRENT_LINE}.txt"
            sed -n "${CURRENT_LINE},${BATCH_END}p" "$WARC_FILE" | \
                sed "s|^|https://data.commoncrawl.org/|" > "$BATCH_URLS"
            
            # 初始化标志
            echo "0" > "$FLAG_FILE"
            
            # 启动网卡监控
            monitor_network() {
                local slow_count=0
                local check_count=0
                
                while [[ $(<"$FLAG_FILE") == "0" ]]; do
                    sleep $MONITOR_INTERVAL
                    check_count=$((check_count+1))
                    
                    local speed=$(get_network_speed "$MAIN_INTERFACE")
                    local active=$(get_active_threads "$REMOTE")
                    local storage_gb=$(($(get_node_storage "$REMOTE") / 1073741824))
                    
                    # 确保都是数字
                    [[ ! "$speed" =~ ^[0-9]+$ ]] && speed=0
                    [[ ! "$active" =~ ^[0-9]+$ ]] && active=0
                    [[ ! "$storage_gb" =~ ^[0-9]+$ ]] && storage_gb=0
                    
                    printf "\r├─ 📊 网卡: %dMB/s | 线程: %d | 存储: %dGB" "$speed" "$active" "$storage_gb"
                    
                    # 前60秒不检测
                    if (( check_count <= 12 )); then
                        continue
                    fi
                    
                    # 低速检测
                    if (( speed < LOW_SPEED_MB )); then
                        slow_count=$((slow_count + MONITOR_INTERVAL))
                    else
                        slow_count=0
                    fi
                    
                    # 触发低速切换
                    if (( slow_count >= LOW_SPEED_SECONDS )); then
                        echo -e "\n├─ 🐌 低速触发: ${speed}MB/s < ${LOW_SPEED_MB}MB/s"
                        echo "1" > "$FLAG_FILE"
                        return
                    fi
                    
                    # 批次超时
                    if (( check_count * MONITOR_INTERVAL >= BATCH_TIMEOUT )); then
                        echo -e "\n├─ ⏰ 批次超时"
                        echo "1" > "$FLAG_FILE"
                        return
                    fi
                done
            }
            
            # 启动监控
            monitor_network &
            MONITOR_PID=$!
            
            # 启动上传进程
            UPLOAD_PIDS=()
            local idx=0
            
            while IFS= read -r url && (( idx < THREADS )); do
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
                idx=$((idx+1))
                echo "├─ 🔗 线程 $idx: ${filename:0:35}..."
                sleep 0.2
            done < "$BATCH_URLS"
            
            echo "├─ ⚡ 启动 ${#UPLOAD_PIDS[@]} 个上传线程"
            
            # 等待完成或中断
            if [[ $(<"$FLAG_FILE") == "1" ]]; then
                echo -e "\n├─ 🛑 监控触发，终止批次"
                force_kill_processes "$REMOTE"
            else
                echo -e "\n├─ ⏳ 等待批次完成..."
                wait_for_processes "${UPLOAD_PIDS[@]}"
            fi
            
            # 停止监控
            kill -TERM "$MONITOR_PID" 2>/dev/null || true
            
            # 批次统计
            FINAL_SPEED=$(get_network_speed "$MAIN_INTERFACE")
            [[ ! "$FINAL_SPEED" =~ ^[0-9]+$ ]] && FINAL_SPEED=0
            
            NEW_STORAGE=$(verify_batch_completion "$REMOTE" "$LAST_STORAGE")
            
            if (( NEW_STORAGE > LAST_STORAGE )); then
                size_diff_gb=$(echo "scale=2; ($NEW_STORAGE - $LAST_STORAGE) / 1073741824" | bc -l)
                echo -e "\n├─ ✅ 批次完成 | 新增 ${size_diff_gb}GB | 速度 ${FINAL_SPEED}MB/s"
                LAST_STORAGE=$NEW_STORAGE
                NO_PROGRESS=0
            else
                echo -e "\n├─ ⚠️ 无变化 (${NO_PROGRESS}/3) | 速度 ${FINAL_SPEED}MB/s"
                NO_PROGRESS=$((NO_PROGRESS+1))
            fi
            
            # 连续失败则切换节点
            if (( NO_PROGRESS >= 3 )); then
                echo "└─ 🚫 连续失败，切换节点"
                break
            fi
            
            # 更新进度
            CURRENT_LINE=$(( BATCH_END + 1 ))
            echo "$CURRENT_LINE" > "$PROGRESS_FILE"
            rm -f "$BATCH_URLS"
        done
        
        # 清理
        rm -f "$FLAG_FILE"
        echo -e "└─ ✅ 节点 \033[38;5;82m$REMOTE\033[0m 完成"
    done
    
    # 检查是否循环
    if (( REPEAT_INTERVAL_HOURS == 0 )); then
        echo -e "\n\033[38;5;46m🎉 任务完成！\033[0m"
        break
    fi
    
    echo -e "\n\033[38;5;226m💤 休眠 ${REPEAT_INTERVAL_HOURS}h...\033[0m"
    sleep $(( REPEAT_INTERVAL_HOURS * 3600 ))
done
