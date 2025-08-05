# 获取活跃进程数
get_active_count() {
    local remote="$1"
    pgrep -c -f "rclone.*$remote:" 2>/dev/null || echo "0"
}#!/bin/bash
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

# 调试模式（设置为1启用调试输出）
DEBUG_MODE=${DEBUG_MODE:-0}

###################### 调试和日志函数 ######################
debug_log() {
    local msg="$1"
    [[ "$DEBUG_MODE" == "1" ]] && echo "[DEBUG $(date '+%H:%M:%S')] $msg" >&2
}

error_log() {
    local msg="$1"
    echo "[ERROR $(date '+%H:%M:%S')] $msg" >&2
}

###################### 网卡监控函数（简化重写） ######################
# 重置网卡监控
reset_network_monitor() {
    local interface="$1"
    local speed_file="$TMP_DIR/net_${interface}.tmp"
    
    # 清理旧数据
    rm -f "$speed_file"
    debug_log "重置网卡监控: $interface"
    
    # 立即获取一次基准数据
    local stats_line=$(grep "$interface:" /proc/net/dev 2>/dev/null | head -1)
    if [[ -n "$stats_line" ]]; then
        local current_bytes=$(echo "$stats_line" | awk '{print $10}')
        local current_time=$(date +%s)
        if [[ "$current_bytes" =~ ^[0-9]+$ ]]; then
            echo "$current_bytes $current_time" > "$speed_file"
            debug_log "网卡监控初始化完成，基准字节数: $current_bytes"
        else
            debug_log "获取网卡基准数据失败"
        fi
    else
        debug_log "网卡 $interface 不存在"
    fi
}

# 使用 vnstat 或 iftop 风格的简单监控
get_network_speed_simple() {
    local interface="$1"
    local speed_file="$TMP_DIR/net_${interface}.tmp"
    
    # 使用 cat /proc/net/dev 获取网络统计
    local stats_line=$(grep "$interface:" /proc/net/dev 2>/dev/null | head -1)
    if [[ -z "$stats_line" ]]; then
        debug_log "网卡 $interface 在 /proc/net/dev 中未找到"
        echo "0"
        return
    fi
    
    # 提取发送字节数（第10列）
    local current_bytes=$(echo "$stats_line" | awk '{print $10}')
    local current_time=$(date +%s)
    
    if [[ ! "$current_bytes" =~ ^[0-9]+$ ]]; then
        debug_log "无效的字节数: $current_bytes"
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
            
            debug_log "速度计算: 字节差=$bytes_diff, 时间差=$time_diff"
            
            if (( time_diff >= 5 && bytes_diff >= 0 )); then
                # 计算MB/s
                local speed_mb=$((bytes_diff / time_diff / 1048576))
                debug_log "计算速度: ${speed_mb}MB/s"
                echo "$speed_mb"
            else
                debug_log "时间差或字节差无效"
                echo "0"
            fi
        else
            debug_log "历史数据无效"
            echo "0"
        fi
    else
        debug_log "无历史数据，返回0"
        echo "0"
    fi
    
    # 保存当前数据
    echo "$current_bytes $current_time" > "$speed_file"
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

###################### 进程管理函数（简化重写） ######################
# 简单的进程等待（防卡死版）
simple_wait_processes() {
    local -a pids=("$@")
    local wait_count=0
    local max_wait=120  # 最多等待6分钟
    
    while (( wait_count < max_wait )); do
        local alive=0
        
        for pid in "${pids[@]}"; do
            if ps -p "$pid" > /dev/null 2>&1; then
                alive=$((alive+1))
            fi
        done
        
        if (( alive == 0 )); then
            echo -e "\n├─ ✅ 所有进程完成"
            return 0
        fi
        
        # 每分钟输出一次状态
        if (( wait_count % 20 == 0 && wait_count > 0 )); then
            echo -e "\n├─ ⏳ 等待中... 剩余 $alive 个进程"
        fi
        
        wait_count=$((wait_count+1))
        sleep 3
    done
    
    # 超时强制终止
    echo -e "\n├─ ⚠️ 等待超时，强制终止"
    for pid in "${pids[@]}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
    sleep 2
    return 1
}

# 简单的进程清理
simple_cleanup() {
    local remote="$1"
    echo "├─ 🧹 清理进程..."
    
    # 获取所有相关进程
    local pids=($(pgrep -f "rclone.*$remote:" 2>/dev/null || true))
    
    if [[ ${#pids[@]} -gt 0 ]]; then
        for pid in "${pids[@]}"; do
            kill -9 "$pid" 2>/dev/null || true
        done
        sleep 2
        echo "├─ ✅ 清理完成"
    else
        echo "├─ ✅ 无需清理"
    fi
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

# 测试网卡监控
echo "   └─ 正在初始化网卡监控..."
echo "   └─ 网卡统计文件: /proc/net/dev"
if grep -q "$MAIN_INTERFACE:" /proc/net/dev; then
    echo "   └─ 网卡监控就绪"
    # 初始化网卡监控
    reset_network_monitor "$MAIN_INTERFACE"
else
    echo "   └─ ⚠️ 网卡在 /proc/net/dev 中未找到"
    echo "   └─ 可用网卡列表:"
    cat /proc/net/dev | grep ":" | awk -F: '{print "       " $1}' | sed 's/^ *//'
fi
echo "   💡 如需调试模式，运行: DEBUG_MODE=1 $0"

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
        
        [[ -f "$PROGRESS_FILE" ]] || echo "$START_LINE" > "$PROGRESS_FILE"
        CURRENT_LINE=$(<"$PROGRESS_FILE")
        
        # 重新初始化网卡监控
        reset_network_monitor "$MAIN_INTERFACE"
        
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
                sleep 0.1
            done < "$BATCH_URLS"
            
            echo "├─ ⚡ 启动 ${#UPLOAD_PIDS[@]} 个上传线程"
            
            # 简化的监控循环（主线程）
            local monitor_count=0
            local slow_count=0
            local low_speed_triggered=false
            
            while true; do
                # 检查进程状态
                local alive=0
                for pid in "${UPLOAD_PIDS[@]}"; do
                    if ps -p "$pid" > /dev/null 2>&1; then
                        alive=$((alive+1))
                    fi
                done
                
                # 如果所有进程完成，退出监控
                if (( alive == 0 )); then
                    echo -e "\n├─ ✅ 所有进程完成"
                    break
                fi
                
                # 获取监控数据
                local speed=$(get_network_speed_simple "$MAIN_INTERFACE")
                local storage_gb=$(($(get_node_storage "$REMOTE") / 1073741824))
                
                # 确保是数字
                [[ ! "$speed" =~ ^[0-9]+$ ]] && speed=0
                [[ ! "$storage_gb" =~ ^[0-9]+$ ]] && storage_gb=0
                
                printf "\r├─ 📊 网卡: %dMB/s | 线程: %d | 存储: %dGB" "$speed" "$alive" "$storage_gb"
                
                monitor_count=$((monitor_count+1))
                
                # 前60秒不检测低速
                if (( monitor_count > 12 )); then
                    if (( speed < LOW_SPEED_MB )); then
                        slow_count=$((slow_count+5))
                    else
                        slow_count=0
                    fi
                    
                    # 低速触发
                    if (( slow_count >= LOW_SPEED_SECONDS )); then
                        echo -e "\n├─ 🐌 低速触发: ${speed}MB/s < ${LOW_SPEED_MB}MB/s"
                        low_speed_triggered=true
                        break
                    fi
                fi
                
                # 超时检测
                if (( monitor_count > 120 )); then  # 10分钟
                    echo -e "\n├─ ⏰ 批次超时"
                    break
                fi
                
                sleep 5
            done
            
            # 清理进程
            if [[ "$low_speed_triggered" == true ]] || (( monitor_count > 120 )); then
                simple_cleanup "$REMOTE"
            fi
            
            # 批次统计
            FINAL_SPEED=$(get_network_speed_simple "$MAIN_INTERFACE")
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
