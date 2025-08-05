#!/bin/bash
# CC-G v9 - CommonCrawl Global Uploader (网卡速度监控版)
# ✨ 核心特性：
#   - 直接监控网卡上传速度，简单可靠
#   - 自动低速切换节点 (< 20MB/s 超过30秒)
#   - 默认随机全选节点，25小时循环
#   - 全新大气启动界面

set -euo pipefail

###################### 基本配置 ######################
WARC_LIST_URL="https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-30/warc.paths.gz"
DEST_PATH="/dx"
MAX_TRANSFER="700G"

# 2.5 Gbps 带宽优化
THREADS=16
CHUNK_SIZE="256M"
BUFFER_SIZE="2G"
MULTI_THREAD_STREAMS=8
CHECKERS=32
LOW_SPEED_MB=5         # 低于 5 MB/s
LOW_SPEED_SECONDS=60   # 持续 60 秒判为低速

TMP_DIR="/tmp/warc_uploader"
LOG_DIR="./logs"
mkdir -p "$TMP_DIR" "$LOG_DIR"

export LANG=zh_CN.UTF-8
export LC_ALL=zh_CN.UTF-8

###################### 网卡速度监控函数 ######################

# 获取主要网卡接口
get_main_interface() {
    # 获取默认路由的网卡接口
    local interface=$(ip route | grep '^default' | awk '{print $5}' | head -1)
    
    # 处理容器环境的接口名称 (如 eth0@if20)
    interface=${interface%%@*}
    
    # 验证接口是否存在
    if [[ -d "/sys/class/net/$interface" ]]; then
        echo "$interface"
    else
        # 备用方案：查找第一个有效的网络接口
        for iface in /sys/class/net/*; do
            local name=$(basename "$iface")
            [[ "$name" != "lo" ]] && [[ -f "$iface/statistics/tx_bytes" ]] && {
                echo "$name"
                return
            }
        done
    fi
}

# 获取网卡上传速度 (MB/s)
get_network_upload_speed() {
    local interface="$1"
    local bytes_file="/sys/class/net/$interface/statistics/tx_bytes"
    
    if [[ ! -f "$bytes_file" ]]; then
        echo "0"
        return
    fi
    
    local current_bytes=$(<"$bytes_file")
    local timestamp=$(date +%s)
    local speed_file="$TMP_DIR/network_speed_${interface}"
    
    if [[ -f "$speed_file" ]]; then
        local prev_data=($(<"$speed_file"))
        local prev_bytes=${prev_data[0]}
        local prev_time=${prev_data[1]}
        
        local bytes_diff=$((current_bytes - prev_bytes))
        local time_diff=$((timestamp - prev_time))
        
        if (( time_diff > 0 )); then
            # 转换为 MB/s (1MB = 1000000 bytes)
            local speed_mb=$((bytes_diff / time_diff / 1000000))
            echo "$speed_mb"
        else
            echo "0"
        fi
    else
        echo "0"
    fi
    
    # 保存当前数据
    echo "$current_bytes $timestamp" > "$speed_file"
}

# 获取节点累计上传量 (GB)
get_node_total_uploaded() {
    local remote="$1"
    local current_bytes=$(rclone size "$remote:$DEST_PATH" --json 2>/dev/null | jq -r '.bytes // 0')
    # 转换为 GB (1GB = 1073741824 bytes)
    local gb=$((current_bytes / 1073741824))
    echo "$gb"
}

cleanup() {
    local mon_pid=${1:-}
    [[ -n "$mon_pid" && $(kill -0 "$mon_pid" 2>/dev/null || echo 0) ]] && {
        kill -TERM "$mon_pid" 2>/dev/null || true
        sleep 1
        kill -KILL "$mon_pid" 2>/dev/null || true
    }
    
    # 强制终止所有相关rclone进程
    local pids=($(pgrep -f "rclone copyurl.*$REMOTE:" 2>/dev/null || true))
    for pid in "${pids[@]}"; do
        [[ -n "$pid" ]] && {
            kill -TERM "$pid" 2>/dev/null || true
        }
    done
    
    # 等待进程终止
    sleep 2
    
    # 强制杀死顽固进程
    for pid in "${pids[@]}"; do
        [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null && {
            kill -KILL "$pid" 2>/dev/null || true
        }
    done
}

# 批次容量验证
MAX_VERIFY_ATTEMPTS=6
VERIFY_INTERVAL=30
verify_batch() {
    local last_bytes=$1
    local new_bytes=0 attempts=0
    while (( attempts < MAX_VERIFY_ATTEMPTS )); do
        new_bytes=$(rclone size "$REMOTE:$DEST_PATH" --json 2>/dev/null | jq -r '.bytes // 0')
        (( new_bytes > last_bytes )) && { echo "$new_bytes"; return 0; }
        attempts=$((attempts+1))
        sleep $VERIFY_INTERVAL
    done
    echo "$last_bytes"
    return 1
}

###################### 大气启动界面 ######################
show_banner() {
    clear
    echo -e "\033[38;5;39m"
    cat << 'EOF'
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║    ██████╗ ██████╗        ██████╗     ██╗   ██╗ █████╗                      ║
║   ██╔════╝██╔════╝       ██╔════╝     ██║   ██║██╔══██╗                     ║
║   ██║     ██║     █████╗ ██║  ███╗    ██║   ██║╚██████║                     ║
║   ██║     ██║     ╚════╝ ██║   ██║    ╚██╗ ██╔╝ ╚═══██║                     ║
║   ╚██████╗╚██████╗       ╚██████╔╝     ╚████╔╝  █████╔╝                     ║
║    ╚═════╝ ╚═════╝        ╚═════╝       ╚═══╝   ╚════╝                      ║
║                                                                              ║
║              🌍 COMMONCRAWL GLOBAL UPLOADER v9.0 🌍                         ║
║                                                                              ║
║                     🚀 网卡速度监控 • 智能节点切换                               ║
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

###################### 用户交互 ######################
show_banner

# 检查依赖
if ! command -v bc &> /dev/null; then
    echo "❌ 需要安装 bc 计算器: sudo apt install bc"
    exit 1
fi

# 获取网卡信息
MAIN_INTERFACE=$(get_main_interface)
if [[ -z "$MAIN_INTERFACE" ]]; then
    echo "❌ 无法检测主网卡接口"
    echo "   可用接口: $(ls /sys/class/net/ | grep -v lo | tr '\n' ' ')"
    exit 1
fi

# 显示网卡详细信息
INTERFACE_IP=$(ip addr show "$MAIN_INTERFACE" | grep 'inet ' | awk '{print $2}' | head -1)
echo -e "\033[38;5;226m🌐 网络接口\033[0m: $MAIN_INTERFACE ($INTERFACE_IP)"

# 测试网卡速度读取
TEST_SPEED=$(get_network_upload_speed "$MAIN_INTERFACE")
echo "   └─ 初始上传速度: ${TEST_SPEED}MB/s"

# 默认设置
DEFAULT_REPEAT=25
echo -e "\033[38;5;51m⏰ 循环间隔\033[0m"
read -rp "   循环间隔小时数 (默认${DEFAULT_REPEAT}小时，0=仅执行一次): " REPEAT_INTERVAL_HOURS
REPEAT_INTERVAL_HOURS=${REPEAT_INTERVAL_HOURS:-$DEFAULT_REPEAT}

# 节点选择
ALL_REMOTES=($(rclone listremotes | sed 's/:$//'))
if [[ ${#ALL_REMOTES[@]} -eq 0 ]]; then
    echo "❌ 未检测到rclone存储节点"
    exit 1
fi

echo -e "\n\033[38;5;51m🟢 存储节点\033[0m (共${#ALL_REMOTES[@]}个)"
for i in "${!ALL_REMOTES[@]}"; do
    echo "   ├─ [$((i+1))] ${ALL_REMOTES[i]}"
done

echo -e "\n\033[38;5;196m🎯 节点选择\033[0m"
read -rp "   选择节点 (默认全选并随机排序，数字用空格分隔): " NODE_SELECTION

if [[ -z "$NODE_SELECTION" ]]; then
    # 默认全选并随机排序
    SELECTED_REMOTES=($(printf '%s\n' "${ALL_REMOTES[@]}" | shuf))
    echo "   ✅ 已选择全部节点并随机排序"
else
    # 手动选择
    SELECTED_REMOTES=()
    for num in $NODE_SELECTION; do
        if [[ $num =~ ^[0-9]+$ ]] && (( num >= 1 && num <= ${#ALL_REMOTES[@]} )); then
            SELECTED_REMOTES+=("${ALL_REMOTES[$((num-1))]}")
        fi
    done
fi

if [[ ${#SELECTED_REMOTES[@]} -eq 0 ]]; then
    echo "❌ 未选择有效节点"
    exit 1
fi

echo "   已选择: ${SELECTED_REMOTES[*]}"

read -rp "📍 起始文件行号 (默认1): " START_LINE
START_LINE=${START_LINE:-1}

###################### 下载文件列表 ######################
WARC_FILE="$TMP_DIR/warc.paths"
echo -e "\n\033[38;5;226m📥 正在获取文件列表...\033[0m"
if curl -sL "$WARC_LIST_URL" | gunzip -c > "$WARC_FILE"; then
    TOTAL_LINES=$(wc -l < "$WARC_FILE")
    echo "✅ 成功获取 $TOTAL_LINES 个文件"
else
    echo "❌ 获取文件列表失败"
    exit 1
fi

###################### 主循环 ######################
echo -e "\n\033[38;5;51m🚀 开始上传任务...\033[0m"

while :; do
    echo -e "\n\033[48;5;21m========== 新一轮上传 $(date '+%F %T') ==========\033[0m"
    
    for REMOTE in "${SELECTED_REMOTES[@]}"; do
        echo -e "\n┌─ \033[38;5;82m🚀 节点: $REMOTE\033[0m"
        PROGRESS_FILE="$TMP_DIR/${REMOTE}.progress"
        LOGFILE="$LOG_DIR/${REMOTE}_$(date +%F_%H-%M-%S).log"
        FLAG_FILE="$TMP_DIR/${REMOTE}_slow.flag"

        [[ -f "$PROGRESS_FILE" ]] || echo "$START_LINE" > "$PROGRESS_FILE"
        CURRENT_LINE=$(<"$PROGRESS_FILE")

        # 重置网卡速度基准
        rm -f "$TMP_DIR/network_speed_${MAIN_INTERFACE}"
        get_network_upload_speed "$MAIN_INTERFACE" > /dev/null

        # 初始容量
        LAST_USED=$(rclone size "$REMOTE:$DEST_PATH" --json | jq -r '.bytes // 0')

        NO_PROGRESS=0
        while (( CURRENT_LINE <= TOTAL_LINES )); do
            BATCH_END=$(( CURRENT_LINE + THREADS - 1 ))
            (( BATCH_END > TOTAL_LINES )) && BATCH_END=$TOTAL_LINES
            echo -e "\n├─ 🚀 批次 $CURRENT_LINE-$BATCH_END 共 $((BATCH_END-CURRENT_LINE+1)) 文件"

            BATCH_LIST="$TMP_DIR/${REMOTE}_batch_${CURRENT_LINE}.txt"
            BATCH_URLS="$TMP_DIR/${REMOTE}_urls_${CURRENT_LINE}.txt"
            sed -n "${CURRENT_LINE},${BATCH_END}p" "$WARC_FILE" > "$BATCH_LIST"
            sed "s|^|https://data.commoncrawl.org/|" "$BATCH_LIST" > "$BATCH_URLS"

            echo 0 > "$FLAG_FILE"

            ##### 网卡速度监控子线程 #####
            monitor_network_speed() {
                local slow_count=0
                local check_count=0
                
                while [[ $(<"$FLAG_FILE") == 0 ]]; do
                    sleep 5
                    check_count=$((check_count+1))
                    
                    local speed=$(get_network_upload_speed "$MAIN_INTERFACE")
                    local active_threads=$(pgrep -cf "rclone.*$REMOTE:" || echo 0)
                    local total_uploaded=$(get_node_total_uploaded "$REMOTE")
                    
                    printf "\r├─ 📊 网卡速度: %d MB/s | 活跃线程: %d | 已上传: %dGB" "$speed" "$active_threads" "$total_uploaded"
                    
                    # 前60秒不检测低速
                    if (( check_count <= 12 )); then
                        continue
                    fi
                    
                    # 低速检测
                    if (( speed < LOW_SPEED_MB )); then
                        slow_count=$((slow_count+5))
                    else
                        slow_count=0
                    fi
                    
                    if (( slow_count >= LOW_SPEED_SECONDS )); then
                        echo -e "\n├─ 🐌 网卡低速触发: ${speed}MB/s < ${LOW_SPEED_MB}MB/s (持续${LOW_SPEED_SECONDS}秒)"
                        echo 1 > "$FLAG_FILE"
                        return
                    fi
                    
                    # 超时检测 (10分钟)
                    if (( check_count >= 120 )); then
                        echo -e "\n├─ ⏰ 超时切换"
                        echo 1 > "$FLAG_FILE"
                        return
                    fi
                done
            }
            monitor_network_speed & 
            MON_PID=$!

            ##### 启动上传线程 #####
            UPLOAD_PIDS=()
            idx=0
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
                echo "├─ 🔗 启动线程 $idx: ${filename:0:40}..."
                sleep 0.2  # 避免同时启动过多连接
            done < "$BATCH_URLS"
            
            echo "├─ ⚡ 共 ${#UPLOAD_PIDS[@]} 线程，网卡监控已启动"

            ##### 等待上传完成或低速触发 #####
            while :; do
                [[ $(<"$FLAG_FILE") == 1 ]] && {
                    echo -e "\n├─ 🛑 低速中止批次，正在终止进程..."
                    
                    # 立即终止所有上传进程
                    for p in "${UPLOAD_PIDS[@]}"; do
                        kill -KILL "$p" 2>/dev/null || true
                    done
                    
                    # 等待进程清理
                    sleep 3
                    echo "├─ ✅ 进程清理完成"
                    break
                }
                
                alive=0
                for p in "${UPLOAD_PIDS[@]}"; do
                    kill -0 "$p" 2>/dev/null && alive=$((alive+1))
                done
                
                (( alive == 0 )) && break
                sleep 3
            done

            cleanup "$MON_PID"

            ##### 批次统计 #####
            FINAL_SPEED=$(get_network_upload_speed "$MAIN_INTERFACE")
            NEW_USED=$(verify_batch "$LAST_USED")
            verify_ok=$?
            size_diff_gb=$(echo "scale=2; ($NEW_USED - $LAST_USED) / 1073741824" | bc -l)
            
            if (( verify_ok == 0 )); then
                echo -e "\n├─ ✅ 批次完成 | 新增 ${size_diff_gb}GB | 网卡速度 ${FINAL_SPEED}MB/s"
                LAST_USED=$NEW_USED
                NO_PROGRESS=0
            else
                echo -e "\n├─ ⚠️  无容量变化 (尝试 ${NO_PROGRESS}/3) | 网卡速度 ${FINAL_SPEED}MB/s"
                NO_PROGRESS=$((NO_PROGRESS+1))
            fi

            # 连续失败切节点
            (( NO_PROGRESS >= 3 )) && {
                echo "└─ 🚫 连续 3 次无进展，切换节点"
                break
            }

            CURRENT_LINE=$(( BATCH_END + 1 ))
            echo "$CURRENT_LINE" > "$PROGRESS_FILE"
            rm -f "$BATCH_LIST" "$BATCH_URLS"
        done
        
        rm -f "$FLAG_FILE" 
        echo -e "└─ ✅ 节点 \033[38;5;82m$REMOTE\033[0m 完成本轮"
    done

    (( REPEAT_INTERVAL_HOURS == 0 )) && {
        echo -e "\n\033[38;5;46m🎉 全部任务完成！\033[0m"
        exit 0
    }
    
    echo -e "\n\033[38;5;226m💤 休眠 ${REPEAT_INTERVAL_HOURS}h 后继续...\033[0m"
    sleep $(( REPEAT_INTERVAL_HOURS*3600 ))
done
