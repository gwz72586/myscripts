#!/bin/bash
# CC-G 3.0 - CommonCrawl Global Uploader

set -euo pipefail

# ASCII Art Logo
echo "
 ██████╗ ██████╗      ██████╗     ██████╗ 
██╔════╝██╔════╝     ██╔════╝     ╚════██╗
██║     ██║     █████╗██║  ███╗     █████╔╝
██║     ██║     ╚════╝██║   ██║     ╚═══██╗
╚██████╗╚██████╗     ╚██████╔╝    ██████╔╝
 ╚═════╝ ╚═════╝      ╚═════╝     ╚═════╝ 
                                           
🌐 CommonCrawl Global Uploader v3.0
📊 优化版本 - 2.5Gbps 带宽配置
"

######################### 配置参数 #########################
WARC_LIST_URL="https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-30/warc.paths.gz"
DEST_PATH="/dx"
MAX_TRANSFER="700G"

# 2.5Gbps带宽优化参数
THREADS=16
CHUNK_SIZE="256M"
BUFFER_SIZE="2G"
MULTI_THREAD_STREAMS=8
CHECKERS=32
LOW_SPEED_MB=50
LOW_SPEED_SECONDS=45

TMP_DIR="/tmp/warc_uploader"
LOG_DIR="./logs"
mkdir -p "$TMP_DIR" "$LOG_DIR"

export LANG=zh_CN.UTF-8
export LC_ALL=zh_CN.UTF-8

echo "🔧 配置参数: 带宽2.5G | 线程${THREADS} | 块大小${CHUNK_SIZE} | 缓冲${BUFFER_SIZE}"

######################### 清理函数 #########################
cleanup() {
    local upload_pid=${1:-}
    local mon_pid=${2:-}
    
    if [[ -n "$mon_pid" ]] && kill -0 "$mon_pid" 2>/dev/null; then
        kill -TERM "$mon_pid" 2>/dev/null || true
        sleep 1
        kill -KILL "$mon_pid" 2>/dev/null || true
    fi
    
    if [[ -n "$upload_pid" ]] && kill -0 "$upload_pid" 2>/dev/null; then
        kill -TERM "$upload_pid" 2>/dev/null || true
        sleep 2
        if kill -0 "$upload_pid" 2>/dev/null; then
            kill -KILL "$upload_pid" 2>/dev/null || true
        fi
    fi
    
    pkill -f "rclone copyurl.*$REMOTE:" 2>/dev/null || true
}

######################### 用户输入 #########################
echo "📡 正在连接 CommonCrawl 服务器..."

read -rp "⏰ 循环间隔小时数 (0=仅执行一次): " REPEAT_INTERVAL_HOURS
ALL_REMOTES=$(rclone listremotes | sed 's/:$//')
echo -e "\n🟢 可用存储节点:\n$(echo "$ALL_REMOTES" | sed 's/^/   ├─ /')"
read -rp "🎯 选择要使用的节点 (空格分隔): " -a SELECTED_REMOTES
read -rp "📍 起始文件行号 (默认1): " START_LINE
START_LINE=${START_LINE:-1}

######################### 获取文件列表 #########################
WARC_FILE="$TMP_DIR/warc.paths"
printf "📥 获取文件列表... "
if curl -sL "$WARC_LIST_URL" | gunzip -c > "$WARC_FILE" 2>/dev/null; then
    TOTAL_LINES=$(wc -l < "$WARC_FILE")
    echo "完成 (共 $TOTAL_LINES 个文件)"
else
    echo "❌ 失败"
    exit 1
fi

######################### 主循环 #########################
while :; do
  echo -e "\n========== 开始新一轮上传 $(date '+%F %T') =========="

  for REMOTE in "${SELECTED_REMOTES[@]}"; do
    echo -e "\n┌─ 🚀 存储节点: $REMOTE"
    PROGRESS_FILE="$TMP_DIR/${REMOTE}.progress"
    USED_FILE="$TMP_DIR/${REMOTE}.used"
    LOGFILE="$LOG_DIR/${REMOTE}_$(date +%F_%H-%M-%S).log"
    SPEED_LOG="$TMP_DIR/${REMOTE}_speed.log"
    FLAG_FILE="$TMP_DIR/${REMOTE}_slow.flag"

    # 初始化进度文件
    if [[ ! -f "$PROGRESS_FILE" ]]; then
        echo "$START_LINE" > "$PROGRESS_FILE"
        echo "├─ 📝 新建进度记录，从第 $START_LINE 行开始"
    fi
    CURRENT_LINE=$(cat "$PROGRESS_FILE")
    
    # 检查存储空间
    printf "├─ 📊 检查存储空间... "
    LAST_USED=$(rclone about "$REMOTE:" --json 2>/dev/null | jq -r '.used' 2>/dev/null || echo 0)
    echo "$LAST_USED" > "$USED_FILE"
    echo "完成"
    NO_PROGRESS=0

    while [ "$CURRENT_LINE" -le "$TOTAL_LINES" ]; do
      # 批量获取多个文件进行并发上传
      BATCH_SIZE=$THREADS
      BATCH_END=$((CURRENT_LINE + BATCH_SIZE - 1))
      if (( BATCH_END > TOTAL_LINES )); then
        BATCH_END=$TOTAL_LINES
      fi
      
      echo -e "\n├─ 🚀 批量上传 $CURRENT_LINE-$BATCH_END (共 $((BATCH_END - CURRENT_LINE + 1)) 个文件)"
      
      # 重置标记和日志
      echo 0 > "$FLAG_FILE"
      : > "$SPEED_LOG"
      
      # 创建临时文件列表
      BATCH_LIST="$TMP_DIR/${REMOTE}_batch_${CURRENT_LINE}.txt"
      BATCH_URLS="$TMP_DIR/${REMOTE}_urls_${CURRENT_LINE}.txt"
      
      # 生成批次文件列表和URL列表
      sed -n "${CURRENT_LINE},${BATCH_END}p" "$WARC_FILE" > "$BATCH_LIST"
      sed "s|^|https://data.commoncrawl.org/|" "$BATCH_LIST" > "$BATCH_URLS"
      
      echo "├─ 📋 准备上传 $(wc -l < "$BATCH_LIST") 个文件"

      # 多线程速度监控函数
      monitor_speed() {
        local slow_count=0
        local startup_grace=30
        local check_count=0
        local total_speed=0
        
        while [[ -f "$FLAG_FILE" && $(cat "$FLAG_FILE" 2>/dev/null || echo 0) == 0 ]]; do
          sleep 5
          check_count=$((check_count + 1))
          total_speed=0
          
          # 监控所有上传进程的速度
          if [[ -f "$SPEED_LOG" && -s "$SPEED_LOG" ]]; then
            recent_lines=$(tail -n 20 "$SPEED_LOG" 2>/dev/null | grep -v "^$" || echo "")
            if [[ -n "$recent_lines" ]]; then
              while IFS= read -r line; do
                if echo "$line" | grep -q "B/s"; then
                  speed_str=$(echo "$line" | grep -o '[0-9.][0-9.]*[[:space:]]*[KMGT]*i*B/s')
                  if [[ -n "$speed_str" ]]; then
                    speed_num=$(echo "$speed_str" | grep -o '^[0-9.][0-9.]*')
                    speed_unit=$(echo "$speed_str" | grep -o '[KMGT]*i*B/s')
                    case "$speed_unit" in
                      "B/s") speed_mib=$(echo "scale=1; $speed_num / 1048576" | bc -l 2>/dev/null || echo "0") ;;
                      "KiB/s") speed_mib=$(echo "scale=1; $speed_num / 1024" | bc -l 2>/dev/null || echo "0") ;;
                      "MiB/s") speed_mib=$speed_num ;;
                      "GiB/s") speed_mib=$(echo "scale=1; $speed_num * 1024" | bc -l 2>/dev/null || echo "0") ;;
                      "KB/s") speed_mib=$(echo "scale=1; $speed_num * 0.000953674" | bc -l 2>/dev/null || echo "0") ;;
                      "MB/s") speed_mib=$(echo "scale=1; $speed_num * 0.953674" | bc -l 2>/dev/null || echo "0") ;;
                      *) speed_mib=0 ;;
                    esac
                    total_speed=$(echo "scale=1; $total_speed + $speed_mib" | bc -l 2>/dev/null || echo "$total_speed")
                  fi
                fi
              done <<< "$recent_lines"
              # 显示总速度
              if (( check_count % 2 == 0 )); then
                printf "\r├─ 📈 总上传速度: ${total_speed} MiB/s (${THREADS}线程)                    "
              fi
              # 启动宽限期
              if (( check_count * 5 <= startup_grace )); then
                continue
              fi
              # 检测低速
              total_speed_int=${total_speed%.*}
              total_speed_int=${total_speed_int:-0}
              if (( total_speed_int < LOW_SPEED_MB )); then
                slow_count=$((slow_count + 5))
                if (( slow_count >= LOW_SPEED_SECONDS )); then
                  echo -e "\n├─ 🐌 低速切换: ${total_speed} < ${LOW_SPEED_MB} MiB/s"
                  echo 1 > "$FLAG_FILE"
                  return
                fi
              else
                slow_count=0
              fi
            fi
          fi
          # 超时保护
          if (( check_count * 5 >= 600 )); then
            echo -e "\n├─ ⏰ 超时切换"
            echo 1 > "$FLAG_FILE"
            return
          fi
        done
      }

      # 启动监控
      monitor_speed & 
      MON_PID=$!

      # 批量启动多个上传进程
      UPLOAD_PIDS=()
      line_num=$CURRENT_LINE
      
      while IFS= read -r url; do
        filename=$(basename "${url#https://data.commoncrawl.org/}")
        rclone copyurl "$url" "$REMOTE:$DEST_PATH" \
          --auto-filename \
          --drive-chunk-size "$CHUNK_SIZE" \
          --buffer-size "$BUFFER_SIZE" \
          --multi-thread-streams "$MULTI_THREAD_STREAMS" \
          --checkers 4 \
          --tpslimit 0 \
          --disable-http2 \
          --max-transfer "$MAX_TRANSFER" \
          --timeout 30m \
          --retries 2 \
          --low-level-retries 5 \
          --stats 5s \
          --stats-one-line \
          --quiet \
          >> "$SPEED_LOG" 2>> "$LOGFILE" &
        UPLOAD_PIDS+=($!)
        echo "├─ 🔗 启动线程 $((line_num - CURRENT_LINE + 1)): ${filename:0:40}..."
        line_num=$((line_num + 1))
        # 控制并发数，避免系统过载
        if (( ${#UPLOAD_PIDS[@]} >= THREADS )); then
          break
        fi
      done < "$BATCH_URLS"

      echo "├─ ⚡ 已启动 ${#UPLOAD_PIDS[@]} 个并发上传进程"

      # 等待所有上传完成或低速触发
      while true; do
        flag_value=$(cat "$FLAG_FILE" 2>/dev/null || echo 0)
        if [[ "$flag_value" == "1" ]]; then
          echo -e "\n├─ 🛑 检测到低速，终止所有上传进程"
          for pid in "${UPLOAD_PIDS[@]}"; do
            kill -TERM "$pid" 2>/dev/null || true
          done
          sleep 2
          for pid in "${UPLOAD_PIDS[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
              kill -KILL "$pid" 2>/dev/null || true
            fi
          done
          break
        fi
        # 检查是否还有进程在运行
        running_count=0
        for pid in "${UPLOAD_PIDS[@]}"; do
          if kill -0 "$pid" 2>/dev/null; then
            running_count=$((running_count + 1))
          fi
        done
        if (( running_count == 0 )); then
          echo -e "\n├─ ✅ 所有上传进程已完成"
          break
        fi
        sleep 3
      done

      # 清理进程
      printf "\r├─ 🧹 清理进程... "
      cleanup "" "$MON_PID"
      for pid in "${UPLOAD_PIDS[@]}"; do
        kill -KILL "$pid" 2>/dev/null || true
      done
      pkill -f "rclone copyurl.*${REMOTE}:" 2>/dev/null || true
      echo "完成"

      # 清理临时文件
      rm -f "$BATCH_LIST" "$BATCH_URLS"

      # 检查是否因低速切换
      final_flag=$(cat "$FLAG_FILE" 2>/dev/null || echo 0)
      if [[ "$final_flag" == "1" ]]; then
        echo "└─ 🔁 切换到下一个存储节点"
        break
      fi

      # 验证上传结果
      printf "├─ 📊 验证批次上传结果... "
      sleep 5
      NEW_USED=$(rclone about "$REMOTE:" --json 2>/dev/null | jq -r '.used' 2>/dev/null || echo 0)
      if (( NEW_USED <= LAST_USED )); then
        NO_PROGRESS=$((NO_PROGRESS+1))
        echo "失败 ($NO_PROGRESS/3)"
      else
        size_diff=$(( (NEW_USED - LAST_USED) / 1024 / 1024 ))
        echo "成功 (+${size_diff}MB)"
        NO_PROGRESS=0
        echo "$NEW_USED" > "$USED_FILE"
        LAST_USED=$NEW_USED
      fi
      
      # 连续失败则切换
      if (( NO_PROGRESS >= 3 )); then
        echo "└─ 🚫 连续失败，切换存储节点"
        break
      fi

      # 更新进度到批次结束
      CURRENT_LINE=$((BATCH_END + 1))
      echo "$CURRENT_LINE" > "$PROGRESS_FILE"
    done
    
    # 清理临时文件
    rm -f "$FLAG_FILE" "$SPEED_LOG"
    echo "└─ ✅ 节点 $REMOTE 处理完成"
  done

  if (( REPEAT_INTERVAL_HOURS == 0 )); then
    echo -e "\n🎉 任务完成，程序退出"
    exit 0
  fi
  
  echo -e "\n💤 等待 ${REPEAT_INTERVAL_HOURS} 小时后继续..."
  sleep $(( REPEAT_INTERVAL_HOURS * 3600 ))
done
