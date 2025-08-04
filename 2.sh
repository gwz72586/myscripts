#!/bin/bash
# ver 1.1  ── 自动上传脚本（修复低速检测和卡死问题）

set -euo pipefail

######################### 基本配置 #########################
WARC_LIST_URL="https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-30/warc.paths.gz"
DEST_PATH="/dx"                  # 远程保存路径
MAX_TRANSFER="700G"              # 每账号上传上限
THREADS=12                       # --transfers & 分块线程数
CHUNK_SIZE="512M"
BUFFER_SIZE="1G"
LOW_SPEED_MB=10                  # 低速阈值 10 MiB/s
LOW_SPEED_SECONDS=60             # 低速持续秒数
TMP_DIR="/tmp/warc_uploader"
LOG_DIR="./logs"
mkdir -p "$TMP_DIR" "$LOG_DIR"

######################### 环境与输入 #########################
export LANG=zh_CN.UTF-8
export LC_ALL=zh_CN.UTF-8

# 清理函数
cleanup() {
    local upload_pid=${1:-}
    local mon_pid=${2:-}
    
    # 终止监控进程
    if [[ -n "$mon_pid" ]] && kill -0 "$mon_pid" 2>/dev/null; then
        kill -TERM "$mon_pid" 2>/dev/null || true
        sleep 1
        kill -KILL "$mon_pid" 2>/dev/null || true
    fi
    
    # 终止上传进程及其子进程
    if [[ -n "$upload_pid" ]] && kill -0 "$upload_pid" 2>/dev/null; then
        # 先尝试优雅终止
        kill -TERM "$upload_pid" 2>/dev/null || true
        sleep 3
        
        # 强制终止
        if kill -0 "$upload_pid" 2>/dev/null; then
            kill -KILL "$upload_pid" 2>/dev/null || true
        fi
    fi
    
    # 清理可能的孤儿进程
    pkill -f "rclone copyurl.*$REMOTE:" 2>/dev/null || true
}

read -rp "每隔多少小时重复执行上传任务（0 表示仅执行一次）: " REPEAT_INTERVAL_HOURS
ALL_REMOTES=$(rclone listremotes | sed 's/:$//')
echo -e "🟢 可用 remote：\n$ALL_REMOTES"
read -rp "请输入要使用的 remote 名称（空格分隔）： " -a SELECTED_REMOTES
read -rp "从第几行开始抓取（默认 1）: " START_LINE
START_LINE=${START_LINE:-1}
echo

######################### 下载路径列表 #########################
WARC_FILE="$TMP_DIR/warc.paths"
curl -sL "$WARC_LIST_URL" | gunzip -c > "$WARC_FILE"
TOTAL_LINES=$(wc -l < "$WARC_FILE")

######################### 主循环 #########################
while :; do
  echo -e "\n========== 新一轮上传开始 $(date '+%F %T') =========="

  for REMOTE in "${SELECTED_REMOTES[@]}"; do
    echo -e "\n🚀 当前 remote: $REMOTE"
    PROGRESS_FILE="$TMP_DIR/${REMOTE}.progress"
    USED_FILE="$TMP_DIR/${REMOTE}.used"
    LOGFILE="$LOG_DIR/${REMOTE}_$(date +%F_%H-%M-%S).log"
    SPEED_LOG="$TMP_DIR/${REMOTE}_speed.log"
    FLAG_FILE="$TMP_DIR/${REMOTE}_slow.flag"

    # 确保进度文件存在
    if [[ ! -f "$PROGRESS_FILE" ]]; then
        echo "$START_LINE" > "$PROGRESS_FILE"
    fi
    CURRENT_LINE=$(<"$PROGRESS_FILE")
    LAST_USED=$(rclone about "$REMOTE:" --json | jq -r '.used' 2>/dev/null || echo 0)
    echo "$LAST_USED" > "$USED_FILE"
    NO_PROGRESS=0

    while [ "$CURRENT_LINE" -le "$TOTAL_LINES" ]; do
      PATH_LINE=$(sed -n "${CURRENT_LINE}p" "$WARC_FILE")
      URL="https://data.commoncrawl.org/${PATH_LINE}"
      echo -e "\n[$REMOTE] 🔗 $URL"

      # 重置标记和日志
      echo 0 > "$FLAG_FILE"
      : > "$SPEED_LOG"

      # 针对rclone实际输出格式的速度监控函数
      monitor_speed() {
        local slow_count=0
        local check_interval=5
        local no_data_count=0
        local startup_grace=15  # 启动宽限期15秒
        
        echo "🔍 开始速度监控 (阈值: ${LOW_SPEED_MB} MiB/s, 超时: ${LOW_SPEED_SECONDS}s)"
        
        while [[ -f "$FLAG_FILE" && $(<"$FLAG_FILE") == 0 ]]; do
          sleep "$check_interval"
          no_data_count=$((no_data_count + check_interval))
          
          # 检查速度日志是否有最新内容
          if [[ -f "$SPEED_LOG" && -s "$SPEED_LOG" ]]; then
            # 获取最后一行的完整统计信息
            local latest_line=$(tail -n 1 "$SPEED_LOG" 2>/dev/null | grep -v "^$" || echo "")
            echo "📊 最新状态: $latest_line"
            
            if [[ -n "$latest_line" ]]; then
              local speed_mib=0
              
              # 匹配 rclone 格式: "数字 B/s" 或 "数字 MiB/s" 等
              # 支持: 123 B/s, 1.23 KiB/s, 12.34 MiB/s, 123.45 GiB/s
              local speed_str=$(echo "$latest_line" | grep -oP '[\d\.]+\s*[KMGT]?i?B/s' | tail -n 1 || echo "")
              
              if [[ -n "$speed_str" ]]; then
                local speed_num=$(echo "$speed_str" | grep -oP '^[\d\.]+' || echo "0")
                local speed_unit=$(echo "$speed_str" | grep -oP '[KMGT]?i?B/s

      # 启动监控
      monitor_speed & 
      MON_PID=$!

      # 启动上传，增加更详细的输出
      echo "🚀 启动上传进程..."
      rclone copyurl "$URL" "$REMOTE:$DEST_PATH" \
        --auto-filename \
        --drive-chunk-size "$CHUNK_SIZE" \
        --buffer-size "$BUFFER_SIZE" \
        --multi-thread-streams "$THREADS" \
        --transfers "$THREADS" \
        --tpslimit 0 \
        --disable-http2 \
        --max-transfer "$MAX_TRANSFER" \
        --stats-one-line \
        --stats 3s \
        --verbose \
        -P >> "$SPEED_LOG" 2>> "$LOGFILE" &
      UPLOAD_PID=$!

      # 等待上传完成或低速触发
      while kill -0 "$UPLOAD_PID" 2>/dev/null; do
        if [[ $(<"$FLAG_FILE" 2>/dev/null || echo 0) == 1 ]]; then
          echo "🔄 低速标记触发，终止当前上传"
          break
        fi
        sleep 2
      done

      # 清理进程
      cleanup "$UPLOAD_PID" "$MON_PID"
      
      # 等待进程完全终止
      sleep 3

      # 检查是否因低速切换
      if [[ $(<"$FLAG_FILE" 2>/dev/null || echo 0) == 1 ]]; then
        echo "🔁 因低速切换到下一个 remote"
        break
      fi

      # 检查上传是否成功
      if ! wait "$UPLOAD_PID" 2>/dev/null; then
        echo "❌ 上传进程异常退出" | tee -a "$LOGFILE"
      fi

      # 验证上传效果
      sleep 10
      NEW_USED=$(rclone about "$REMOTE:" --json | jq -r '.used' 2>/dev/null || echo 0)
      if (( NEW_USED <= LAST_USED )); then
        NO_PROGRESS=$((NO_PROGRESS+1))
        echo "❌ 无空间增量 ($NO_PROGRESS/3)" | tee -a "$LOGFILE"
      else
        echo "✅ 上传有效: $(( (NEW_USED - LAST_USED) / 1024 / 1024 )) MB" | tee -a "$LOGFILE"
        NO_PROGRESS=0
        echo "$NEW_USED" > "$USED_FILE"
        LAST_USED=$NEW_USED
      fi
      
      # 连续失败则切换
      if (( NO_PROGRESS >= 3 )); then
        echo "🚫 连续 3 次无增量，切换 remote"
        break
      fi

      CURRENT_LINE=$((CURRENT_LINE + 1))
      echo "$CURRENT_LINE" > "$PROGRESS_FILE"
    done
    
    # 清理临时文件
    rm -f "$FLAG_FILE" "$SPEED_LOG"
  done

  if (( REPEAT_INTERVAL_HOURS == 0 )); then
    echo -e "\n✅ 单次任务完成，退出"
    exit 0
  fi
  
  echo -e "\n🕙 本轮结束，休眠 ${REPEAT_INTERVAL_HOURS}h..."
  sleep $(( REPEAT_INTERVAL_HOURS * 3600 ))
done || echo "B/s")
                
                # 转换到 MiB/s
                case "$speed_unit" in
                  "B/s")
                    speed_mib=$(echo "scale=2; $speed_num / 1048576" | bc -l 2>/dev/null || echo "0")
                    ;;
                  "KiB/s")
                    speed_mib=$(echo "scale=2; $speed_num / 1024" | bc -l 2>/dev/null || echo "0")
                    ;;
                  "MiB/s")
                    speed_mib=$speed_num
                    ;;
                  "GiB/s")
                    speed_mib=$(echo "scale=2; $speed_num * 1024" | bc -l 2>/dev/null || echo "0")
                    ;;
                  "KB/s")
                    speed_mib=$(echo "scale=2; $speed_num * 0.000953674" | bc -l 2>/dev/null || echo "0")
                    ;;
                  "MB/s")
                    speed_mib=$(echo "scale=2; $speed_num * 0.953674" | bc -l 2>/dev/null || echo "0")
                    ;;
                  *)
                    speed_mib=0
                    ;;
                esac
                
                local speed_int=$(echo "$speed_mib" | cut -d. -f1)
                speed_int=${speed_int:-0}
                
                echo "📈 当前速度: ${speed_mib} MiB/s (原始: $speed_str)"
                
                # 启动宽限期内不检测低速
                if (( no_data_count <= startup_grace )); then
                  echo "🚀 启动宽限期: ${no_data_count}/${startup_grace}s"
                  continue
                fi
                
                if (( speed_int < LOW_SPEED_MB )); then
                  slow_count=$((slow_count + check_interval))
                  echo "⚠️ 低速警告: ${speed_mib} MiB/s < ${LOW_SPEED_MB} MiB/s (累计: ${slow_count}/${LOW_SPEED_SECONDS}s)"
                else
                  if (( slow_count > 0 )); then
                    echo "✅ 速度恢复: ${speed_mib} MiB/s >= ${LOW_SPEED_MB} MiB/s"
                  fi
                  slow_count=0
                fi
                
                if (( slow_count >= LOW_SPEED_SECONDS )); then
                  echo "🐌 触发低速切换: 连续 ${LOW_SPEED_SECONDS}s < ${LOW_SPEED_MB} MiB/s"
                  echo 1 > "$FLAG_FILE"
                  return
                fi
              else
                echo "⏳ 未找到速度信息... (${no_data_count}s)"
              fi
            else
              echo "📝 等待日志输出... (${no_data_count}s)"
            fi
          else
            echo "📂 等待日志文件... (${no_data_count}s)"
          fi
          
          # 如果超过2分钟没有有效速度数据，可能是连接问题
          if (( no_data_count >= 120 )); then
            echo "❌ 长时间无有效数据，可能连接异常，切换remote"
            echo 1 > "$FLAG_FILE"
            return
          fi
        done
      }

      # 启动监控
      monitor_speed & 
      MON_PID=$!

      # 启动上传，增加更详细的输出
      echo "🚀 启动上传进程..."
      rclone copyurl "$URL" "$REMOTE:$DEST_PATH" \
        --auto-filename \
        --drive-chunk-size "$CHUNK_SIZE" \
        --buffer-size "$BUFFER_SIZE" \
        --multi-thread-streams "$THREADS" \
        --transfers "$THREADS" \
        --tpslimit 0 \
        --disable-http2 \
        --max-transfer "$MAX_TRANSFER" \
        --stats-one-line \
        --stats 3s \
        --verbose \
        -P >> "$SPEED_LOG" 2>> "$LOGFILE" &
      UPLOAD_PID=$!

      # 等待上传完成或低速触发
      while kill -0 "$UPLOAD_PID" 2>/dev/null; do
        if [[ $(<"$FLAG_FILE" 2>/dev/null || echo 0) == 1 ]]; then
          echo "🔄 低速标记触发，终止当前上传"
          break
        fi
        sleep 2
      done

      # 清理进程
      cleanup "$UPLOAD_PID" "$MON_PID"
      
      # 等待进程完全终止
      sleep 3

      # 检查是否因低速切换
      if [[ $(<"$FLAG_FILE" 2>/dev/null || echo 0) == 1 ]]; then
        echo "🔁 因低速切换到下一个 remote"
        break
      fi

      # 检查上传是否成功
      if ! wait "$UPLOAD_PID" 2>/dev/null; then
        echo "❌ 上传进程异常退出" | tee -a "$LOGFILE"
      fi

      # 验证上传效果
      sleep 10
      NEW_USED=$(rclone about "$REMOTE:" --json | jq -r '.used' 2>/dev/null || echo 0)
      if (( NEW_USED <= LAST_USED )); then
        NO_PROGRESS=$((NO_PROGRESS+1))
        echo "❌ 无空间增量 ($NO_PROGRESS/3)" | tee -a "$LOGFILE"
      else
        echo "✅ 上传有效: $(( (NEW_USED - LAST_USED) / 1024 / 1024 )) MB" | tee -a "$LOGFILE"
        NO_PROGRESS=0
        echo "$NEW_USED" > "$USED_FILE"
        LAST_USED=$NEW_USED
      fi
      
      # 连续失败则切换
      if (( NO_PROGRESS >= 3 )); then
        echo "🚫 连续 3 次无增量，切换 remote"
        break
      fi

      CURRENT_LINE=$((CURRENT_LINE + 1))
      echo "$CURRENT_LINE" > "$PROGRESS_FILE"
    done
    
    # 清理临时文件
    rm -f "$FLAG_FILE" "$SPEED_LOG"
  done

  if (( REPEAT_INTERVAL_HOURS == 0 )); then
    echo -e "\n✅ 单次任务完成，退出"
    exit 0
  fi
  
  echo -e "\n🕙 本轮结束，休眠 ${REPEAT_INTERVAL_HOURS}h..."
  sleep $(( REPEAT_INTERVAL_HOURS * 3600 ))
done
