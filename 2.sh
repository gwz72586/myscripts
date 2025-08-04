#!/bin/bash

# ========== 配置 ==========
WARC_LIST_URL="https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-30/warc.paths.gz"
DEST_PATH="/dx"
MAX_TRANSFER="700G"
TMP_DIR="/tmp/warc_upload"
LOG_DIR="./logs"
mkdir -p "$TMP_DIR" "$LOG_DIR"

export LANG=zh_CN.UTF-8
export LC_ALL=zh_CN.UTF-8

# ========== 用户输入 ==========
read -p "请输入每隔多少小时重复执行上传任务（输入 0 仅执行一次）: " REPEAT_INTERVAL_HOURS
ALL_REMOTES=$(rclone listremotes | sed 's/:$//')
echo "🟢 可用 remote："
echo "$ALL_REMOTES"
read -p "请输入你希望使用的 remote 名称（以空格分隔，例如 d1 d2）: " -a SELECTED_REMOTES
read -p "请输入从第几行开始抓取（默认 1）: " START_LINE
START_LINE=${START_LINE:-1}
echo

# 下载并解压链接列表
WARC_LIST_FILE="$TMP_DIR/warc.paths"
curl -sL "$WARC_LIST_URL" | gunzip -c > "$WARC_LIST_FILE"
TOTAL_LINES=$(wc -l < "$WARC_LIST_FILE")

# ========= 主循环 =========
while true; do
  echo "🕓 开始上传任务（当前时间：$(date)）"

  for REMOTE in "${SELECTED_REMOTES[@]}"; do
    echo "🚀 开始 remote: $REMOTE"
    PROGRESS_FILE="$TMP_DIR/${REMOTE}.progress"
    USED_FILE="$TMP_DIR/${REMOTE}.used"
    LOGFILE="$LOG_DIR/${REMOTE}_$(date +%F_%H-%M-%S).log"
    SPEED_LOG="$TMP_DIR/${REMOTE}_speed.log"

    CURRENT_LINE=$(cat "$PROGRESS_FILE" 2>/dev/null || echo "$START_LINE")
    LAST_USED=$(rclone about "$REMOTE:" --json | jq -r '.used' 2>/dev/null)
    echo "$LAST_USED" > "$USED_FILE"
    NO_PROGRESS_COUNT=0

    while [ "$CURRENT_LINE" -le "$TOTAL_LINES" ]; do
      PATH_LINE=$(sed -n "${CURRENT_LINE}p" "$WARC_LIST_FILE")
      URL="https://data.commoncrawl.org/${PATH_LINE}"
      echo -e "\n[$REMOTE] 🔗 上传：$URL"

      : > "$SPEED_LOG"
      SLOW_TIME=0

      # 启动速率监控
      (
        while true; do
          SPEED=$(tail -n20 "$SPEED_LOG" | grep -oP 'Speed:\s+\K[\d\.]+(?=\sMiB/s)' | tail -n1)
          SPEED=${SPEED:-0}; SPEED_INT=${SPEED%.*}
          if [ "$SPEED_INT" -lt 10 ]; then SLOW_TIME=$((SLOW_TIME + 5)); else SLOW_TIME=0; fi
          if [ "$SLOW_TIME" -ge 60 ]; then
            echo "⚠️ 速率低于10MiB/s达60秒，跳过 $REMOTE" | tee -a "$LOGFILE"
            kill "$UPLOAD_PID" 2>/dev/null
            exit
          fi
          sleep 5
        done
      ) &
      MON_PID=$!

      # 上传执行
      rclone copyurl "$URL" "$REMOTE:$DEST_PATH" \
        --auto-filename \
        --drive-chunk-size 512M \
        --buffer-size 1G \
        --multi-thread-streams 12 \
        --transfers 12 \
        --tpslimit 0 \
        --disable-http2 \
        --max-transfer "$MAX_TRANSFER" \
        --stats-one-line -P >> "$SPEED_LOG" 2>&1 &
      UPLOAD_PID=$!
      wait $UPLOAD_PID
      kill $MON_PID 2>/dev/null

      # 自检上传有效性（通过 used 字节判断）
      sleep 10
      NEW_USED=$(rclone about "$REMOTE:" --json | jq -r '.used' 2>/dev/null)
      if [ "$NEW_USED" -le "$LAST_USED" ]; then
        NO_PROGRESS_COUNT=$((NO_PROGRESS_COUNT + 1))
        echo "❌ 上传无效 第 $NO_PROGRESS_COUNT 次" | tee -a "$LOGFILE"
      else
        echo "✅ 上传成功" | tee -a "$LOGFILE"
        NO_PROGRESS_COUNT=0
        echo "$NEW_USED" > "$USED_FILE"
      fi

      if [ "$NO_PROGRESS_COUNT" -ge 3 ]; then
        echo "🚫 连续 3 次上传无效，切换 remote..." | tee -a "$LOGFILE"
        break
      fi

      CURRENT_LINE=$((CURRENT_LINE + 1))
      echo "$CURRENT_LINE" > "$PROGRESS_FILE"
    done
  done

  if [ "$REPEAT_INTERVAL_HOURS" -eq 0 ]; then
    echo "✅ 本轮任务完成，脚本退出"
    break
  fi

  echo "🕙 所有 remote 执行完毕，等待 $REPEAT_INTERVAL_HOURS 小时后重新开始..."
  sleep $((REPEAT_INTERVAL_HOURS * 3600))
done
