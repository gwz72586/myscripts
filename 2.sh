#!/bin/bash
# ver 1.0  ── 自动上传脚本（支持低速切换、多 remote、断点续传、循环执行）

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

    CURRENT_LINE=$(<"$PROGRESS_FILE" 2>/dev/null || echo "$START_LINE")
    LAST_USED=$(rclone about "$REMOTE:" --json | jq -r '.used' 2>/dev/null || echo 0)
    echo "$LAST_USED" > "$USED_FILE"
    NO_PROGRESS=0

    while [ "$CURRENT_LINE" -le "$TOTAL_LINES" ]; do
      PATH_LINE=$(sed -n "${CURRENT_LINE}p" "$WARC_FILE")
      URL="https://data.commoncrawl.org/${PATH_LINE}"
      echo -e "\n[$REMOTE] 🔗 $URL"

      : >"$SPEED_LOG"; echo 0 >"$FLAG_FILE"

      monitor_speed() {
        local slow=0
        tail -Fn0 "$SPEED_LOG" | \
        grep --line-buffered -oP 'Speed:\s+\K[\d\.]+(?=\sMiB/s)' | \
        while read -r sp; do
          sp_int=${sp%.*}
          (( sp_int < LOW_SPEED_MB )) && slow=$((slow+5)) || slow=0
          if (( slow >= LOW_SPEED_SECONDS )); then
            echo 1 >"$FLAG_FILE"
            echo "⚠️ 低速 $LOW_SPEED_MB MiB/s * ${LOW_SPEED_SECONDS}s → 跳过 $REMOTE"
            pkill -TERM -P "$UPLOAD_PID" 2>/dev/null
            pkill -KILL -P "$UPLOAD_PID" 2>/dev/null
            pkill -TERM  "$UPLOAD_PID"   2>/dev/null
            pkill -KILL  "$UPLOAD_PID"   2>/dev/null
            break
          fi
        done
      }
      monitor_speed & MON_PID=$!

      rclone copyurl "$URL" "$REMOTE:$DEST_PATH" \
        --auto-filename \
        --drive-chunk-size "$CHUNK_SIZE" \
        --buffer-size "$BUFFER_SIZE" \
        --multi-thread-streams "$THREADS" \
        --transfers "$THREADS" \
        --tpslimit 0 \
        --disable-http2 \
        --max-transfer "$MAX_TRANSFER" \
        --stats-one-line -P >>"$SPEED_LOG" 2>>"$LOGFILE" &
      UPLOAD_PID=$!

      # 非阻塞等待
      while kill -0 "$UPLOAD_PID" 2>/dev/null; do
        [[ $(<"$FLAG_FILE") == 1 ]] && break
        sleep 2
      done
      kill "$MON_PID" 2>/dev/null; pkill -P "$MON_PID" tail 2>/dev/null

      # 若低速标记触发
      if [[ $(<"$FLAG_FILE") == 1 ]]; then
        echo "🔁 已切换 remote（低速触发）"
        break
      fi

      # 自检上传有效
      sleep 10
      NEW_USED=$(rclone about "$REMOTE:" --json | jq -r '.used' 2>/dev/null || echo 0)
      if (( NEW_USED <= LAST_USED )); then
        NO_PROGRESS=$((NO_PROGRESS+1))
        echo "❌ 无空间增量 ($NO_PROGRESS/3)" | tee -a "$LOGFILE"
      else
        echo "✅ 上传有效" | tee -a "$LOGFILE"
        NO_PROGRESS=0
        echo "$NEW_USED" >"$USED_FILE"
        LAST_USED=$NEW_USED
      fi
      (( NO_PROGRESS >= 3 )) && { echo "🚫 连续 3 次无增量 → 切 remote"; break; }

      CURRENT_LINE=$((CURRENT_LINE + 1))
      echo "$CURRENT_LINE" >"$PROGRESS_FILE"
    done
    rm -f "$FLAG_FILE"
  done

  (( REPEAT_INTERVAL_HOURS == 0 )) && { echo -e "\n✅ 单次任务完成，退出"; exit 0; }
  echo -e "\n🕙 本轮结束，休眠 ${REPEAT_INTERVAL_HOURS}h..."
  sleep $(( REPEAT_INTERVAL_HOURS * 3600 ))
done
