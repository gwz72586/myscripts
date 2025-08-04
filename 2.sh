#!/bin/bash
set -euo pipefail

WARC_LIST_URL="https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-30/warc.paths.gz"
DEST_PATH="/dx"
MAX_TRANSFER="700G"
TMP_DIR="/tmp/warc_upload2"
LOG_DIR="./logs"
mkdir -p "$TMP_DIR" "$LOG_DIR"

export LANG=zh_CN.UTF-8
export LC_ALL=zh_CN.UTF-8

read -rp "请输入每隔多少小时重复执行上传任务（输入 0 仅执行一次）: " REPEAT_INTERVAL_HOURS
ALL_REMOTES=$(rclone listremotes | sed 's/:$//')
echo -e "🟢 可用 remote：\n$ALL_REMOTES"
read -rp "请输入希望使用的 remote（如 d1 d2）: " -a SELECTED_REMOTES
read -rp "请输入从第几行开始抓取（默认 1）: " START_LINE
START_LINE=${START_LINE:-1}
echo

WARC_FILE="$TMP_DIR/warc.paths"
curl -sL "$WARC_LIST_URL" | gunzip -c > "$WARC_FILE"
TOTAL_LINES=$(wc -l < "$WARC_FILE")

# ========= 主循环 =========
while :; do
  echo "🕓 开始上传任务（$(date +"%F %T")）"
  for REMOTE in "${SELECTED_REMOTES[@]}"; do
    echo -e "\n🚀 Starting remote: $REMOTE"
    PROGRESS_FILE="$TMP_DIR/${REMOTE}.progress"
    USED_FILE="$TMP_DIR/${REMOTE}.used"
    LOGFILE="$LOG_DIR/${REMOTE}_$(date +%F_%H-%M-%S).log"
    SPEED_LOG="$TMP_DIR/${REMOTE}_speed.log"
    FLAG_FILE="$TMP_DIR/${REMOTE}_slow.flag"

    CURRENT_LINE=$(<"$PROGRESS_FILE" 2>/dev/null || echo "$START_LINE")
    rm -f "$FLAG_FILE"
    LAST_USED=$(rclone about "$REMOTE:" --json | jq -r '.used' 2>/dev/null)
    echo "$LAST_USED" > "$USED_FILE"
    NO_PROGRESS_COUNT=0

    while [ "$CURRENT_LINE" -le "$TOTAL_LINES" ]; do
      PATH_LINE=$(sed -n "${CURRENT_LINE}p" "$WARC_FILE")
      URL="https://data.commoncrawl.org/${PATH_LINE}"
      echo -e "\n[$REMOTE] 🔗 Uploading: $URL"

      : > "$SPEED_LOG"
      echo "0" > "$FLAG_FILE"

      monitor_speed() {
        local SLOW_TIME=0
        tail -Fn0 "$SPEED_LOG" | \
        grep --line-buffered -oP 'Speed:\s+\K[\d\.]+(?=\sMiB/s)' | \
        while read SPEED; do
          SPEED_INT=${SPEED%.*}
          if (( SPEED_INT < 10 )); then
            SLOW_TIME=$(( SLOW_TIME + 5 ))
          else
            SLOW_TIME=0
          fi
          if (( SLOW_TIME >= 60 )); then
            echo "1" > "$FLAG_FILE"
            echo "⚠️ Low speed <10MiB/s for 60s → skipping remote $REMOTE"
            kill "$UPLOAD_PID" 2>/dev/null
            break
          fi
        done
      }

      monitor_speed &
      MON_PID=$!

      rclone copyurl "$URL" "$REMOTE:$DEST_PATH" \
        --auto-filename \
        --drive-chunk-size 512M \
        --buffer-size 1G \
        --multi-thread-streams 12 \
        --transfers 12 \
        --tpslimit 0 \
        --disable-http2 \
        --max-transfer "$MAX_TRANSFER" \
        --stats-one-line -P >> "$SPEED_LOG" 2>>"$LOGFILE" &
      UPLOAD_PID=$!
      wait "$UPLOAD_PID" || true
      kill "$MON_PID" 2>/dev/null

      [ "$(cat "$FLAG_FILE")" == "1" ] && {
        echo "🔁 Skipping remote $REMOTE"
        break
      }

      sleep 10
      NEW_USED=$(rclone about "$REMOTE:" --json | jq -r '.used' 2>/dev/null)
      if (( NEW_USED <= LAST_USED )); then
        NO_PROGRESS_COUNT=$(( NO_PROGRESS_COUNT + 1 ))
        echo "❌ Upload no progress (count $NO_PROGRESS_COUNT)." | tee -a "$LOGFILE"
      else
        echo "✅ Upload success." | tee -a "$LOGFILE"
        NO_PROGRESS_COUNT=0
        echo "$NEW_USED" > "$USED_FILE"
      fi

      (( NO_PROGRESS_COUNT >= 3 )) && {
        echo "🚫 3 consecutive failures → skip remote $REMOTE." | tee -a "$LOGFILE"
        break
      }

      CURRENT_LINE=$((CURRENT_LINE + 1))
      echo "$CURRENT_LINE" > "$PROGRESS_FILE"
    done

    # end per-remote loop
  done

  ((REPEAT_INTERVAL_HOURS == 0)) && { echo "✅ All done, exiting."; exit 0; }

  echo "🕙 Completed all remotes. Sleeping for $REPEAT_INTERVAL_HOURS hours..."
  sleep $(( REPEAT_INTERVAL_HOURS * 3600 ))
done
