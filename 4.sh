#!/bin/bash
# CC-G 3.4 - CommonCrawl Global Uploader (2.5 Gbps 优化)
# 更新时间 2025‑08‑05
#   • THREADS 默认改为 8，应用户需求。
#   • rclone 输出全部合并到 $SPEED_LOG，保证监控能抓到速率行；同时通过 tee 追加到 $LOGFILE。
#   • stats 日志级别固定为 INFO，避免被误过滤。
#   • monitor_speed 正则已同时支持 MiB/s、MB/s、MBytes/s。

set -euo pipefail
VERSION="3.4"

###################### 依赖检查 ######################
for cmd in jq rclone; do
  command -v $cmd >/dev/null 2>&1 || { echo "[ERROR] $cmd 未安装" >&2; exit 1; }
done

###################### 基本配置 ######################
WARC_LIST_URL="https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-30/warc.paths.gz"
DEST_PATH="/dx"
MAX_TRANSFER="700G"

THREADS=8          # 默认并发改为 8
CHUNK_SIZE="256M"
BUFFER_SIZE="2G"
MULTI_THREAD_STREAMS=8
CHECKERS=32
LOW_SPEED_MB=50
LOW_SPEED_SECONDS=45
STATS_INTERVAL="5s"

TMP_DIR="/tmp/warc_uploader"
LOG_DIR="./logs"
mkdir -p "$TMP_DIR" "$LOG_DIR"

export LANG=zh_CN.UTF-8
export LC_ALL=zh_CN.UTF-8

###################### 工具函数 ######################
cleanup() { local mon=$1; [[ -n "$mon" && $(kill -0 "$mon" 2>/dev/null || echo 0) ]] && { kill -TERM "$mon" 2>/dev/null || true; sleep 1; kill -KILL "$mon" 2>/dev/null || true; }; pkill -f "rclone copyurl.*$REMOTE:" 2>/dev/null || true; }

MAX_VERIFY=6; VERIFY_INTERVAL=30
verify_batch() { local last=$1 new tries=0; while (( tries<MAX_VERIFY )); do new=$(rclone size "$REMOTE:$DEST_PATH" --json | jq -r '.bytes // 0'); (( new>last )) && { echo "$new"; return 0; }; tries=$((tries+1)); sleep $VERIFY_INTERVAL; done; echo "$last"; return 1; }

###################### 欢迎信息 ######################
cat <<EOF
 ██████╗ ██████╗      ██████╗     ██████╗
██╔════╝██╔════╝     ██╔════╝     ╚════██╗
██║     ██║     █████╗██║  ███╗     █████╔╝
██║     ██║     ╚════╝██║   ██║     ╚═══██╗
╚██████╗╚██████╗     ╚██████╔╝    ██████╔╝
 ╚═════╝ ╚═════╝      ╚═════╝     ╚═════╝
🌐 CommonCrawl Global Uploader v$VERSION
📊 2.5 Gbps 带宽配置 (THREADS=$THREADS)
EOF

###################### 用户交互 ######################
read -rp "⏰ 循环间隔小时数 (默认25, 0=仅一次): " REPEAT_INTERVAL_HOURS
REPEAT_INTERVAL_HOURS=${REPEAT_INTERVAL_HOURS:-25}

ALL_REMOTES=$(rclone listremotes | sed 's/:$//')
IFS=$'\n' read -r -d '' -a REM_ARR < <(printf '%s\n' $ALL_REMOTES && printf '\0')
echo -e "\n🟢 可用节点:"; for r in "${REM_ARR[@]}"; do echo "   ├─ $r"; done
read -rp "🎯 选择节点 (空格分隔，留空=倒序全选): " -a SELECTED_REMOTES
if [[ ${#SELECTED_REMOTES[@]} -eq 0 ]]; then for (( idx=${#REM_ARR[@]}-1; idx>=0; idx-- )); do SELECTED_REMOTES+=("${REM_ARR[idx]}"); done; fi

read -rp "📍 起始行号 (默认1): " START_LINE; START_LINE=${START_LINE:-1}

###################### 下载 WARC 列表 ######################
WARC_FILE="$TMP_DIR/warc.paths"
echo -n "📥 下载 warc.paths.gz... "
curl -sL "$WARC_LIST_URL" | gunzip -c > "$WARC_FILE" && echo "完成" || { echo "失败"; exit 1; }
TOTAL_LINES=$(wc -l < "$WARC_FILE"); echo "总文件数: $TOTAL_LINES"

###################### 主循环 ######################
while :; do
  declare -A REMOTE_MB=(); TOTAL_MB=0
  echo -e "\n========== 新一轮上传 $(date '+%F %T') =========="
  for REMOTE in "${SELECTED_REMOTES[@]}"; do
    echo -e "\n┌─ 🚀 节点: $REMOTE"
    PROGRESS_FILE="$TMP_DIR/${REMOTE}.progress"; [[ -f "$PROGRESS_FILE" ]] || echo "$START_LINE" > "$PROGRESS_FILE"
    CURRENT=$(<"$PROGRESS_FILE")
    LAST_USED=$(rclone size "$REMOTE:$DEST_PATH" --json | jq -r '.bytes // 0')
    NO_PROGRESS=0; REMOTE_MB[$REMOTE]=0

    LOGFILE="$LOG_DIR/${REMOTE}_$(date +%F_%H-%M-%S).log"; SPEED_LOG="$TMP_DIR/${REMOTE}_speed.log"; FLAG_FILE="$TMP_DIR/${REMOTE}_slow.flag"

    while (( CURRENT<=TOTAL_LINES )); do
      END=$(( CURRENT+THREADS-1 )); (( END>TOTAL_LINES )) && END=$TOTAL_LINES
      echo -e "\n├─ 🚀 批次 $CURRENT-$END 共 $((END-CURRENT+1))"; echo 0 > "$FLAG_FILE"; :>"$SPEED_LOG"
      BATCH_LIST="$TMP_DIR/${REMOTE}_batch_${CURRENT}.txt"; BATCH_URLS="$TMP_DIR/${REMOTE}_urls_${CURRENT}.txt"
      sed -n "${CURRENT},${END}p" "$WARC_FILE" > "$BATCH_LIST"; sed "s|^|https://data.commoncrawl.org/|" "$BATCH_LIST" > "$BATCH_URLS"

      monitor_speed() {
        local slow=0 tick=0 sum avg; while [[ $(<"$FLAG_FILE") == 0 ]]; do sleep 5; tick=$((tick+1)); sum=0; for v in $(tail -n 20 "$SPEED_LOG" | grep -oE '[0-9.]+(MiB/s|MB/s|MBytes/s)'); do sum=$(awk -v s="$sum" -v v="$(echo $v | grep -oE '^[0-9.]+')" 'BEGIN{print s+v}'); done; avg=$(awk -v s="$sum" -v n="$(tail -n 20 "$SPEED_LOG" | grep -cE '(MiB/s|MB/s|MBytes/s)')" 'BEGIN{ if(n) printf "%.1f", s/n; else print 0 }'); (( tick%2==0 )) && printf "\r├─ 📈 平均 %.1f MiB/s " "$avg"; [[ $tick -le 6 ]] && continue; (( ${avg%.*}<LOW_SPEED_MB )) && slow=$((slow+5)) || slow=0; (( slow>=LOW_SPEED_SECONDS )) && { echo -e "\n├─ 🐌 低速 → 切批次"; echo 1 > "$FLAG_FILE"; return; }; (( tick*5 >= 600 )) && { echo -e "\n├─ ⏰ 超时"; echo 1 > "$FLAG_FILE"; return; }; done }
      monitor_speed & MON=$!

      PIDS=(); idx=0
      while IFS= read -r url && (( idx<THREADS )); do
        # 全部输出 >> $SPEED_LOG，再 tee 到 LOGFILE 供排错
        ( rclone copyurl "$url" "$REMOTE:$DEST_PATH" \
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
            --stats $STATS_INTERVAL \
            --stats-one-line \
            --stats-log-level INFO \
            --log-level INFO \
            --log-format "stats" 2>&1 | tee -a "$SPEED_LOG" >> "$LOGFILE" ) &
        PIDS+=("$!"); idx=$((idx+1))
      done < "$BATCH_URLS"
      echo "├─ ⚡ 启动 ${#PIDS[@]} 线程"

      while :; do [[ $(<"$FLAG_FILE") == 1 ]] && { echo "├─ 🛑 中止"; for p in "${PIDS[@]}"; do kill -TERM "$p" 2>/dev/null || true; done; break; }; alive=0; for p in "${PIDS[@]}"; do kill -0 "$p" 2>/dev/null && alive=$((alive+1)); done; (( alive==0 )) && break; sleep 3; done
      cleanup "$MON"

      AVG=$(grep -oE '[0-9.]+MiB/s' "$SPEED_LOG" | awk -F'MiB/s' '{sum+=$1} END{ if(NR) printf "%.1f", sum/NR; else print 0}')
      NEW_USED=$(verify_batch "$LAST_USED"); ok=$?; DIFF=$(( (NEW_USED-LAST_USED)/1024/1024 ))
      if (( ok==0 )); then echo "├─ ✅ +${DIFF} MB | 平均 ${AVG} MiB/s"; LAST_USED=$NEW_USED; NO_PROGRESS=0; REMOTE
