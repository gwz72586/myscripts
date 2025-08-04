#!/bin/bash
# CC-G 3.1 - CommonCrawl Global Uploader (2.5 Gbps 优化)
# ✨ 关键改动
#   1. 用 rclone size --json 即时验证容量，解决 Google Drive about 延迟问题。
#   2. 新增 verify_batch() 带重试窗口；连续验证失败 3 次才视为无进展。
#   3. 实时统计并输出每批次平均速度与实际新增容量。
#   4. 保持原有低速切换逻辑。

set -euo pipefail

###################### 基本配置 ######################
WARC_LIST_URL="https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-30/warc.paths.gz"
DEST_PATH="/dx"
MAX_TRANSFER="700G"

# 2.5 Gbps 带宽优化
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

###################### 工具函数 ######################
# 进程清理
cleanup() {
  local mon_pid=${1:-}
  [[ -n "$mon_pid" && $(kill -0 "$mon_pid" 2>/dev/null || echo 0) ]] && {
    kill -TERM "$mon_pid" 2>/dev/null || true; sleep 1; kill -KILL "$mon_pid" 2>/dev/null || true;
  }
  pkill -f "rclone copyurl.*$REMOTE:" 2>/dev/null || true
}

# 批次容量验证（使用 rclone size）
MAX_VERIFY_ATTEMPTS=6  # 共尝试 6×30 s ≈ 3 min
VERIFY_INTERVAL=30
verify_batch() {
  local last_bytes=$1
  local new_bytes=0
  local attempts=0
  while (( attempts < MAX_VERIFY_ATTEMPTS )); do
    new_bytes=$(rclone size "$REMOTE:$DEST_PATH" --json 2>/dev/null | jq -r '.bytes // 0')
    if (( new_bytes > last_bytes )); then
      echo "$new_bytes"  # 返回最新已用字节数
      return 0
    fi
    attempts=$((attempts+1))
    sleep $VERIFY_INTERVAL
  done
  echo "$last_bytes"  # 未变化
  return 1
}

###################### 用户交互 ######################
cat <<'EOF'
 ██████╗ ██████╗      ██████╗     ██████╗
██╔════╝██╔════╝     ██╔════╝     ╚════██╗
██║     ██║     █████╗██║  ███╗     █████╔╝
██║     ██║     ╚════╝██║   ██║     ╚═══██╗
╚██████╗╚██████╗     ╚██████╔╝    ██████╔╝
 ╚═════╝ ╚═════╝      ╚═════╝     ╚═════╝
🌐 CommonCrawl Global Uploader v3.1
📊 优化版本 - 2.5 Gbps 带宽
EOF

echo "🔧 THREADS=$THREADS | CHUNK=$CHUNK_SIZE | BUFFER=$BUFFER_SIZE"

read -rp "⏰ 循环间隔小时数 (0=仅执行一次): " REPEAT_INTERVAL_HOURS
ALL_REMOTES=$(rclone listremotes | sed 's/:$//')
echo -e "\n🟢 可用存储节点:\n$(echo "$ALL_REMOTES" | sed 's/^/   ├─ /')"
read -rp "🎯 选择要使用的节点 (空格分隔): " -a SELECTED_REMOTES
read -rp "📍 起始文件行号 (默认1): " START_LINE
START_LINE=${START_LINE:-1}

###################### 下载文件列表 ######################
WARC_FILE="$TMP_DIR/warc.paths"
echo -n "📥 获取文件列表... "
curl -sL "$WARC_LIST_URL" | gunzip -c > "$WARC_FILE" && echo "完成" || { echo "失败"; exit 1; }
TOTAL_LINES=$(wc -l < "$WARC_FILE")
echo "总文件数: $TOTAL_LINES"

###################### 主循环 ######################
while :; do
  echo -e "\n========== 新一轮上传 $(date '+%F %T') =========="
  for REMOTE in "${SELECTED_REMOTES[@]}"; do
    echo -e "\n┌─ 🚀 节点: $REMOTE"
    PROGRESS_FILE="$TMP_DIR/${REMOTE}.progress"
    USED_FILE="$TMP_DIR/${REMOTE}.used"
    LOGFILE="$LOG_DIR/${REMOTE}_$(date +%F_%H-%M-%S).log"
    SPEED_LOG="$TMP_DIR/${REMOTE}_speed.log"
    FLAG_FILE="$TMP_DIR/${REMOTE}_slow.flag"

    [[ -f "$PROGRESS_FILE" ]] || echo "$START_LINE" > "$PROGRESS_FILE"
    CURRENT_LINE=$(<"$PROGRESS_FILE")

    # 初始容量
    LAST_USED=$(rclone size "$REMOTE:$DEST_PATH" --json | jq -r '.bytes // 0')
    echo "$LAST_USED" > "$USED_FILE"

    NO_PROGRESS=0

    while (( CURRENT_LINE <= TOTAL_LINES )); do
      BATCH_END=$(( CURRENT_LINE + THREADS - 1 ))
      (( BATCH_END > TOTAL_LINES )) && BATCH_END=$TOTAL_LINES

      echo -e "\n├─ 🚀 批次 $CURRENT_LINE-$BATCH_END 共 $((BATCH_END-CURRENT_LINE+1)) 文件"

      # 准备批次列表
      BATCH_LIST="$TMP_DIR/${REMOTE}_batch_${CURRENT_LINE}.txt"
      BATCH_URLS="$TMP_DIR/${REMOTE}_urls_${CURRENT_LINE}.txt"
      sed -n "${CURRENT_LINE},${BATCH_END}p" "$WARC_FILE" > "$BATCH_LIST"
      sed "s|^|https://data.commoncrawl.org/|" "$BATCH_LIST" > "$BATCH_URLS"

      # 重置监控标记
      echo 0 > "$FLAG_FILE"; : > "$SPEED_LOG"

      # 速度监控后台进程
      monitor_speed() {
        local slow=0 checks=0 tot=0
        while [[ $(<"$FLAG_FILE") == 0 ]]; do
          sleep 5; checks=$((checks+1)); tot=0
          # 取最近统计
          local lines=$(tail -n 20 "$SPEED_LOG" | grep -o '[0-9.]\+MiB/s' || true)
          for s in $lines; do tot=$(awk -v t="$tot" -v v="${s%MiB/s}" 'BEGIN{print t+v}'); done
          local avg=0; [[ $checks -gt 0 && $lines ]] && avg=$(awk -v t="$tot" -v n="$(echo "$lines" | wc -w)" 'BEGIN{printf "%.1f", t/n}')
          (( checks % 2 == 0 )) && printf "\r├─ 📈 即时速度 %.1f MiB/s " "$avg"
          # 启动宽限 & 低速检测
          [[ $checks -le 6 ]] && continue  # 前 30 s 不检测
          (( ${avg%.*} < LOW_SPEED_MB )) && slow=$((slow+5)) || slow=0
          if (( slow >= LOW_SPEED_SECONDS )); then
            echo -e "\n├─ 🐌 低速触发: $avg MiB/s < $LOW_SPEED_MB"
            echo 1 > "$FLAG_FILE"; return; fi
          # 安全上限
          (( checks*5 >= 600 )) && { echo -e "\n├─ ⏰ 超时切换"; echo 1 > "$FLAG_FILE"; return; }
        done
      }
      monitor_speed & MON_PID=$!

      # 启动上传子进程
      UPLOAD_PIDS=(); idx=0
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
          >> "$SPEED_LOG" 2>> "$LOGFILE" &
        UPLOAD_PIDS+=("$!"); idx=$((idx+1))
        echo "├─ 🔗 启动线程 $idx: ${filename:0:40}..."
      done < "$BATCH_URLS"
      echo "├─ ⚡ 共 ${#UPLOAD_PIDS[@]} 线程"

      # 等待上传完成或低速触发
      while :; do
        [[ $(<"$FLAG_FILE") == 1 ]] && {
          echo "├─ 🛑 低速中止批次"; for p in "${UPLOAD_PIDS[@]}"; do kill -TERM "$p" 2>/dev/null || true; done; break; }
        alive=0; for p in "${UPLOAD_PIDS[@]}"; do kill -0 "$p" 2>/dev/null && alive=$((alive+1)); done
        (( alive == 0 )) && break; sleep 3
      done

      # 清理
      cleanup "$MON_PID"

      # 统计平均速度
      AVG_SPEED=$(grep -o '[0-9.]\+MiB/s' "$SPEED_LOG" | awk -F'MiB/s' '{sum+=$1} END{ if(NR) printf "%.1f", sum/NR; else print 0}')

      # 容量验证
      NEW_USED=$(verify_batch "$LAST_USED") && verify_ok=$? || verify_ok=$?
      size_diff=$(( (NEW_USED - LAST_USED)/1024/1024 ))
      if (( verify_ok == 0 )); then
        echo "├─ ✅ 批次完成 | 新增 ${size_diff} MB | 平均速度 ${AVG_SPEED} MiB/s"
        LAST_USED=$NEW_USED; NO_PROGRESS=0
      else
        echo "├─ ⚠️  无容量变化 (尝试 ${NO_PROGRESS}/3) | 平均速度 ${AVG_SPEED} MiB/s"
        NO_PROGRESS=$((NO_PROGRESS+1))
      fi

      # 连续失败切节点
      if (( NO_PROGRESS >= 3 )); then
        echo "└─ 🚫 连续 3 次无进展，切换节点"
        break
      fi

      # 记录进度
      CURRENT_LINE=$(( BATCH_END + 1 ))
      echo "$CURRENT_LINE" > "$PROGRESS_FILE"
      # 清理临时
      rm -f "$BATCH_LIST" "$BATCH_URLS" "$SPEED_LOG"
    done
    rm -f "$FLAG_FILE" "$SPEED_LOG"
    echo "└─ ✅ 节点 $REMOTE 完成本轮"
  done

  (( REPEAT_INTERVAL_HOURS == 0 )) && { echo "🎉 全部任务完成"; exit 0; }
  echo "💤 休眠 ${REPEAT_INTERVAL_HOURS}h 后继续..."; sleep $(( REPEAT_INTERVAL_HOURS*3600 ))
done
