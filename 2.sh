#!/bin/bash

# === Step 1: 获取可用 remote 并让用户选择 ===
ALL_REMOTES=$(rclone listremotes | sed 's/:$//')
echo "🟢 检测到以下 remote 可用："
echo "$ALL_REMOTES"
echo
read -p "请输入你希望使用的 remote 名称（以空格分隔，例如：d1 d2 d3）: " -a SELECTED_REMOTES
echo "✅ 你选择的 remote 有：${SELECTED_REMOTES[*]}"
echo

# === Step 2: 选择从第几行开始抓取 ===
read -p "请输入从第几行开始抓取路径（默认 1）: " INPUT_START_LINE
if [[ "$INPUT_START_LINE" =~ ^[0-9]+$ && "$INPUT_START_LINE" -ge 1 ]]; then
  START_LINE=$INPUT_START_LINE
else
  START_LINE=1
fi

# === Step 3: 公共参数定义 ===
CC_LIST_URL="https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-30/warc.paths.gz"
DEST_PATH="/dx/"
MAX_TRANSFER="700G"
THREADS=8
TMPDIR=".cc_log"
mkdir -p "$TMPDIR"

# 捕获 Ctrl+C 中断
trap 'echo -e "\n⛔️ 已中断，终止所有任务..."; pkill -P $$; exit 1' INT TERM

# 下载链接列表
echo "📥 正在下载并解压 warc.paths.gz..."
curl -sL "$CC_LIST_URL" | gunzip -c > "$TMPDIR/all_paths.txt"
TOTAL_LINES=$(wc -l < "$TMPDIR/all_paths.txt")
echo "📄 共解析 $TOTAL_LINES 条路径，将从第 $START_LINE 行开始..."

# === Step 4: 将任务均分给每个 remote ===
TOTAL_REMOTES=${#SELECTED_REMOTES[@]}
LINES_PER_REMOTE=$(( (TOTAL_LINES - START_LINE + 1) / TOTAL_REMOTES ))

for ((i = 0; i < TOTAL_REMOTES; i++)); do
  REMOTE=${SELECTED_REMOTES[$i]}
  FROM_LINE=$(( START_LINE + i * LINES_PER_REMOTE ))
  TO_LINE=$(( FROM_LINE + LINES_PER_REMOTE - 1 ))

  [ $i -eq $((TOTAL_REMOTES - 1)) ] && TO_LINE=$TOTAL_LINES

  TASK_FILE="$TMPDIR/task_${REMOTE}.txt"
  sed -n "${FROM_LINE},${TO_LINE}p" "$TMPDIR/all_paths.txt" > "$TASK_FILE"

  echo "📦 分配给 [$REMOTE] 的任务：$TASK_FILE（$FROM_LINE - $TO_LINE 行）"

  # === Step 5: 每个 remote 顺序上传，8 并发 ===
  LOGFILE="${TMPDIR}/${REMOTE}.log"
  > "$LOGFILE"

  echo "🚀 开始上传 remote: [$REMOTE]..."
  cat "$TASK_FILE" | xargs -P $THREADS -I{} bash -c '
    URL="https://data.commoncrawl.org/{}"
    REMOTE="'"$REMOTE"'"
    DEST_PATH="'"$DEST_PATH"'"
    MAX_TRANSFER="'"$MAX_TRANSFER"'"
    LOGFILE="'"$LOGFILE"'"

    # 显示当前上传量
    UPLOADED=$(rclone size "$REMOTE:$DEST_PATH" 2>/dev/null | grep "Total size" | awk -F: "{print \$2}" | xargs)
    echo -e "\033[1;32m📦 [$REMOTE] 当前已上传：$UPLOADED / $MAX_TRANSFER\033[0m"
    echo "[$REMOTE] 上传：$URL"

    rclone copyurl "$URL" "$REMOTE:$DEST_PATH" \
      --auto-filename \
      --drive-chunk-size 512M \
      --buffer-size 512M \
      --drive-pacer-min-sleep 10ms \
      --drive-pacer-burst 200 \
      --multi-thread-streams 8 \
      --transfers 8 \
      --tpslimit 10 \
      --tpslimit-burst 100 \
      --disable-http2 \
      --max-transfer "$MAX_TRANSFER" \
      --stats-one-line -P >> "$LOGFILE" 2>&1
  '

  echo "✅ [$REMOTE] 上传完成。"

  # 汇总统计
  FILE_COUNT=$(grep -c -E "Copied \(new\)|Checks:" "$LOGFILE")
  SIZE_LINE=$(grep "Transferred:" "$LOGFILE" | tail -n1 | awk '{print $2,$3}')
  if [[ "$SIZE_LINE" == *"G"* ]]; then
    SIZE_GB=$(echo "$SIZE_LINE" | sed 's/[^0-9.]//g')
  elif [[ "$SIZE_LINE" == *"M"* ]]; then
    SIZE_MB=$(echo "$SIZE_LINE" | sed 's/[^0-9.]//g')
    SIZE_GB=$(awk "BEGIN{printf \"%.2f\", $SIZE_MB/1024}")
  else
    SIZE_GB="0.00"
  fi

  echo -e "\n📊 [$REMOTE] 上传汇总："
  printf "%-10s %-15s %-15s\n" "REMOTE" "文件数" "传输总量"
  printf "%-10s %-15s %-15s\n" "$REMOTE" "$FILE_COUNT" "${SIZE_GB}G"
  echo "-----------------------------"

done

echo "🎉 所有 remote 上传任务全部完成。"
