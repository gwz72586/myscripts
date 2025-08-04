#!/bin/bash

REMOTE=d1                # 只用一个 remote，按需修改
CC_LIST_URL="https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-30/warc.paths.gz"
DEST_PATH="/dx/"
MAX_TRANSFER="700G"
THREADS=8                # 并发线程数
TMPDIR=".cc_log"
mkdir -p "$TMPDIR"

# 选择起始行
read -p "请输入从第几行开始抓取路径（默认 1）: " INPUT_START_LINE
if [[ "$INPUT_START_LINE" =~ ^[0-9]+$ ]] && [ "$INPUT_START_LINE" -ge 1 ]]; then
  START_LINE=$INPUT_START_LINE
else
  START_LINE=1
fi

# 强制终止处理
trap 'echo -e "\n⛔ 检测到中断，正在终止所有上传进程..."; pkill -P $$; exit 1' INT TERM

echo "📥 正在下载并解压路径列表..."
curl -sL "$CC_LIST_URL" | gunzip -c > "$TMPDIR/all_paths.txt"
TOTAL_LINES=$(wc -l < "$TMPDIR/all_paths.txt")
echo "📄 共解析到 $TOTAL_LINES 条路径，将从第 $START_LINE 行开始..."

# 分配任务文件
sed -n "${START_LINE},${TOTAL_LINES}p" "$TMPDIR/all_paths.txt" > "$TMPDIR/task_${REMOTE}.txt"
LOGFILE="${TMPDIR}/${REMOTE}.log"

# 并发8线程上传
cat "$TMPDIR/task_${REMOTE}.txt" | xargs -I{} -P${THREADS} bash -c '
  URL="https://data.commoncrawl.org/{}"
  # 实时显示网盘端统计
  UPLOADED=$(rclone size "'"${REMOTE}:${DEST_PATH}"'" 2>/dev/null | grep "Total size" | awk -F: "{print \$2}" | xargs)
  echo -e "\033[1;32m📦 当前 ['"$REMOTE"'] 已累计上传：\$UPLOADED / '"${MAX_TRANSFER}"'\033[0m"
  echo "['"$REMOTE"'] 上传：\$URL"
  rclone copyurl "\$URL" "'"${REMOTE}:${DEST_PATH}"'" \
    --auto-filename \
    --drive-chunk-size 512M \
    --buffer-size 512M \
    --drive-pacer-min-sleep 10ms \
    --drive-pacer-burst 200 \
    --tpslimit 10 \
    --tpslimit-burst 100 \
    --multi-thread-streams 8 \
    --transfers 8 \
    --disable-http2 \
    --max-transfer '"${MAX_TRANSFER}"' \
    --stats-one-line -P >> "'"$LOGFILE"'" 2>&1
'

echo "✅ $REMOTE 上传全部任务完成，日志保存在 $LOGFILE"

# 上传统计
FILE_COUNT=$(grep -c -E "Copied \(new\)|Checks:" "$LOGFILE")
SIZE_LINE=$(grep "Transferred:" "$LOGFILE" | tail -n1 | awk '{print $2,$3}')
if [[ $SIZE_LINE == *"G"* ]]; then
  SIZE_GB=$(echo $SIZE_LINE | sed 's/[^0-9\.]//g')
elif [[ $SIZE_LINE == *"M"* ]]; then
  SIZE_MB=$(echo $SIZE_LINE | sed 's/[^0-9\.]//g')
  SIZE_GB=$(awk "BEGIN{printf \"%.2f\", $SIZE_MB/1024}")
else
  SIZE_GB="0.00"
fi

printf "\n📊 上传汇总：\n"
printf "%-10s %-15s %-15s\n" "REMOTE" "文件数" "传输总量"
printf "%-10s %-15s %-15s\n" "$REMOTE" "$FILE_COUNT" "${SIZE_GB}G"
