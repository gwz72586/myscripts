#!/bin/bash

# === 功能：自动拉取 CommonCrawl warc.paths.gz 并轮询多账号上传，每账号限 700G ===

# === Step 1: 自动获取所有可用 remote ===
ALL_REMOTES=$(rclone listremotes | sed 's/:$//')
echo "🟢 检测到以下 remote 可用："
echo "$ALL_REMOTES"
echo
read -p "请输入你希望使用的 remote 名称（以空格分隔，例如：d1 d2 d3）: " -a SELECTED_REMOTES
echo "✅ 你选择的 remote 有：${SELECTED_REMOTES[*]}"
echo

# === Step 2: 配置 ===
CC_LIST_URL="https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-30/warc.paths.gz"
MAX_TRANSFER="700G"
THREADS=6
DEST_PATH="/dx/"
TMPDIR=".cc_log"
mkdir -p "$TMPDIR"

# === Step 3: 下载路径列表 ===
echo "📥 正在下载并解压路径列表..."
curl -sL "$CC_LIST_URL" | gunzip -c > "$TMPDIR/all_paths.txt"
TOTAL_LINES=$(wc -l < "$TMPDIR/all_paths.txt")

echo "📄 共解析到 $TOTAL_LINES 条路径，准备分配上传任务..."
echo

# === Step 4: 启动每个 remote 上传任务 ===
LINES_PER_REMOTE=$(( TOTAL_LINES / ${#SELECTED_REMOTES[@]} + 1 ))
START_LINE=1
TASK_INDEX=1

for REMOTE in "${SELECTED_REMOTES[@]}"; do
  END_LINE=$(( START_LINE + LINES_PER_REMOTE - 1 ))
  TASK_FILE="${TMPDIR}/task_${REMOTE}.txt"
  LOGFILE="${TMPDIR}/${REMOTE}.log"

  # 提取任务路径
  sed -n "${START_LINE},${END_LINE}p" "$TMPDIR/all_paths.txt" > "$TASK_FILE"

  echo "🚀 启动 $REMOTE 上传任务（第 $START_LINE ~ $END_LINE 行）..."
  
  (
    COUNT=0
    while read -r PATH_LINE; do
      URL="https://data.commoncrawl.org/CC-MAIN-2025-30/${PATH_LINE}"
      echo "[${REMOTE}] 上传：$URL"

      rclone copyurl "$URL" "${REMOTE}:${DEST_PATH}" \
        --auto-filename \
        --drive-chunk-size 256M \
        --buffer-size 256M \
        --drive-pacer-min-sleep 10ms \
        --drive-pacer-burst 200 \
        --disable-http2 \
        --max-transfer ${MAX_TRANSFER} \
        --stats-one-line -P >> "$LOGFILE" 2>&1

      ((COUNT++))
      echo "[${REMOTE}] 已上传 $COUNT 个文件"

    done < "$TASK_FILE"

    echo "✅ $REMOTE 上传完成或达到 700G 限额，日志保存在 $LOGFILE"
  ) &

  ((START_LINE = END_LINE + 1))
  ((TASK_INDEX++))
done

wait
echo
echo "✅ 全部 remote 上传完成，开始统计上传结果..."

# === Step 5: 上传统计 ===
TOTAL_FILES=0
TOTAL_SIZE=0

printf "\n📊 上传汇总结果：\n"
printf "%-10s %-15s %-15s\n" "REMOTE" "文件数" "传输总量"

for REMOTE in "${SELECTED_REMOTES[@]}"; do
  LOGFILE="${TMPDIR}/${REMOTE}.log"

  # 文件数 = Copied 或 Checks 出现次数
  FILE_COUNT=$(grep -c -E "Copied \(new\)|Checks:" "$LOGFILE")

  # 提取最后一条 Transferred 数据
  SIZE_LINE=$(grep "Transferred:" "$LOGFILE" | tail -n1 | awk '{print $2,$3}')
  
  if [[ $SIZE_LINE == *"G"* ]]; then
    SIZE_GB=$(echo $SIZE_LINE | sed 's/[^0-9\.]//g')
  elif [[ $SIZE_LINE == *"M"* ]]; then
    SIZE_MB=$(echo $SIZE_LINE | sed 's/[^0-9\.]//g')
    SIZE_GB=$(awk "BEGIN{printf \"%.2f\", $SIZE_MB/1024}")
  else
    SIZE_GB="0.00"
  fi

  TOTAL_FILES=$((TOTAL_FILES + FILE_COUNT))
  TOTAL_SIZE=$(awk "BEGIN{printf \"%.2f\", $TOTAL_SIZE + $SIZE_GB}")

  printf "%-10s %-15s %-15s\n" "$REMOTE" "$FILE_COUNT" "${SIZE_GB}G"
done

printf "\n🧮 总计上传文件数：%s 个\n" "$TOTAL_FILES"
printf "💾 总上传数据量：%s GB\n\n" "$TOTAL_SIZE"
echo "📂 日志目录：$TMPDIR/"
