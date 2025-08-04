#!/bin/bash

SESSION_NAME="uploader"
SCRIPT_URL="https://raw.githubusercontent.com/gwz72586/myscripts/main/2.sh"

# 检查 tmux 是否存在
if ! command -v tmux &>/dev/null; then
    echo "🔧 未检测到 tmux，正在安装..."
    apt update && apt install tmux -y || {
        echo "❌ 安装 tmux 失败"; exit 1;
    }
fi

# 判断是否已有会话
if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
    echo "⚠️ 发现已有 [$SESSION_NAME] 会话，正在附加..."
    tmux attach -t "$SESSION_NAME"
else
    echo "🚀 创建新的 tmux 会话 [$SESSION_NAME] 并开始上传任务..."
    tmux new-session -s "$SESSION_NAME" -d
    tmux send-keys -t "$SESSION_NAME" "bash <(curl -sL $SCRIPT_URL)" C-m
    sleep 2
    tmux attach -t "$SESSION_NAME"
fi

