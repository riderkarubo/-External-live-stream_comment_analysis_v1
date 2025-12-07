#!/bin/bash

# Streamlitアプリの再起動スクリプト（キャッシュクリア含む）

echo "🔄 Streamlitアプリを再起動します..."

# プロジェクトのディレクトリに移動
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "📁 作業ディレクトリ: $SCRIPT_DIR"

# Pythonキャッシュをクリア
echo "🧹 Pythonキャッシュ（__pycache__）を削除しています..."
find . -type d -name "__pycache__" -exec rm -r {} + 2>/dev/null || true
find . -type f -name "*.pyc" -delete 2>/dev/null || true
find . -type f -name "*.pyo" -delete 2>/dev/null || true

echo "✅ キャッシュの削除が完了しました"

# 仮想環境が存在するか確認
if [ -d "env" ]; then
    echo "🐍 仮想環境をアクティベートしています..."
    source env/bin/activate
else
    echo "⚠️  仮想環境が見つかりません。グローバルのPython環境を使用します。"
fi

# Streamlitプロセスが実行中か確認
STREAMLIT_PID=$(pgrep -f "streamlit run app.py" | head -1)
if [ ! -z "$STREAMLIT_PID" ]; then
    echo "⏹️  実行中のStreamlitプロセスを停止しています（PID: $STREAMLIT_PID）..."
    kill $STREAMLIT_PID 2>/dev/null || true
    sleep 2
fi

# Streamlitアプリを起動
echo "🚀 Streamlitアプリを起動しています..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

streamlit run app.py

