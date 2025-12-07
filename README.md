# ライブ配信チャット分析ツール

ライブ配信のチャット（コメント）CSVデータをアップロードし、AIを活用してコメントを分析し、Googleスプレッドシートに結果を出力するWebアプリケーションです。

## 機能

- CSVファイルのアップロードとデータ抽出
- AIによるコメント属性・感情分析
- Googleスプレッドシートへの結果出力（2シート構成）
- 統計情報の可視化

## セットアップ

### 1. 必要な環境

- Python 3.8以上
- OpenAI APIキー（GPT-5 miniを使用）
- Google Cloud プロジェクト（Google Sheets API有効化）
- Googleサービスアカウント認証情報

### 2. インストール

```bash
# リポジトリをクローン
cd live-stream_comment_analysis

# 仮想環境を作成（推奨）
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 依存パッケージをインストール
pip install -r requirements.txt
```

### 3. 環境変数の設定

プロジェクトルートに`.env`ファイルを作成し、以下の内容を設定してください：

```bash
# .envファイルを作成
touch .env  # または、テキストエディタで新規作成
```

`.env`ファイルの内容：

```env
# OpenAI API（GPT-5 miniを使用）
OPENAI_API_KEY=your_openai_api_key_here

# Google Sheets API
# Option 1: Service Account JSON file path
GOOGLE_SERVICE_ACCOUNT_FILE=path/to/service-account.json

# Option 2: Service Account JSON as string (alternative to file path)
# GOOGLE_CREDENTIALS_JSON={"type": "service_account", ...}
```

**注意**: `.env`ファイルは機密情報を含むため、Gitにコミットしないでください（`.gitignore`に含まれています）。

### 4. Google Cloud設定

1. [Google Cloud Console](https://console.cloud.google.com/)でプロジェクトを作成
2. Google Sheets APIを有効化
3. サービスアカウントを作成
4. サービスアカウントの認証情報（JSONファイル）をダウンロード
5. `.env`ファイルにパスを設定

## 使用方法

```bash
streamlit run app.py
```

ブラウザで`http://localhost:8501`にアクセスし、CSVファイルをアップロードしてください。

## CSVファイル形式

以下の列が必要です：

- `guest_id`: ゲストID
- `username`: ユーザー名
- `original_text`: コメント本文
- `inserted_at`: 投稿日時

## 出力されるスプレッドシート

### メインシート

- 全コメントデータ
- チャットの属性（8種類）
- チャット感情（4種類）
- 統計情報

### 質問シート

- 質問コメントのみ
- 回答状況
- 質問回答率

## ライセンス

MIT License

