"""Googleスプレッドシート操作モジュール"""
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json
import os
from typing import Dict, List, Optional, Callable
import pandas as pd
import time
from config import (
    GOOGLE_SERVICE_ACCOUNT_FILE,
    GOOGLE_CREDENTIALS_JSON,
    COLOR_MAP,
    CHAT_ATTRIBUTES,
    CHAT_SENTIMENTS,
    ANSWER_STATUSES,
    COMPANY_NAME
)


def get_credentials():
    """Google認証情報を取得"""
    if GOOGLE_SERVICE_ACCOUNT_FILE and os.path.exists(GOOGLE_SERVICE_ACCOUNT_FILE):
        return service_account.Credentials.from_service_account_file(
            GOOGLE_SERVICE_ACCOUNT_FILE,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
    elif GOOGLE_CREDENTIALS_JSON:
        credentials_info = json.loads(GOOGLE_CREDENTIALS_JSON)
        return service_account.Credentials.from_service_account_info(
            credentials_info,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
    else:
        raise ValueError("Google認証情報が設定されていません。")


def get_service():
    """Google Sheets APIサービスを取得"""
    creds = get_credentials()
    service = build("sheets", "v4", credentials=creds)
    return service


def create_spreadsheet(title: str, progress_callback: Optional[Callable] = None) -> str:
    """
    新しいスプレッドシートを作成
    
    Args:
        title: スプレッドシートのタイトル
        progress_callback: 進捗コールバック関数
        
    Returns:
        スプレッドシートID
    """
    try:
        if progress_callback:
            progress_callback("スプレッドシートを作成中...", 0.1)
        
        service = get_service()
        
        spreadsheet = {
            "properties": {
                "title": title
            }
        }
        
        spreadsheet = service.spreadsheets().create(body=spreadsheet).execute()
        spreadsheet_id = spreadsheet.get("spreadsheetId")
        
        if progress_callback:
            progress_callback("スプレッドシートを作成しました", 0.2)
        
        return spreadsheet_id
        
    except HttpError as error:
        error_msg = f"スプレッドシート作成エラー: {error}"
        print(error_msg)
        raise Exception(error_msg) from error
    except Exception as error:
        error_msg = f"予期しないエラー: {str(error)}"
        print(error_msg)
        raise Exception(error_msg) from error


def write_data_to_sheet(
    spreadsheet_id: str,
    sheet_name: str,
    data: List[List],
    start_cell: str = "A1",
    progress_callback: Optional[Callable] = None
):
    """
    データをシートに書き込む
    
    Args:
        spreadsheet_id: スプレッドシートID
        sheet_name: シート名
        data: 書き込むデータ（2次元リスト）
        start_cell: 開始セル（例: "A1"）
        progress_callback: 進捗コールバック関数
    """
    try:
        if progress_callback:
            progress_callback(f"データを書き込んでいます... ({len(data)}行)", 0.0)
        
        service = get_service()
        
        range_name = f"{sheet_name}!{start_cell}"
        
        # 大量データの場合は分割して書き込み
        max_rows_per_request = 5000
        if len(data) > max_rows_per_request:
            # データを分割
            for i in range(0, len(data), max_rows_per_request):
                chunk = data[i:i + max_rows_per_request]
                end_row = i + len(chunk)
                
                # 範囲を計算
                start_row_num = i + 1 if i > 0 else 1
                chunk_range = f"{sheet_name}!{start_cell.split('!')[-1].split(':')[0].replace('A', 'A').replace('1', str(start_row_num))}"
                
                body = {"values": chunk}
                
                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=chunk_range,
                    valueInputOption="RAW",
                    body=body
                ).execute()
                
                if progress_callback:
                    progress = min(0.9, end_row / len(data) * 0.8)
                    progress_callback(f"データを書き込んでいます... ({end_row}/{len(data)}行)", progress)
        else:
            body = {"values": data}
            
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption="RAW",
                body=body
            ).execute()
        
        if progress_callback:
            progress_callback("データの書き込みが完了しました", 0.9)
        
    except HttpError as error:
        error_msg = f"データ書き込みエラー: {error}"
        print(error_msg)
        raise Exception(error_msg) from error
    except Exception as error:
        error_msg = f"予期しないエラー: {str(error)}"
        print(error_msg)
        raise Exception(error_msg) from error


def apply_data_validation(
    spreadsheet_id: str,
    sheet_id: int,
    column_index: int,
    options: List[str],
    start_row: int,
    end_row: int
):
    """
    データ検証（ドロップダウン）を適用
    
    Args:
        spreadsheet_id: スプレッドシートID
        sheet_id: シートID
        column_index: 列インデックス（0始まり）
        options: ドロップダウンオプション
        start_row: 開始行（0始まり）
        end_row: 終了行（0始まり）
    """
    try:
        service = get_service()
        
        requests = [{
            "setDataValidation": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": start_row,
                    "endRowIndex": end_row,
                    "startColumnIndex": column_index,
                    "endColumnIndex": column_index + 1
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [{"userEnteredValue": opt} for opt in options]
                    },
                    "showCustomUi": True,
                    "strict": False
                }
            }
        }]
        
        body = {"requests": requests}
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()
        
    except HttpError as error:
        error_msg = f"データ検証適用エラー: {error}"
        print(error_msg)
        raise Exception(error_msg) from error
    except Exception as error:
        error_msg = f"予期しないエラー: {str(error)}"
        print(error_msg)
        raise Exception(error_msg) from error


def apply_color_formatting(
    spreadsheet_id: str,
    sheet_id: int,
    column_index: int,
    value_color_map: Dict[str, Dict],
    data: List[List],
    header_rows: int = 1,
    progress_callback: Optional[Callable] = None
):
    """
    セルに色を適用（値に基づく）- 最適化版
    
    Args:
        spreadsheet_id: スプレッドシートID
        sheet_id: シートID
        column_index: 列インデックス（0始まり）
        value_color_map: 値と色のマッピング
        data: データ（2次元リスト）
        header_rows: ヘッダー行数
        progress_callback: 進捗コールバック関数
    """
    try:
        service = get_service()
        
        # 値ごとに色をグループ化して効率化
        color_groups: Dict[str, List[int]] = {}
        
        for row_idx, row in enumerate(data):
            if row_idx < header_rows:
                continue  # ヘッダー行はスキップ
            
            if column_index >= len(row):
                continue
            
            cell_value = str(row[column_index]).strip()
            
            # 値に対応する色を取得
            color_key = None
            for key in value_color_map.keys():
                if key in cell_value or cell_value == key:
                    color_key = key
                    break
            
            if color_key:
                if color_key not in color_groups:
                    color_groups[color_key] = []
                color_groups[color_key].append(row_idx)
        
        if not color_groups:
            return
        
        # 同じ色のセルを範囲でまとめて処理
        requests = []
        total_cells = sum(len(rows) for rows in color_groups.values())
        processed = 0
        
        for color_key, row_indices in color_groups.items():
            color = value_color_map.get(color_key)
            if not color:
                continue
            
            # 連続する行を範囲としてまとめる
            row_indices.sort()
            ranges = []
            start = row_indices[0]
            end = row_indices[0]
            
            for idx in row_indices[1:]:
                if idx == end + 1:
                    end = idx
                else:
                    ranges.append((start, end))
                    start = idx
                    end = idx
            ranges.append((start, end))
            
            # 範囲ごとにリクエストを作成
            for start_row, end_row in ranges:
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": start_row,
                            "endRowIndex": end_row + 1,
                            "startColumnIndex": column_index,
                            "endColumnIndex": column_index + 1
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": color
                            }
                        },
                        "fields": "userEnteredFormat.backgroundColor"
                    }
                })
                processed += (end_row - start_row + 1)
                
                if progress_callback:
                    progress = min(0.9, processed / total_cells * 0.8)
                    progress_callback(f"色付けを適用中... ({processed}/{total_cells})", progress)
        
        if requests:
            # リクエストをバッチで実行（Google APIの制限に合わせて分割）
            batch_size = 50  # より安全なサイズに変更
            total_batches = (len(requests) + batch_size - 1) // batch_size
            
            for i in range(0, len(requests), batch_size):
                batch_requests = requests[i:i + batch_size]
                body = {"requests": batch_requests}
                
                # リトライロジック
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        service.spreadsheets().batchUpdate(
                            spreadsheetId=spreadsheet_id,
                            body=body
                        ).execute()
                        break
                    except HttpError as e:
                        if attempt < max_retries - 1:
                            wait_time = (attempt + 1) * 2  # 指数バックオフ
                            time.sleep(wait_time)
                            if progress_callback:
                                progress_callback(f"リトライ中... ({attempt + 1}/{max_retries})", 0.85)
                        else:
                            raise
                
                # レート制限対策の待機
                if i + batch_size < len(requests):
                    time.sleep(0.5)
        
        if progress_callback:
            progress_callback("色付けが完了しました", 0.95)
        
    except HttpError as error:
        error_msg = f"色付け適用エラー: {error}"
        print(error_msg)
        raise Exception(error_msg) from error
    except Exception as error:
        error_msg = f"予期しないエラー: {str(error)}"
        print(error_msg)
        raise Exception(error_msg) from error


def create_sheet(spreadsheet_id: str, sheet_name: str) -> int:
    """
    新しいシートを作成
    
    Args:
        spreadsheet_id: スプレッドシートID
        sheet_name: シート名
        
    Returns:
        シートID
    """
    try:
        service = get_service()
        
        requests = [{
            "addSheet": {
                "properties": {
                    "title": sheet_name
                }
            }
        }]
        
        body = {"requests": requests}
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()
        
        sheet_id = response["replies"][0]["addSheet"]["properties"]["sheetId"]
        return sheet_id
        
    except HttpError as error:
        error_msg = f"シート作成エラー: {error}"
        print(error_msg)
        raise Exception(error_msg) from error
    except Exception as error:
        error_msg = f"予期しないエラー: {str(error)}"
        print(error_msg)
        raise Exception(error_msg) from error


def get_sheet_id(spreadsheet_id: str, sheet_name: str) -> Optional[int]:
    """
    シートIDを取得
    
    Args:
        spreadsheet_id: スプレッドシートID
        sheet_name: シート名
        
    Returns:
        シートID（見つからない場合はNone）
    """
    try:
        service = get_service()
        
        spreadsheet = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id
        ).execute()
        
        for sheet in spreadsheet.get("sheets", []):
            if sheet["properties"]["title"] == sheet_name:
                return sheet["properties"]["sheetId"]
        
        return None
        
    except HttpError as error:
        print(f"シートID取得エラー: {error}")
        return None
    except Exception as error:
        print(f"予期しないエラー: {error}")
        return None


def update_sheet_name(spreadsheet_id: str, sheet_id: int, new_name: str):
    """
    シート名を変更
    
    Args:
        spreadsheet_id: スプレッドシートID
        sheet_id: シートID
        new_name: 新しいシート名
    """
    try:
        service = get_service()
        
        requests = [{
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "title": new_name
                },
                "fields": "title"
            }
        }]
        
        body = {"requests": requests}
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()
        
    except HttpError as error:
        error_msg = f"シート名変更エラー: {error}"
        print(error_msg)
        raise Exception(error_msg) from error
    except Exception as error:
        error_msg = f"予期しないエラー: {str(error)}"
        print(error_msg)
        raise Exception(error_msg) from error


def create_main_sheet(
    spreadsheet_id: str,
    df: pd.DataFrame,
    statistics: Dict,
    progress_callback: Optional[Callable] = None
):
    """
    メインシートを作成
    
    Args:
        spreadsheet_id: スプレッドシートID
        df: データフレーム
        statistics: 統計情報
        progress_callback: 進捗コールバック関数
    """
    try:
        if progress_callback:
            progress_callback("メインシートを作成中...", 0.0)
        # シートIDを取得（既存のSheet1を使用、なければ作成）
        sheet_name = "メインシート"
        if progress_callback:
            progress_callback("シート情報を取得中...", 0.1)
        
        sheet_id = get_sheet_id(spreadsheet_id, "Sheet1")
        if not sheet_id:
            sheet_id = get_sheet_id(spreadsheet_id, "シート1")
            if not sheet_id:
                # デフォルトシートのIDを取得
                service = get_service()
                spreadsheet = service.spreadsheets().get(
                    spreadsheetId=spreadsheet_id
                ).execute()
                sheet_id = spreadsheet["sheets"][0]["properties"]["sheetId"]
        
        # シート名を変更
        update_sheet_name(spreadsheet_id, sheet_id, sheet_name)
        
        if progress_callback:
            progress_callback("データを準備中...", 0.15)
        
        # 統計情報を作成
        stats_data = [
            ["=== 統計情報 ==="],
            [f"全コメント件数: {statistics['total_comments']}"],
            [""],
            ["【チャットの属性別件数】"]
        ]
        
        for attr, count in statistics["attribute_counts"].items():
            stats_data.append([f"{attr}: {count}件"])
        
        stats_data.append([""])
        stats_data.append(["【チャット感情別件数】"])
        
        for sentiment, count in statistics["sentiment_counts"].items():
            stats_data.append([f"{sentiment}: {count}件"])
        
        stats_data.append([""])
        stats_data.append(["=== コメントデータ ==="])
        
        # データを準備
        headers = ["guest_id", "username", "original_text", "inserted_at", "チャットの属性", "チャット感情"]
        data = [headers]
        
        for _, row in df.iterrows():
            data.append([
                str(row.get("guest_id", "")),
                str(row.get("username", "")),
                str(row.get("original_text", "")),
                str(row.get("inserted_at", "")),
                str(row.get("チャットの属性", "")),
                str(row.get("チャット感情", ""))
            ])
        
        # 全てのデータを結合
        all_data = stats_data + data
        
        # データを書き込み
        write_data_to_sheet(spreadsheet_id, sheet_name, all_data, "A1", progress_callback)
        
        if progress_callback:
            progress_callback("書式設定を適用中...", 0.5)
        
        # ヘッダー行を確認（統計情報の行数 + 1）
        header_row_index = len(stats_data)
        data_start_row = header_row_index
        data_end_row = header_row_index + len(df) + 1  # +1はヘッダー行
        
        # username列にドロップダウンを適用（色分けなし）
        username_col = headers.index("username")
        apply_data_validation(
            spreadsheet_id,
            sheet_id,
            username_col,
            df["username"].unique().tolist(),
            data_start_row,
            data_end_row
        )
        
        if progress_callback:
            progress_callback("ドロップダウンを設定中...", 0.6)
        
        # チャットの属性列にドロップダウンと色分けを適用
        attribute_col = headers.index("チャットの属性")
        apply_data_validation(
            spreadsheet_id,
            sheet_id,
            attribute_col,
            CHAT_ATTRIBUTES,
            data_start_row,
            data_end_row
        )
        apply_color_formatting(
            spreadsheet_id,
            sheet_id,
            attribute_col,
            {attr: COLOR_MAP.get(attr, {}) for attr in CHAT_ATTRIBUTES},
            data,
            header_row_index,
            progress_callback
        )
        
        if progress_callback:
            progress_callback("チャット感情の書式設定中...", 0.8)
        
        # チャット感情列にドロップダウンと色分けを適用
        sentiment_col = headers.index("チャット感情")
        apply_data_validation(
            spreadsheet_id,
            sheet_id,
            sentiment_col,
            CHAT_SENTIMENTS,
            data_start_row,
            data_end_row
        )
        apply_color_formatting(
            spreadsheet_id,
            sheet_id,
            sentiment_col,
            {sent: COLOR_MAP.get(sent, {}) for sent in CHAT_SENTIMENTS},
            data,
            header_row_index,
            progress_callback
        )
        
        if progress_callback:
            progress_callback("メインシートの作成が完了しました", 1.0)
        
    except Exception as e:
        error_msg = f"メインシート作成エラー: {str(e)}"
        print(error_msg)
        raise Exception(error_msg) from e


def create_question_sheet(
    spreadsheet_id: str,
    df: pd.DataFrame,
    statistics: Dict,
    progress_callback: Optional[Callable] = None
):
    """
    質問シートを作成
    
    Args:
        spreadsheet_id: スプレッドシートID
        df: 質問コメントのデータフレーム
        statistics: 統計情報
        progress_callback: 進捗コールバック関数
    """
    try:
        if progress_callback:
            progress_callback("質問シートを作成中...", 0.0)
        
        # 新しいシートを作成
        sheet_name = "質問シート"
        sheet_id = create_sheet(spreadsheet_id, sheet_name)
        
        if progress_callback:
            progress_callback("データを準備中...", 0.2)
        
        # 統計情報を作成
        stats_data = [
            ["=== 統計情報 ==="],
            [f"質問コメント件数: {statistics['total_questions']}"],
            [f"質問回答率: {statistics['answer_rate']:.1f}%"],
            [""],
            ["=== 質問コメントデータ ==="]
        ]
        
        # データを準備
        headers = ["guest_id", "username", "original_text", "inserted_at", "チャットの属性", "チャット感情", "回答状況"]
        data = [headers]
        
        for _, row in df.iterrows():
            data.append([
                str(row.get("guest_id", "")),
                str(row.get("username", "")),
                str(row.get("original_text", "")),
                str(row.get("inserted_at", "")),
                str(row.get("チャットの属性", "")),
                str(row.get("チャット感情", "")),
                str(row.get("回答状況", "未回答"))
            ])
        
        # 全てのデータを結合
        all_data = stats_data + data
        
        # データを書き込み
        write_data_to_sheet(spreadsheet_id, sheet_name, all_data, "A1", progress_callback)
        
        if progress_callback:
            progress_callback("書式設定を適用中...", 0.7)
        
        # ヘッダー行を確認
        header_row_index = len(stats_data)
        data_start_row = header_row_index
        data_end_row = header_row_index + len(df) + 1
        
        # 回答状況列にドロップダウンと色分けを適用
        answer_status_col = headers.index("回答状況")
        apply_data_validation(
            spreadsheet_id,
            sheet_id,
            answer_status_col,
            ANSWER_STATUSES,
            data_start_row,
            data_end_row
        )
        apply_color_formatting(
            spreadsheet_id,
            sheet_id,
            answer_status_col,
            {status: COLOR_MAP.get(status, {}) for status in ANSWER_STATUSES},
            data,
            header_row_index,
            progress_callback
        )
        
        if progress_callback:
            progress_callback("質問シートの作成が完了しました", 1.0)
        
    except Exception as e:
        error_msg = f"質問シート作成エラー: {str(e)}"
        print(error_msg)
        raise Exception(error_msg) from e


def calculate_statistics(df: pd.DataFrame) -> Dict:
    """
    統計情報を計算
    
    Args:
        df: データフレーム
        
    Returns:
        統計情報の辞書
    """
    stats = {
        "total_comments": len(df),
        "attribute_counts": {},
        "sentiment_counts": {}
    }
    
    if "チャットの属性" in df.columns:
        stats["attribute_counts"] = df["チャットの属性"].value_counts().to_dict()
    
    if "チャット感情" in df.columns:
        stats["sentiment_counts"] = df["チャット感情"].value_counts().to_dict()
    
    return stats


def calculate_question_statistics(question_df: pd.DataFrame) -> Dict:
    """
    質問コメントの統計情報を計算
    
    Args:
        question_df: 質問コメントのデータフレーム
        
    Returns:
        統計情報の辞書
    """
    total_questions = len(question_df)
    
    if total_questions == 0:
        return {
            "total_questions": 0,
            "answer_rate": 0.0
        }
    
    # 回答状況が「出演者」または「運営」の場合、回答済みとカウント
    answered = question_df["回答状況"].isin(["出演者", "運営"]).sum()
    answer_rate = (answered / total_questions) * 100 if total_questions > 0 else 0.0
    
    return {
        "total_questions": total_questions,
        "answer_rate": answer_rate
    }

