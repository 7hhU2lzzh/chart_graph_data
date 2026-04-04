import requests
from requests.auth import HTTPBasicAuth
import re
import json
import time
import os
import pandas as pd
import io
from datetime import datetime

# --- 設定 ---
BASIC_USER = os.environ.get("BASIC_USER", "admin")
BASIC_PASS = os.environ.get("BASIC_PASS", "password")

JSON_URL = "https://www.seiheki.com/stock_full_data.json"
CSV_URL = "https://www.seiheki.com/yutai_database_ALL.csv"
SAVE_CSV_FILE = "yutai_database_ALL.csv"

BROKERS = ["日興", "カブ", "楽天", "SBI", "GMO", "松井", "マネ"]
BASE_COLS = ["銘柄コード", "権利年", "権利日までの日数", "カレンダー日付"]

def parse_gokigen_date(date_str):
    try:
        clean_str = date_str.replace("年", "-").replace("月", "-").replace("日", "")
        parts = clean_str.split("-")
        year = int(parts[0]) + 2000 if int(parts[0]) < 100 else int(parts[0])
        month = int(parts[1])
        day = int(parts[2])
        return datetime(year, month, day)
    except:
        return None

def get_timeseries_data(code, kenri_md_list):
    url = f"https://gokigen-life.tokyo/{code}yutai/"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200: return None
        html = response.text

        daily_data = {}

        # ─── ① 今回データ：【証券名】過去90日間 ブロックから取得 ───
        blocks = re.split(r"【([^】]+?)】過去90日間", html)
        if len(blocks) >= 3:
            for i in range(1, len(blocks), 2):
                broker_name = blocks[i]
                content = blocks[i+1]
                if len(broker_name) > 10: continue
                _parse_hz_tz(content, broker_name, code, kenri_md_list, daily_data)

        # ─── ② 前回権利日データ：*select2() 関数内から取得 ───
        select2_map = {
            'nselect2': '日興', 'kselect2': 'カブ', 'rselect2': '楽天',
            'sselect2': 'SBI',  'gselect2': 'GMO',  'mselect2': '松井',
            'xselect2': 'マネ'
        }
        for func_name, broker_name in select2_map.items():
            # 関数本体を抽出（90日間データ=1つ目のhz/tzを使う）
            match = re.search(rf"function {func_name}\(\)\{{(.*?)\}}\s*function", html, re.DOTALL)
            if not match:
                match = re.search(rf"function {func_name}\(\)\{{(.*?)\}}", html, re.DOTALL)
            if not match: continue
            _parse_hz_tz(match.group(1), broker_name, code, kenri_md_list, daily_data)

        return list(daily_data.values()) if daily_data else None
    except:
        return None


def _parse_hz_tz(content, broker_name, code, kenri_md_list, daily_data):
    """contentからvar hz/tzを抽出し、daily_dataに追記する共通処理"""
    hz_match = re.search(r"var hz=\[([^\]]*?)\]", content)
    tz_match = re.search(r"var tz=\[([^\]]*?)\]", content)
    if not hz_match or not tz_match: return

    hz_str = hz_match.group(1).replace("null", "0")
    tz_str = tz_match.group(1)

    try:
        stocks = json.loads("[" + hz_str + "]")
        dates = json.loads("[" + tz_str + "]")
    except: return

    if not dates: return
    last_dt = parse_gokigen_date(dates[-1])
    if not last_dt: return

    best_kenri_date, min_diff = None, 9999
    for (m, d) in kenri_md_list:
        for y in [last_dt.year, last_dt.year - 1, last_dt.year + 1]:
            try:
                candidate = datetime(y, m, d)
                diff = abs((candidate - last_dt).days)
                if diff < min_diff:
                    min_diff, best_kenri_date = diff, candidate
            except: pass

    if not best_kenri_date: return

    for j in range(len(dates)):
        date_str = dates[j]
        curr_dt = parse_gokigen_date(date_str)
        if not curr_dt: continue
        days_left = (best_kenri_date - curr_dt).days
        if days_left < 0: continue

        try:
            num_val = int(stocks[j])
        except:
            num_val = 0

        # キーを「権利年_日付」にして今年・去年のデータが混在しても分離できるようにする
        data_key = f"{best_kenri_date.year}_{date_str}"
        if data_key not in daily_data:
            daily_data[data_key] = {"銘柄コード": int(code), "権利年": best_kenri_date.year,
                                    "権利日までの日数": days_left, "カレンダー日付": date_str}
        daily_data[data_key][broker_name] = num_val

if __name__ == "__main__":
    now_month = datetime.now().month
    target_months = [now_month, (now_month % 12) + 1, ((now_month + 1) % 12) + 1]
    
    print(f"📅 今回のターゲット: {target_months}月", flush=True)
    print(f"🌐 サーバーからJSONデータを取得中: {JSON_URL}", flush=True)
    
    try:
        res_json = requests.get(JSON_URL, auth=HTTPBasicAuth(BASIC_USER, BASIC_PASS), timeout=10)
        res_json.raise_for_status()
        raw_data = res_json.json()
    except Exception as e:
        print(f"❌ JSONの取得エラー: {e}", flush=True)
        exit(1)

    kenri_map = {}
    for item in raw_data.get("data", []):
        code = str(item.get("code"))
        d_kenri = item.get("d_kenri", "")
        md_list = []
        matches = re.findall(r'(\d+)月(\d+)日', str(d_kenri))
        is_target = False
        for m in matches:
            month = int(m[0])
            md_list.append((month, int(m[1])))
            if month in target_months: is_target = True
        if is_target:
            kenri_map[code] = md_list

    target_codes = list(kenri_map.keys())
    print(f"🎯 ターゲット銘柄数: {len(target_codes)}", flush=True)

    print(f"🌐 サーバーから最新のCSVを取得中: {CSV_URL}", flush=True)
    old_df = pd.DataFrame(columns=BASE_COLS + BROKERS)
    try:
        res_csv = requests.get(CSV_URL, auth=HTTPBasicAuth(BASIC_USER, BASIC_PASS), timeout=15)
        if res_csv.status_code == 200:
            csv_data = res_csv.content.decode('utf-8-sig')
            old_df = pd.read_csv(io.StringIO(csv_data))
            print(f"📦 既存データ {len(old_df)} 件を読み込みました。", flush=True)
        else:
            print("⚠️ サーバーにCSVが見つからないため、新規作成として開始します。", flush=True)
    except Exception as e:
        print(f"⚠️ CSVの取得エラー: {e} (新規作成として開始します)", flush=True)

    new_records = []
    for i, code in enumerate(target_codes, 1):
        if i % 50 == 0:
            print(f"Progress: {i}/{len(target_codes)}", flush=True)
        res = get_timeseries_data(code, kenri_map[code])
        if res: new_records.extend(res)
        time.sleep(1.2)

    if new_records:
        new_df = pd.DataFrame(new_records)
        combined_df = pd.concat([old_df, new_df], ignore_index=True)
        combined_df["銘柄コード"] = combined_df["銘柄コード"].astype(int)
        combined_df["権利年"] = combined_df["権利年"].astype(int)
        
        combined_df = combined_df.drop_duplicates(subset=["銘柄コード", "権利年", "カレンダー日付"], keep="last")
        combined_df = combined_df.sort_values(["銘柄コード", "権利日までの日数"], ascending=[True, False])
        
        for b in BROKERS:
            if b not in combined_df.columns:
                combined_df[b] = None
                
        combined_df[BASE_COLS + BROKERS].to_csv(SAVE_CSV_FILE, index=False, encoding="utf-8-sig")
        print(f"✅ 更新完了: {SAVE_CSV_FILE} に保存しました。", flush=True)
    else:
        print("⚠️ 新規データなし。", flush=True)
        old_df.to_csv(SAVE_CSV_FILE, index=False, encoding="utf-8-sig")

    # ==========================================
    # 💡 ここからが「別のリポと同じ」安全なFTP処理です
    # ==========================================
    FTP_HOST = os.environ.get("FTP_HOST")
    FTP_USER = os.environ.get("FTP_USER")
    FTP_PASS = os.environ.get("FTP_PASS")

    if FTP_HOST and FTP_USER and FTP_PASS:
        print(f"🚀 サーバー({FTP_HOST})へFTPアップロードを開始します...", flush=True)
        import ftplib
        try:
            ftp = ftplib.FTP()
            ftp.connect(FTP_HOST, 21, timeout=15)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.set_pasv(True) # エラーを防ぐパッシブモード
            
            # /www/ ディレクトリへの移動（なければ直下にアップロード）
            try:
                ftp.cwd("/www")
            except:
                pass
            
            with open(SAVE_CSV_FILE, 'rb') as f:
                ftp.storbinary(f'STOR {SAVE_CSV_FILE}', f)
                
            ftp.quit()
            print("✅ FTPアップロードに成功しました！", flush=True)
        except Exception as e:
            print(f"❌ FTPアップロード失敗: {e}", flush=True)
    else:
        print("⚠️ FTP情報が設定されていないため、転送をスキップしました。", flush=True)
