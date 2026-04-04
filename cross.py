import requests
import re
import json
import time
import os
import pandas as pd
from datetime import datetime

# --- 設定 ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(BASE_DIR, "yutai_database_ALL.csv")
JSON_FILE = os.path.join(BASE_DIR, "stock_full_data.json")

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
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200: return None
        
        blocks = re.split(r"【([^】]+?)】過去90日間", response.text)
        if len(blocks) < 3: return None

        daily_data = {}
        for i in range(1, len(blocks), 2):
            broker_name = blocks[i]
            content = blocks[i+1]
            if len(broker_name) > 10: continue
            
            hz_match = re.search(r"var hz=\[([^\]]*?)\]", content)
            tz_match = re.search(r"var tz=\[([^\]]*?)\]", content)
            if not hz_match or not tz_match: continue
            
            hz_str = hz_match.group(1).replace("null", "0")
            tz_str = tz_match.group(1)

            try:
                stocks = json.loads("[" + hz_str + "]")
                dates = json.loads("[" + tz_str + "]")
            except: continue
            
            if not dates: continue
            last_dt = parse_gokigen_date(dates[-1])
            if not last_dt: continue
            
            best_kenri_date, min_diff = None, 9999
            for (m, d) in kenri_md_list:
                for y in [last_dt.year, last_dt.year - 1, last_dt.year + 1]:
                    try:
                        candidate = datetime(y, m, d)
                        diff = abs((candidate - last_dt).days)
                        if diff < min_diff:
                            min_diff, best_kenri_date = diff, candidate
                    except: pass
            
            if not best_kenri_date: continue

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

                if date_str not in daily_data:
                    daily_data[date_str] = {"銘柄コード": int(code), "権利年": best_kenri_date.year, 
                                           "権利日までの日数": days_left, "カレンダー日付": date_str}
                daily_data[date_str][broker_name] = num_val
                
        return list(daily_data.values())
    except:
        return None

if __name__ == "__main__":
    # 自動実行時は全月を対象にする
    target_months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    
    if not os.path.exists(JSON_FILE):
        print(f"❌ {JSON_FILE} が見つかりません。")
        exit(1)

    with open(JSON_FILE, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

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
    print(f"🎯 ターゲット銘柄数: {len(target_codes)}")

    # 既存データの読み込み
    if os.path.exists(CSV_FILE):
        old_df = pd.read_csv(CSV_FILE)
    else:
        old_df = pd.DataFrame(columns=BASE_COLS + BROKERS)

    new_records = []
    for i, code in enumerate(target_codes, 1):
        # ログ出力（GitHub Actionsのコンソールで見れる）
        if i % 50 == 0:
            print(f"Progress: {i}/{len(target_codes)}")
            
        res = get_timeseries_data(code, kenri_map[code])
        if res: new_records.extend(res)
        time.sleep(1.2) # 負荷軽減

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
                
        combined_df[BASE_COLS + BROKERS].to_csv(CSV_FILE, index=False, encoding="utf-8-sig")
        print(f"✅ 更新完了: {len(new_records)} 件のデータを処理しました。")
    else:
        print("⚠️ 新規データなし。")