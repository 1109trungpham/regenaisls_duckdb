
### JSONs -> .duckdb

import os
import json
import pandas as pd
import duckdb
from datetime import datetime

def merge_json_into_duckdb(input_json_folder: str, output_duckdb_file: str, duckdb_table_name: str):
    df_list = []
    for filename in os.listdir(input_json_folder):
        if filename.endswith(".json"):
            file_path = os.path.join(input_json_folder, filename)
            
            with open(file_path, "r", encoding="utf-8") as f:
                raw = json.load(f)

            header = raw['data'][0]['header']
            values = []
            for location_ in raw['data']:
                for row in location_['value']:
                    values.append(row)
            df = pd.DataFrame(data=values, columns=header)
            df_list.append(df)
            print(f"✅ Đã đọc xong: {filename}")

    merged_df = pd.concat(df_list, ignore_index=True)
    print(f"📦 Đã gộp {len(df_list)} file, tổng số dòng: {len(merged_df)}")

    con = duckdb.connect(output_duckdb_file)
    con.execute(f"CREATE TABLE {duckdb_table_name} AS SELECT * FROM merged_df")
    print(f"✅ Đã lưu dữ liệu vào {output_duckdb_file} trong bảng '{duckdb_table_name}'")
    con.close()
    return None


def main():
    input_json_folder = "raw_data"
    output_duckdb_file = "database/merged_data.duckdb"
    duckdb_table_name = "weather"

    time_start = datetime.now()
    merge_json_into_duckdb(input_json_folder, output_duckdb_file, duckdb_table_name)

    query = """
    SELECT year, AVG(t2m_max) AS avg_max_temp, SUM(precipitation) AS total_precip
    FROM weather
    GROUP BY year
    ORDER BY year;
    """
    con = duckdb.connect(output_duckdb_file)
    result = con.sql(query).df()
    con.close()
    time_end = datetime.now()

    print(f'Mất: {(time_end - time_start).seconds + ((time_end - time_start).microseconds) / 1000000}s')
    print(result)
    return None

if __name__=="__main__":
    main()