
### .parquet + PARQUET_SCAN()

import os
import json
import pandas as pd
import duckdb
from datetime import datetime

def convert_json_to_parquet(input_json_folder: str, output_parquet_folder:str):
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

            parquet_path = os.path.join(output_parquet_folder, filename.replace(".json", ".parquet"))
            df.to_parquet(parquet_path, index=False)
            print(f"✅ Đã chuyển {filename} → {parquet_path}")
    return None

def main():
    input_json_folder = "raw_data"
    output_parquet_folder = "parquet_data"
    os.makedirs(output_parquet_folder, exist_ok=True)

    time_start = datetime.now()
    convert_json_to_parquet(input_json_folder, output_parquet_folder)

    query = f"""
    SELECT year, AVG(t2m_max) AS avg_max_temp, SUM(precipitation) AS total_precip
    FROM PARQUET_SCAN('{output_parquet_folder}/*.parquet')
    GROUP BY year
    ORDER BY year;
    """
    con = duckdb.connect()
    result = con.sql(query).df()
    con.close()
    time_end = datetime.now()

    print(f'Mất: {(time_end - time_start).seconds + ((time_end - time_start).microseconds) / 1000000}s')
    print(result)
    return None

if __name__=="__main__":
    main()

