
### .parquet + PARQUET_SCAN()

import os
import json
import pandas as pd
import duckdb
from datetime import datetime

input_folder = "raw_data"
output_folder = "parquet_data"
os.makedirs(output_folder, exist_ok=True)

time_start = datetime.now()

for filename in os.listdir(input_folder):
    if filename.endswith(".json"):
        file_path = os.path.join(input_folder, filename)
        
        with open(file_path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        header = raw['data'][0]['header']
        values = []
        for location_ in raw['data']:
            for row in location_['value']:
                values.append(row)
        df = pd.DataFrame(data=values, columns=header)

        # Xuất ra file Parquet với tên tương ứng
        parquet_path = os.path.join(output_folder, filename.replace(".json", ".parquet"))
        df.to_parquet(parquet_path, index=False)
        print(f"✅ Đã chuyển {filename} → {parquet_path}")


con = duckdb.connect()
query = """
SELECT year, AVG(t2m_max) AS avg_max_temp, SUM(precipitation) AS total_precip
FROM PARQUET_SCAN('parquet_data/*.parquet')
GROUP BY year
ORDER BY year;
"""

result = con.sql(query).df()
print(result.head())

con.close()
time_end = datetime.now()

print(f'Mất: {(time_end - time_start).seconds + ((time_end - time_start).microseconds) / 1000000}s')