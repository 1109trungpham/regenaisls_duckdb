
# B1: Tạo nguồn dữ liệu, chạy: python source/json_creator.py
# B2: Chạy pipeline: python source/pipeline2p2.py
# B3: Chạy lại pipeline: python source/pipeline2p2.py

import os
import json
import pandas as pd
import duckdb
from datetime import datetime
import shutil
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
from datetime import datetime

def convert_json_to_parquet(new_json_folder: str, output_parquet_folder:str):

    json_files = [f for f in os.listdir(new_json_folder) if f.endswith('.json')]
    if not json_files:
        print(f"\nKhông tìm thấy file JSON mới nào trong thư mục: {new_json_folder}'.\n")
        return False
    
    print(f"\nTìm thấy {len(json_files)} file JSON mới trong thư mục: {new_json_folder}.\n")
    print('Bắt đầu quá trình chuyển đổi...\n')

    for filename in os.listdir(new_json_folder):
        if filename.endswith(".json"):
            file_path = os.path.join(new_json_folder, filename)
            
            with open(file_path, "r", encoding="utf-8") as f:
                raw = json.load(f)

            col_name = ["lon", "lat", "day", "month", "year", "day_of_year", "t2m_max", "t2m_min", "precipitation"]
            values = []
            
            for loc in raw['data']:
                lon_lat = loc['location']
                for row_data in loc['value']:
                    combined_row = [*lon_lat, *row_data]
                    values.append(combined_row)

            df = pd.DataFrame(data=values, columns=col_name)

            parquet_path = os.path.join(output_parquet_folder, filename.replace(".json", ".parquet"))
            df.to_parquet(parquet_path, index=False)
            print(f"✅ Đã chuyển {filename} → {parquet_path}")
    return True

def load_parquet_to_duckdb(output_parquet_folder:str, output_duckdb_file: str, table_name: str):
    con = duckdb.connect(output_duckdb_file)
    con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                longitude DOUBLE,
                latitude DOUBLE,
                day INTEGER,
                month INTEGER,
                year INTEGER,
                day_of_year INTEGER,
                t2m_max DOUBLE,
                t2m_min DOUBLE,
                precipitation DOUBLE,
                PRIMARY KEY (day, month, year, longitude, latitude)
            );
        """)
    
    con.execute(f"""
            INSERT INTO {table_name}
            SELECT * FROM '{output_parquet_folder}/*.parquet'
            ON CONFLICT (day, month, year, longitude, latitude) DO UPDATE SET
                t2m_max = EXCLUDED.t2m_max,
                t2m_min = EXCLUDED.t2m_min,
                precipitation = EXCLUDED.precipitation
            ;
    """)
    print(f"\nĐã UPSERT dữ liệu mới vào bảng '{table_name}'.\n")

    # print(f"\n10 hàng đầu tiên từ bảng '{table_name}':")
    # print(con.execute(f"SELECT * FROM {table_name} LIMIT 10;").df())

    # print("\nKích thước của tệp DuckDB:")
    # print(con.execute("PRAGMA database_size;").df())
    return None

def visualize_summary(result_df, output_dir="charts"):
    if result_df.empty:
        print("Không có dữ liệu để trực quan hoá.")
        return

    sns.set(style="whitegrid")
    os.makedirs(output_dir, exist_ok=True)

    fig, ax1 = plt.subplots(figsize=(10, 6))

    # Trục Y thứ nhất: avg_max_temp
    color1 = "tab:red"
    ax1.set_xlabel("Năm")
    ax1.set_ylabel("Nhiệt độ tối đa trung bình (°C)", color=color1)
    sns.lineplot(data=result_df, x="year", y="avg_max_temp", marker='o', color=color1, ax=ax1)
    ax1.tick_params(axis="y", labelcolor=color1)

    # Trục Y thứ hai: total_precip
    ax2 = ax1.twinx()
    color2 = "tab:blue"
    ax2.set_ylabel("Tổng lượng mưa (mm)", color=color2)
    sns.lineplot(data=result_df, x="year", y="total_precip", marker='o', color=color2, ax=ax2)
    ax2.tick_params(axis="y", labelcolor=color2)

    ax1.xaxis.set_major_locator(ticker.MultipleLocator(1))  # Hiển thị mỗi năm một mốc
    ax1.set_xticks(result_df["year"]) 

    plt.title("Biến động nhiệt độ tối đa trung bình và lượng mưa theo năm")
    plt.tight_layout()

    # Lưu ra file
    png_path = os.path.join(output_dir, f"weather_summary_{datetime.now()}.png")
    fig.savefig(png_path)
    print(f"✅ Biểu đồ đã được lưu tại: {png_path}\n")
    plt.show()

def move_all_files(src_folder, dest_folder):
    if not os.path.isdir(src_folder):
        print(f"Thư mục nguồn '{src_folder}' không hợp lệ.")
        return
    if not os.path.isdir(dest_folder):
        os.makedirs(dest_folder)
        print(f"Thư mục đích '{dest_folder}' đã được tạo.")

    for filename in os.listdir(src_folder):
        src_path = os.path.join(src_folder, filename)
        dest_path = os.path.join(dest_folder, filename)

        if os.path.isfile(src_path):
            try:
                shutil.move(src_path, dest_path)
                print(f"Đã chuyển: {src_path} → {dest_path}")
            except Exception as e:
                print(f"Lỗi khi chuyển {src_path}: {e}")

def delete_all_files_in_directory(directory_path):
    if not os.path.isdir(directory_path):
        print(f"'{directory_path}' không phải là một thư mục hợp lệ.")
        return

    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        if os.path.isfile(file_path):
            try:
                os.remove(file_path)
                print(f"Đã xoá: {file_path}")
            except Exception as e:
                print(f"Lỗi khi xoá {file_path}: {e}")


def main():
    raw_json_folder = "raw_data"
    new_json_folder = "new_data"    # Thư mục chứa các tệp JSON mới
    output_parquet_folder = "parquet_data"
    output_duckdb_folder = "database"
    database_name = "weather_data_database"
    table_name = "weather_data_table"
    output_duckdb_file = os.path.join(output_duckdb_folder, f"{database_name}.duckdb")

    os.makedirs(output_parquet_folder, exist_ok=True)
    os.makedirs(output_duckdb_folder, exist_ok=True)

    time_start = datetime.now()

    # Xử lý từng JSON mới thành Parquet
    status = convert_json_to_parquet(new_json_folder, output_parquet_folder)

    # Tải dữ liệu từ Parquet vào DuckDB
    if status:
        load_parquet_to_duckdb(output_parquet_folder, output_duckdb_file, table_name)

    # Truy vấn tổng hợp 
    query = f"""
    SELECT year, AVG(t2m_max) AS avg_max_temp, SUM(precipitation) AS total_precip
    FROM {table_name}
    GROUP BY year
    ORDER BY year;
    """
    # query = f"""
    # SELECT *
    # FROM {table_name}
    # """
    con = duckdb.connect(f'{output_duckdb_file}')
    result = con.sql(query).df()
    con.close()
    time_end = datetime.now()
    print("Kết quả tổng hợp:")
    print(result)
    print(f'\nTổng thời gian: {(time_end - time_start).seconds + ((time_end - time_start).microseconds) / 1000000}s\n')

    # Trực quan hoá
    visualize_summary(result)

    # Chuyển tất cả file JSON mới sang thư mục raw_data và 
    # Xoá tất cả file Parquet trong thư mục parquet_data
    if status:
        print()
        move_all_files(new_json_folder, raw_json_folder)
        delete_all_files_in_directory(output_parquet_folder)

    return None

if __name__=="__main__":
    main()

