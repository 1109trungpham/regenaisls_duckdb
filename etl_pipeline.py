
import os
import json
import pandas as pd
import duckdb
from datetime import datetime
import shutil
import matplotlib
matplotlib.use("Agg") 
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
import great_expectations as gx
import logging
from io import BytesIO
import psutil
import gc

def print_ram_usage(message):
    process = psutil.Process(os.getpid())
    mem = process.memory_info().rss / 1024 / 1024  # MB
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] [{message}] RAM sử dụng: {mem:.2f} MB")

def convert_json_to_parquet(new_json_folder: str, output_parquet_folder:str, batch_definition, context):
    print_ram_usage("🚦 Trước khi bắt đầu")

    json_files = [f for f in os.listdir(new_json_folder) if f.endswith('.json')]
    if not json_files:
        print(f"\nKhông tìm thấy file JSON mới nào trong thư mục: {new_json_folder}.\n")
        return False
    print(f"\nTìm thấy {len(json_files)} file JSON mới trong thư mục: {new_json_folder}.\n")
    print('Bắt đầu quá trình chuyển đổi...')

    col_name = ["longitude", "latitude", "day", "month", "year", "day_of_year", "t2m_max", "t2m_min", "precipitation"]
    for filename in os.listdir(new_json_folder):
        if filename.endswith(".json"):
            file_path = os.path.join(new_json_folder, filename)
            
            with open(file_path, "r", encoding="utf-8") as f:
                try:
                    raw = json.load(f)
                except json.JSONDecodeError:
                    print(f"Lỗi đọc file JSON: {file_path}")
                    shutil.move(file_path, 'error_data')
                    continue

            values = []
            for loc in raw['data']:
                lon_lat = loc['location']
                for row_data in loc['value']:
                    combined_row = [*lon_lat, *row_data]
                    values.append(combined_row)

            df = pd.DataFrame(data=values, columns=col_name)

            if valid(df, batch_definition, context) is None:
                print(f"Chất lượng dữ liệu của {file_path} không đạt yêu cầu.")
                try:
                    shutil.move(file_path, 'error_data')
                    print('Đã chuyển file không đạt chất lượng vào thư mục error_data.')
                except Exception as e:
                    print(e)
                finally:
                    continue

            parquet_path = os.path.join(output_parquet_folder, filename.replace(".json", ".parquet"))
            df.to_parquet(parquet_path, index=False)
            print(f"✅ Đã chuyển {filename} → {parquet_path}")
            print_ram_usage(f"📄 Sau khi xử lý {filename}")

            # Giải phóng bộ nhớ
            del df, values, raw
            gc.collect()
            print_ram_usage(f"🧹 Sau khi giải phóng bộ nhớ của {filename}")
            
    print_ram_usage("🏁 Sau khi hoàn tất toàn bộ pipeline")
    return True

def valid(df, batch_definition, context):

    suite = gx.ExpectationSuite(name= "data_suite")

    suite.add_expectation(gx.expectations.ExpectTableColumnCountToEqual(value=9))
    col_name = ['longitude','latitude','day','month','year','day_of_year','t2m_max','t2m_min','precipitation']
    suite.add_expectation(gx.expectations.ExpectTableColumnsToMatchSet(column_set=col_name))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(column="year", type_="int"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="longitude", min_value=-180.0, max_value=180.0))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="latitude", min_value=-90.0, max_value=90.0))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="day", min_value=1, max_value=31))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="month", min_value=1, max_value=12))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="year", min_value=1945, max_value=2025))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="day_of_year", min_value=1, max_value=366))

    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    validation_results = batch.validate(suite)
    logging.info(f"DEBUG - result: {validation_results}")

    if not validation_results["success"]:
        logging.error("Dữ liệu không đạt yêu cầu.")
        return None
    logging.info("Dữ liệu hợp lệ sau kiểm tra")
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
                precipitation = EXCLUDED.precipitation;
        """)
    print(f"✅ Đã UPSERT dữ liệu mới vào bảng '{table_name}'.\n")

    # print(f"\n10 hàng đầu tiên từ bảng '{table_name}':")
    # print(con.execute(f"SELECT * FROM {table_name} LIMIT 10;").df())
    # print("\nKích thước của tệp DuckDB:")
    # print(con.execute("PRAGMA database_size;").df())
    return None

def duckdb_query(duckdb_fileabase, query):
    con = duckdb.connect(f'{duckdb_fileabase}')
    result = con.sql(query).df()
    con.close()
    return result

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
    timestamp = datetime.now().strftime("%Y_%m_%d_%H:%M:%S")
    png_path = os.path.join(output_dir, f"weather_summary_{timestamp}.png")

    fig.savefig(png_path)
    print(f"✅ Biểu đồ đã được lưu tại: {png_path}")
    # plt.show()
    
    buf = BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)
    return buf

def move_all_files(src_folder, dest_folder):
    if not os.path.isdir(src_folder):
        print(f"Thư mục nguồn '{src_folder}' không hợp lệ.")
        return
    if not os.path.isdir(dest_folder):
        os.makedirs(dest_folder)
        print(f"Thư mục đích '{dest_folder}' đã được tạo.")
    i = 0
    for filename in os.listdir(src_folder):
        src_path = os.path.join(src_folder, filename)
        dest_path = os.path.join(dest_folder, filename)

        if os.path.isfile(src_path):
            try:
                shutil.move(src_path, dest_path)
                i += 1
            except Exception as e:
                print(f"Lỗi khi chuyển {src_path}: {e}")
    print(f"✅ Đã di chuyển {i} file JSON từ thư mục {src_folder} sang thư mục {dest_folder}.")

def delete_all_files(directory_path):
    if not os.path.isdir(directory_path):
        print(f"'{directory_path}' không phải là một thư mục hợp lệ.")
        return
    i = 0
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        if os.path.isfile(file_path):
            try:
                os.remove(file_path)
                i += 1
            except Exception as e:
                print(f"Lỗi khi xoá {file_path}: {e}")
    print(f"✅ Đã xoá {i} file Parquet trong thư mục {directory_path}.")


def main():
    timestamp = datetime.now().strftime("%Y_%m_%d_%H:%M:%S")
    logging.basicConfig(filename=f"logs/etl_{timestamp}.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logging.info("ETL Pipeline bắt đầu")

    os.makedirs("new_data", exist_ok=True)
    os.makedirs("parquet_data", exist_ok=True)
    os.makedirs("raw_data", exist_ok=True)
    os.makedirs("error_data", exist_ok=True)
    os.makedirs("database", exist_ok=True)
    os.makedirs("charts", exist_ok=True)

    new_json_folder       = "new_data"
    output_parquet_folder = "parquet_data"
    raw_json_folder       = "raw_data"
    output_duckdb_folder  = "database"

    database_name = "weather_data_database"
    table_name    = "weather_data_table"
    output_duckdb_file = os.path.join(output_duckdb_folder, f"{database_name}.duckdb")
    
    time_start = datetime.now()

    # Khởi tạo context
    context = gx.get_context()
    pandas_datasource = context.data_sources.add_pandas(name="pandas_source")
    data_asset = pandas_datasource.add_dataframe_asset(name="pandas_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name="pandas_batch")

    # Xử lý từng JSON mới thành Parquet
    status = convert_json_to_parquet(new_json_folder, output_parquet_folder, batch_definition, context)

    # Tải dữ liệu từ Parquet vào DuckDB
    if status:
        print(f"\nBắt đầu quá trình UPSERT dữ liệu mới vào bảng '{table_name}'...")
        load_parquet_to_duckdb(output_parquet_folder, output_duckdb_file, table_name)

    try:
        # Truy vấn tổng hợp 
        query = f"""
        SELECT year, AVG(t2m_max) AS avg_max_temp, SUM(precipitation) AS total_precip
        FROM {table_name}
        GROUP BY year
        ORDER BY year;
        """
        result = duckdb_query(output_duckdb_file, query)
        time_end = datetime.now()
        print("Kết quả tổng hợp:")
        print(result)
        print(f'\nTổng thời gian: {(time_end - time_start).seconds + ((time_end - time_start).microseconds) / 1000000}s\n')

        # Trực quan hoá
        visualize_summary(result)
    except Exception as e:
        print(e)

    if status:
        print("\nDọn dẹp dữ liệu...")
        move_all_files(new_json_folder, raw_json_folder)
        delete_all_files(output_parquet_folder)
        
    print("\nKết thúc chương trình.\n")
    return None

if __name__=="__main__":
    main()

