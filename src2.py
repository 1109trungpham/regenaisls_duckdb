import os
import json
import shutil
import logging
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from io import BytesIO
import pandas as pd
import duckdb
import psutil
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

matplotlib.use("Agg")

# ----- CẤU HÌNH -----
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "new_data"
RAW_DIR = BASE_DIR / "raw_data"
ERROR_DIR = BASE_DIR / "error_data"
PARQUET_DIR = BASE_DIR / "parquet_data"
CHART_DIR = BASE_DIR / "charts"
DB_PATH = BASE_DIR / "database" / "weather_data.duckdb"
TABLE_NAME = "weather_data_table"

# ----- LOGGING -----
timestamp = datetime.now().strftime("%Y_%m_%d_%H:%M:%S")
logging.basicConfig(
    filename=BASE_DIR / f"logs/etl_{timestamp}.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ----- RAM TRACKER -----
def print_ram_usage(message):
    mem = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] [{message}] RAM sử dụng: {mem:.2f} MB")

# ----- VALIDATE + CONVERT -----
def validate_and_convert(file_path: Path) -> Path | None:
    import great_expectations as gx  # tránh bị pickle
    import pandas as pd
    import shutil
    import gc

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except json.JSONDecodeError:
        shutil.move(file_path, ERROR_DIR)
        return None

    values = []
    for loc in raw["data"]:
        lon_lat = loc.get("location")
        for row in loc.get("value", []):
            values.append([*lon_lat, *row])

    columns = ["longitude", "latitude", "day", "month", "year",
               "day_of_year", "t2m_max", "t2m_min", "precipitation"]
    df = pd.DataFrame(values, columns=columns)

    # Khởi tạo GE context & suite trong subprocess
    context = gx.get_context()
    datasource = context.data_sources.add_pandas("pandas_source")
    asset = datasource.add_dataframe_asset("asset")
    batch_def = asset.add_batch_definition_whole_dataframe("batch")

    suite = gx.ExpectationSuite(name="data_suite")
    suite.add_expectation(gx.expectations.ExpectTableColumnCountToEqual(value=9))
    suite.add_expectation(gx.expectations.ExpectTableColumnsToMatchSet(column_set=columns))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="longitude", min_value=-180, max_value=180))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="latitude", min_value=-90, max_value=90))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="day", min_value=1, max_value=31))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="month", min_value=1, max_value=12))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="year", min_value=1900, max_value=2100))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="day_of_year", min_value=1, max_value=366))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(column="day_of_year", type_="int"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(column="t2m_max", type_="float"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(column="t2m_min", type_="float"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(column="precipitation", type_="float"))


    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    result = batch.validate(suite)

    if not result["success"]:
        shutil.move(file_path, ERROR_DIR)
        return None

    out_path = PARQUET_DIR / file_path.with_suffix(".parquet").name
    df.to_parquet(out_path, index=False)

    # Giải phóng bộ nhớ
    del df, values, raw
    gc.collect()
    return out_path


# ----- UPSERT VÀO DUCKDB -----
def load_to_duckdb(parquet_files: list[Path]):
    if not parquet_files:
        logging.warning("⚠️ Không có file Parquet hợp lệ.")
        return

    con = duckdb.connect(DB_PATH)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
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

    file_strs = [str(f) for f in parquet_files]
    con.execute(f"""
        INSERT INTO {TABLE_NAME}
        SELECT * FROM read_parquet({file_strs})
        ON CONFLICT (day, month, year, longitude, latitude) DO UPDATE SET
            t2m_max = EXCLUDED.t2m_max,
            t2m_min = EXCLUDED.t2m_min,
            precipitation = EXCLUDED.precipitation;
    """)
    con.close()
    print(f"✅ Đã UPSERT dữ liệu mới vào bảng '{TABLE_NAME}'.")

# ----- KẾT NỐI DUCKDB -----
def duckdb_query(duckdb_fileabase, query):
    con = duckdb.connect(f'{duckdb_fileabase}')
    result = con.sql(query).df()
    con.close()
    return result

# ----- VẼ BIỂU ĐỒ -----
def visualize_summary(df: pd.DataFrame):
    if df.empty:
        print("Không có dữ liệu để vẽ.")
        return

    fig, ax1 = plt.subplots(figsize=(10, 6))
    sns.set(style="whitegrid")
    color1 = "tab:red"
    ax1.set_xlabel("Năm")
    ax1.set_ylabel("Nhiệt độ tối đa trung bình (°C)", color=color1)
    sns.lineplot(data=df, x="year", y="avg_max_temp", marker='o', color=color1, ax=ax1)
    ax1.tick_params(axis="y", labelcolor=color1)
    ax1.xaxis.set_major_locator(ticker.MultipleLocator(1))
    ax1.set_xticks(df["year"])

    ax2 = ax1.twinx()
    color2 = "tab:blue"
    ax2.set_ylabel("Tổng lượng mưa (mm)", color=color2)
    sns.lineplot(data=df, x="year", y="total_precip", marker='o', color=color2, ax=ax2)
    ax2.tick_params(axis="y", labelcolor=color2)

    plt.title("Biến động nhiệt độ tối đa và lượng mưa theo năm")
    plt.tight_layout()

    out_file = CHART_DIR / f"weather_summary_{datetime.now().strftime('%Y_%m_%d_%H:%M:%S')}.png"
    plt.savefig(out_file)
    print(f"✅ Biểu đồ đã được lưu tại: {out_file}")
    buf = BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)
    return buf

# ----- MAIN PIPELINE -----
def run_pipeline():
    os.makedirs(PARQUET_DIR, exist_ok=True)
    json_files = list(DATA_DIR.glob("*.json"))

    if not json_files:
        print("📂 Không có file JSON mới.")
        return
    
    time_start = datetime.now()
    print(f"🚀 Bắt đầu xử lý {len(json_files)} file JSON...")
    print_ram_usage("🚦 Trước khi bắt đầu")


    processed_parquets = []
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as pool:
        futures = [pool.submit(validate_and_convert, f) for f in json_files]
        for future in as_completed(futures):
            result = future.result()
            if result:
                processed_parquets.append(result)

    print_ram_usage("🏁 Sau khi hoàn tất chuyển đổi")
    load_to_duckdb(processed_parquets)

    try:
        # Truy vấn tổng hợp 
        query = f"""
        SELECT year, AVG(t2m_max) AS avg_max_temp, SUM(precipitation) AS total_precip
        FROM {TABLE_NAME}
        GROUP BY year
        ORDER BY year;
        """
        result = duckdb_query(DB_PATH, query)
        time_end = datetime.now()
        print("Kết quả tổng hợp:")
        print(result)
        print(f'\nTổng thời gian: {(time_end - time_start).seconds + ((time_end - time_start).microseconds) / 1000000}s\n')

        # Trực quan hoá
        visualize_summary(result)
    except Exception as e:
        print(e)

    # Cleanup
    for f in json_files:
        shutil.move(f, RAW_DIR)
    for f in processed_parquets:
        os.remove(f)

    print("✅ Kết thúc pipeline.")

# if __name__ == "__main__":
#     start = datetime.now()
#     run_pipeline()
#     print(datetime.now() - start)
