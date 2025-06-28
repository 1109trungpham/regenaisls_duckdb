import pyarrow.parquet as pq
import pyarrow as pa
import os
import shutil 
import duckdb
from datetime import datetime


def merge_new_parquet(input_directory, output_file_path, schema):
    parquet_files = [f for f in os.listdir(input_directory) if f.endswith('.parquet')]
    if not parquet_files:
        print(f"\nKhông tìm thấy file Parquet nào trong thư mục: {input_directory}'.\n")
        return
    print(f"\nTìm thấy {len(parquet_files)} file Parquet trong thư mục: {input_directory}.\n")

    new_tables = []
    time1 = datetime.now()
    for file_name in parquet_files:
        file_path = os.path.join(input_directory, file_name)
        print(f"Đang đọc file: {file_path} ...")
        try:
            table = pq.read_table(file_path)  # Đọc trực tiếp một file Parquet vào một đối tượng pyarrow.Table
            if table.schema.equals(schema):   # Kiểm tra tính hợp lệ
                new_tables.append(table)
            else:
                print(f"Cảnh báo: Schema của '{file_path}' không khớp với schema ban đầu. Bỏ qua file này.")
        except Exception as e:
            print(f"Lỗi khi đọc file: {file_path}: {e}")
            continue

    if not new_tables:
        print("Không có dữ liệu mới hợp lệ để hợp nhất.")
        return
    print(f"Thời gian đọc: {(datetime.now() - time1).seconds + ((datetime.now() - time1).microseconds) / 1000000}s")


    print("\nĐang nối các bảng dữ liệu mới...")
    time2 = datetime.now()
    new_data_table = pa.concat_tables(new_tables)
    print(f"Tổng số hàng dữ liệu mới: {new_data_table.num_rows}")
    print(f"Thời gian nối: {(datetime.now() - time2).seconds + ((datetime.now() - time2).microseconds) / 1000000}s")

    final_table = new_data_table

    # Kiểm tra xem file đầu ra đã tồn tại chưa
    if os.path.exists(output_file_path):
        print(f"\nĐang đọc dữ liệu gốc từ {output_file_path} (nếu có)...")
        try:
            time3 = datetime.now()
            root_table = pq.read_table(output_file_path)
            print(f"Tổng số hàng dữ liệu hiện có: {root_table.num_rows}")
            print(f"Thời gian đọc: {(datetime.now() - time3).seconds + ((datetime.now() - time3).microseconds) / 1000000}s")

            print("\nĐang nối dữ liệu gốc với dữ liệu mới...")
            time4 = datetime.now()
            final_table = pa.concat_tables([root_table, new_data_table])
            print(f"Tổng số hàng sau khi nối: {final_table.num_rows}")
            print(f"Thời gian nối: {(datetime.now() - time4).seconds + ((datetime.now() - time4).microseconds) / 1000000}s")

            query_time = datetime.now()
            con = duckdb.connect(database=':memory:', read_only=False)
            def agg_query(query_str):
                result = con.execute(query_str).fetchdf()
                print("Kết quả truy vấn:")
                print(result)

            # Đăng ký PyArrow Table vào DuckDB dưới một tên tạm thời
            # DuckDB có thể trực tiếp truy vấn các đối tượng PyArrow Table đã được đăng ký
            con.register("my_data_table", final_table)
            qr = f"""
                SELECT year, AVG(t2m_max) AS avg_max_temp, SUM(precipitation) AS total_precip
                FROM my_data_table  -- Sử dụng tên đã đăng ký của bảng PyArrow
                GROUP BY year
                ORDER BY year;
            """
            print("\nĐang chạy truy vấn SQL trên PyArrow Table trong bộ nhớ...")
            agg_query(qr)

            con.close()
            print(f"Thời gian truy vấn: {(datetime.now() - query_time).seconds + ((datetime.now() - query_time).microseconds) / 1000000}s")


        except Exception as e:
            print(f"Lỗi khi đọc file đầu ra hiện có '{output_file_path}': {e}. Sẽ chỉ ghi dữ liệu mới.")

    print(f"\nĐang ghi toàn bộ dữ liệu đã nối vào '{output_file_path}'...")
    try:
        time5 = datetime.now()
        pq.write_table(final_table, output_file_path)
        print(f"Thời gian hợp nhất: {(datetime.now() - time5).seconds + ((datetime.now() - time5).microseconds) / 1000000}s")

        print("\nQuá trình hợp nhất và ghi hoàn tất!")
    except Exception as e:
        print(f"Lỗi khi ghi file '{output_file_path}': {e}")

    finally:
        if os.path.exists(input_directory):
            try:
                print(f"\nĐang xóa thư mục đầu vào: {input_directory}")
                time6 = datetime.now()
                shutil.rmtree(input_directory)
                print(f"Đã xóa thành công thư mục: {input_directory}.")
                print(f"Thời gian xoá: {(datetime.now() - time6).seconds + ((datetime.now() - time6).microseconds) / 1000000}s\n")
            except OSError as e:
                print(f"Lỗi khi xóa thư mục: {input_directory}': {e}")

# def agg_query(qr):
#     time7 = datetime.now()
#     conn = duckdb.connect()

#     result = conn.sql(qr).df()
#     print(result)
#     conn.close()
#     print(f"Thời gian truy vấn: {(datetime.now() - time7).seconds + ((datetime.now() - time7).microseconds) / 1000000}s\n")


def main():
    input_json_folder = "raw_data"
    output_parquet_folder = "parquet_data"
    os.makedirs(output_parquet_folder, exist_ok=True)

    file_path = '/Users/trung/regenai/duckdb/source/wt_data.parquet'
    schema = pq.read_table(file_path).schema
    input_dir = "parquet_data"
    output_dir = "parquet_final"
    output_file = os.path.join(output_dir, "merged_output.parquet")

    if not os.path.exists(input_dir):
        os.makedirs(input_dir)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    from pipeline2 import convert_json_to_parquet
    start_time = datetime.now()
    convert_json_to_parquet(input_json_folder, output_parquet_folder)
    
    merge_new_parquet(input_dir, output_file, schema)

    print(f"Tổng thời gian: {(datetime.now() - start_time).seconds + ((datetime.now() - start_time).microseconds) / 1000000}s\n")
    
if __name__=='__main__':
    main()