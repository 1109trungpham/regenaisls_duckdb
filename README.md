

# Cấu trúc thư mục:
.
├── charts/                   # Thư mục chứa biểu đồ đầu ra (PNG)
├── database/                 # Chứa file DuckDB lưu trữ dữ liệu
├── new_data/                 # Tệp JSON mới chưa xử lý
├── parquet_data/             # Tệp Parquet tạm sau khi chuyển đổi từ JSON
├── raw_data/                 # Lưu trữ JSON gốc sau khi xử lý
├── source/                   # Mã nguồn xử lý dữ liệu
│   ├── json_creator.py
│   ├── json_form.txt
│   ├── parquet_schema.py
│   ├── pipeline1.py
│   ├── pipeline2.py
│   ├── pipeline2p1.py
│   ├── pipeline2p2.py
│   ├── wt_data.json
│   └── wt_data.parquet
├── .gitignore
├── main.py                
├── README.md              
└── requirements.txt         


pip install duckdb

Chạy json_creator.py để tạo nguồn dữ liệu

# Hiệu suất
Pipe2 nhanh hơn Pipe1 khoảng 1s khi xử lý 500MB JSON
