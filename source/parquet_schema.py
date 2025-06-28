import pyarrow.parquet as pq
import pyarrow as pa
import os

file_path = '/Users/trung/regenai/duckdb/source/wt_data.parquet'
table = pq.read_table(file_path)
schema = table.schema
