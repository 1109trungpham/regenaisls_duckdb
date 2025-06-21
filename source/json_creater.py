import shutil
import os

source_file = "raw_data/wt_data.json"
target_folder = "raw_data"

for i in range(1, 61):
    target_file = os.path.join(target_folder, f"wt_data_{i}.json")
    shutil.copy(source_file, target_file)
    print(f"✅ Đã tạo: {target_file}")
