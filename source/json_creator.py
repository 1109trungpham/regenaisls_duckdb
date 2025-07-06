import shutil
import os


source_file = "source/wt_data.json"
target_folder = "new_data"

if not os.path.exists(target_folder):
    os.makedirs(target_folder)

for i in range(1, 11):
    target_file = os.path.join(target_folder, f"wt_data_{i}.json")
    shutil.copy(source_file, target_file)
    print(f"✅ Đã tạo: {target_file}")
