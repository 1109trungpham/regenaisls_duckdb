from fastapi import FastAPI, UploadFile, File
from pydantic import BaseModel, conlist
from typing import List
import os, uuid, json
from pydantic import BaseModel
from typing import List, Union, Literal
from fastapi.responses import StreamingResponse
# from src1 import run_pipeline, duckdb_query, visualize_summary
from src2 import run_pipeline, duckdb_query, visualize_summary



UPLOAD_DIR = "new_data"
os.makedirs(UPLOAD_DIR, exist_ok=True)

app = FastAPI()


# === SCHEMA VALIDATION ===
class WeatherRecord(BaseModel):
    header: List[Literal["day", "month", "year", "day_of_year", "t2m_max", "t2m_min", "precipitation"]]
    value: List[List[Union[int, float]]]
    location: List[float]

class WeatherMultiResponse(BaseModel):
    duration: float
    data: List[WeatherRecord]


@app.post("/upload-and-run")
async def upload_and_run(files: List[UploadFile] = File(...)):
    results = []
    valid_json_files = 0

    for file in files:
        try:
            content = await file.read()
            data = json.loads(content)

            # Validate với Pydantic
            parsed = WeatherMultiResponse(**data)

            # Lưu nếu hợp lệ
            unique_name = f"wt_data_{uuid.uuid4().hex[:8]}.json"
            file_path = os.path.join(UPLOAD_DIR, unique_name)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            results.append({"filename": file.filename, "status": "✅ Hợp lệ"})
            valid_json_files += 1

        except Exception as e:
            results.append({"filename": file.filename, "status": f"❌ Không hợp lệ - {str(e)}"})

    if valid_json_files == 0:
        results.append({"pipeline_status": "⚠️ Không chạy pipeline vì không có file hợp lệ."})
        return {"results": results}

    # 🛠 Gọi ETL Pipeline chính
    try:
        run_pipeline()
        results.append({"pipeline_status": "🚀 Pipeline đã chạy thành công."})
    except Exception as e:
        results.append({"pipeline_status": f"❌ Lỗi khi chạy pipeline: {str(e)}"})

    return {"results": results}

@app.get("/chart")
def get_weather_chart():
    try:
        db_file = "database/weather_data.duckdb"
        table = "weather_data_table"
        query = f"""
            SELECT year, AVG(t2m_max) AS avg_max_temp, SUM(precipitation) AS total_precip
            FROM {table}
            GROUP BY year
            ORDER BY year;
        """
        df = duckdb_query(db_file, query)
        buf = visualize_summary(df)

        return StreamingResponse(buf, media_type="image/png")
    except Exception as e:
        return {"error": str(e)}