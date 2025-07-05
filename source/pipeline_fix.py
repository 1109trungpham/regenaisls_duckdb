import time
import os
import json
import logging
import pandas as pd
import duckdb
from pathlib import Path
from pydantic import BaseModel, conint, confloat
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed


# --- Define ---
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "new_data"
PARQUET_DIR = DATA_DIR / "parquet"
DB_DIR = BASE_DIR / "database"
DB_PATH = DB_DIR / "weather_data.duckdb"
TABLE_NAME = "weather_data"

RENAME_MAP = {
    "day": "day",
    "month": "month",
    "year": "year",
    "day_of_year": "doy",
    "t2m_max": "max_temp",
    "t2m_min": "min_temp",
    "precipitation": "precip",
    "lon": "lon",
    "lat": "lat"
}


DUCKDB_COLUMNS = [
    "day", "month", "year", "doy", "max_temp", "min_temp", "precip", "lon", "lat"
]

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format='%(asctime)s - %(levelname)s - %(message)s' # Define the log message format
)

# --- Data Validation Model ---
class WeatherRecord(BaseModel):
    """
    Pydantic model for validating the structure and types of each weather record.
    Ensures data integrity with type hints and range constraints.
    """
    lon: confloat(ge=-180, le=180)      # Longitude: float between -180 and 180
    lat: confloat(ge=-90, le=90)        # Latitude: float between -90 and 90
    day: conint(ge=1, le=31)            # Day of month: integer between 1 and 31
    month: conint(ge=1, le=12)          # Month: integer between 1 and 12
    year: conint(ge=1900, le=2100)      # Year: integer between 1900 and 2100
    day_of_year: int                    # Day of year: integer (no specific range, but typically 1-366)
    t2m_max: float                      # Maximum temperature at 2 meters
    t2m_min: float                      # Minimum temperature at 2 meters
    precipitation: float                # Total precipitation

# --- JSON to Parquet Conversion ---
def validate_and_convert(json_path: Path) -> Path | None:
    """
    Validates data from a JSON file using Pydantic, transforms it into a Pandas DataFrame,
    renames columns, and saves it as a Parquet file.

    Args:
        json_path (Path): The path to the input JSON file.

    Returns:
        Path | None: The path to the generated Parquet file if successful, otherwise None.
    """
    try:
        with open(json_path, 'r', encoding='utf-8') as f: # Specify encoding for robustness
            raw_data = json.load(f)
        
        valid_records = []
        
        for location_data in raw_data.get("data", []):
            lon, lat = location_data.get("location", [None, None])
            
            if lon is None or lat is None:
                logging.warning(f"⚠️ Skipping record in {json_path.name} due to missing location data.")
                continue

            for row_values in location_data.get("value", []):
                try:
                    # Validate each row against the WeatherRecord Pydantic model
                    record = WeatherRecord(
                        lon=lon, lat=lat,
                        day=row_values[0], month=row_values[1], year=row_values[2], day_of_year=row_values[3],
                        t2m_max=row_values[4], t2m_min=row_values[5], precipitation=row_values[6]
                    )
                    valid_records.append(record.dict())
                except Exception as e:
                    # Log validation errors for individual rows but continue processing
                    logging.debug(f"DEBUG: Validation failed for a row in {json_path.name}: {e}. Skipping row.")
                    continue

        if valid_records:
            df = pd.DataFrame(valid_records)
            
            # Rename columns according to RENAME_MAP and select only DUCKDB_COLUMNS
            df = df.rename(columns=RENAME_MAP)[DUCKDB_COLUMNS]
            
            parquet_path = PARQUET_DIR / json_path.with_suffix(".parquet").name
            df.to_parquet(parquet_path, index=False) 
            logging.info(f"✅ Converted: {json_path.name} → {parquet_path.name}")
            return parquet_path
        else:
            logging.warning(f"⚠️ No valid records found in {json_path.name}. No Parquet file generated.")
            return None

    except json.JSONDecodeError as e:
        logging.error(f"❌ Failed to parse JSON from {json_path.name}: {e}")
        return None
    except Exception as e:
        logging.error(f"❌ An unexpected error occurred while converting {json_path.name}: {e}")
        return None

# --- Load to DuckDB ---
def append_parquets_to_duckdb(parquet_files: list[Path]):
    """
    Appends data from a list of Parquet files into a DuckDB table.
    Creates the database directory and table if they don't exist.

    Args:
        parquet_files (list[Path]): A list of paths to Parquet files to be loaded.
    """
    DB_DIR.mkdir(parents=True, exist_ok=True) # Ensure the database directory exists

    if not parquet_files:
        logging.warning("⚠️ No Parquet files provided to append to DuckDB.")
        return

    con = duckdb.connect(str(DB_PATH))

    try:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                day INTEGER,
                month INTEGER,
                year INTEGER,
                doy INTEGER,
                max_temp DOUBLE,
                min_temp DOUBLE,
                precip DOUBLE,
                lon DOUBLE,
                lat DOUBLE
            );
        """)

        # Convert Path objects to string paths for DuckDB's read_parquet function
        file_list_str = [str(p) for p in parquet_files]
        
        # Use DuckDB's efficient read_parquet function to insert data
        con.execute(f"""
            INSERT INTO {TABLE_NAME}
            SELECT * FROM read_parquet({file_list_str});
        """)
        logging.info(f"📥 Successfully appended {len(parquet_files)} Parquet files to DuckDB table '{TABLE_NAME}'.")
    except duckdb.Error as e:
        logging.error(f"❌ DuckDB error while appending files: {e}")
    except Exception as e:
        logging.error(f"❌ An unexpected error occurred during DuckDB append: {e}")
    finally:
        con.close() # Always ensure the connection is closed

# --- Cleanup ---
def cleanup_parquets(parquet_files: list[Path]):
    """
    Deletes a list of Parquet files from the file system after they have been processed.

    Args:
        parquet_files (list[Path]): A list of paths to Parquet files to be deleted.
    """
    for f in parquet_files:
        try:
            f.unlink() # Delete the file
            logging.info(f"🗑️ Deleted temporary Parquet file: {f.name}")
        except OSError as e:
            logging.warning(f"❌ Could not delete Parquet file {f.name}: {e}")
        except Exception as e:
            logging.warning(f"❌ An unexpected error occurred while deleting {f.name}: {e}")

# --- Main Pipeline Execution ---
def run_pipeline():
    """
    Orchestrates the entire data processing pipeline:
    1. Discovers new JSON files.
    2. Converts JSON to Parquet in parallel (using ThreadPoolExecutor for I/O bound tasks
       and ProcessPoolExecutor for CPU-bound validation/conversion).
    3. Appends generated Parquet files to the DuckDB database.
    4. Cleans up the temporary Parquet files.
    """
    PARQUET_DIR.mkdir(parents=True, exist_ok=True) 
    json_files = list(DATA_DIR.glob("*.json")) 
    
    processed_parquet_files = []

    if not json_files:
        logging.info("📂 No new JSON files found to process. Exiting pipeline.")
        return

    logging.info(f"🚀 Starting to process {len(json_files)} JSON files...")

    with ThreadPoolExecutor(max_workers=4) as thread_pool:
        with ProcessPoolExecutor(max_workers=os.cpu_count()) as process_pool:
            futures = [
                process_pool.submit(validate_and_convert, jf)
                for jf in json_files
            ]
            
            # Wait for all futures to complete and collect results
            for future in as_completed(futures):
                try:
                    # Get the result from the completed future (which is the parquet file path or None)
                    parquet_file_path = future.result()
                    if parquet_file_path:
                        processed_parquet_files.append(parquet_file_path)
                except Exception as e:
                    # Log any exceptions that occurred during conversion of a specific file
                    logging.error(f"❌ Error during JSON to Parquet conversion: {e}")

    if processed_parquet_files:
        logging.info(f"📊 {len(processed_parquet_files)} valid Parquet files generated. Proceeding to load to DuckDB.")
        append_parquets_to_duckdb(processed_parquet_files) 
        cleanup_parquets(processed_parquet_files) 
        logging.info("✅ Data pipeline completed successfully.")
    else:
        logging.info("⚠️ No valid Parquet files were generated. Nothing to append to DuckDB.")

