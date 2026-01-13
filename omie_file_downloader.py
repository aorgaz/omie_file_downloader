import os
import time
import requests
from datetime import datetime, timedelta
from tqdm import tqdm

# --- CONFIGURATION ---
BASE_URL = "https://www.omie.es/es/file-download?parents={parent}&filename={filename}"
ROOT_FOLDER = "omie_data"
LOG_FILE = "download_errors.log"
DELAY = 0.15  # Slightly faster delay (Approx 20-25 mins total)
START_YEAR = 2019
END_YEAR = 2025

FILE_TYPES = {
    "marginalpdbc": {"freq": "daily", "exts": [".1", ".2", ".3"]},
    "pdbc": {"freq": "monthly", "exts": [".zip"]},
    "pdvd": {"freq": "monthly", "exts": [".zip"]},
    "pibci": {"freq": "monthly", "exts": [".zip"]},
    "trades": {"freq": "monthly", "exts": [".zip"]}
}

def log_error(message):
    with open(LOG_FILE, "a") as f:
        f.write(f"{datetime.now()}: {message}\n")

def download_file(session, parent, filename):
    # Determine the specific folder for this file type
    target_folder = os.path.join(ROOT_FOLDER, parent)
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)
        
    save_path = os.path.join(target_folder, filename)
    
    # Skip if already downloaded
    if os.path.exists(save_path):
        return "skipped"
    
    url = BASE_URL.format(parent=parent, filename=filename)
    try:
        with session.get(url, stream=True, timeout=15) as r:
            if r.status_code == 200:
                with open(save_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=32768): # Increased chunk size for speed
                        f.write(chunk)
                return "success"
            elif r.status_code == 404:
                return "not_found"
            else:
                log_error(f"Failed: {filename} (HTTP {r.status_code})")
                return "failed"
    except Exception as e:
        log_error(f"Error: {filename} ({str(e)})")
        return "failed"

def run_downloader():
    if not os.path.exists(ROOT_FOLDER): os.makedirs(ROOT_FOLDER)
    session = requests.Session()
    
    tasks = []
    curr = datetime(START_YEAR, 1, 1)
    end_date = datetime(END_YEAR, 12, 31)
    
    while curr <= end_date:
        # Daily & Annual Logic for marginalpdbc
        for ext in FILE_TYPES["marginalpdbc"]["exts"]:
            fn = f"marginalpdbc_{curr.strftime('%Y%m%d')}{ext}"
            tasks.append(("marginalpdbc", fn))
        
        if curr.month == 1 and curr.day == 1: # Annual zip
            tasks.append(("marginalpdbc", f"marginalpdbc_{curr.year}.zip"))

        # Monthly Logic
        if curr.day == 1:
            for key in ["pdbc", "pdvd", "pibci", "trades"]:
                fn = f"{key}_{curr.strftime('%Y%m')}.zip"
                tasks.append((key, fn))
            
        curr += timedelta(days=1)

    print(f"Starting OMIE data harvest into folders...")
    
    for parent, filename in tqdm(tasks, desc="Overall Progress"):
        status = download_file(session, parent, filename)
        if status == "success":
            time.sleep(DELAY)
        elif status == "failed":
            time.sleep(DELAY * 2) # Wait longer if server is struggling

    print(f"\nTask Complete. Check '{LOG_FILE}' for any issues.")

if __name__ == "__main__":
    run_downloader()