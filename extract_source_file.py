import requests
import zipfile
import os
from utils.aws_util import upload_to_s3
from io import BytesIO
from config import s3_bucket_name, s3_folder

base_url = "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/"
local_download_dir = "/tmp/backblaze_data/"

if not os.path.exists(local_download_dir):
    os.makedirs(local_download_dir)

def download_and_extract_zip(url, download_path):
    if os.path.exists(download_path):
        print(f"{download_path} already exists. Skipping download.")
    else:
        print(f"Downloading from {url} to {download_path}...")
        response = requests.get(url)
        
        if response.status_code == 200:
            with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
                zip_file.extractall(download_path)
                print(f"Extracted files to {download_path}")
        else:
            print(f"Failed to download {url}. Status code: {response.status_code}")


def extract_and_upload_data(start_year, start_quarter, end_year, end_quarter):
    def generate_quarterly_intervals(start, end):
        start_y, start_q = start
        end_y, end_q = end
        for year in range(start_y, end_y + 1):
            for quarter in range(1, 5):
                if (year == start_y and quarter < start_q) or (year == end_y and quarter > end_q):
                    continue
                yield year, f"Q{quarter}"

    for year, quarter in generate_quarterly_intervals((start_year, start_quarter), (end_year, end_quarter)):
        download_path = os.path.join(local_download_dir, f"{year}_{quarter}")
        url = f"{base_url}data_{quarter}_{year}.zip"
        download_and_extract_zip(url, download_path)

        files_to_upload = [
            (os.path.join(root, file), os.path.join(s3_folder, f"{year}/{quarter}/{file}"))
            for root, dirs, files in os.walk(download_path)
            for file in files
            if not (file.startswith("._") or "__MACOSX" in root)
        ]

        for local_file_path, s3_key in files_to_upload:
            upload_to_s3(local_file_path, s3_bucket_name, s3_key)
    os.rmdir(local_download_dir)

print("Finished extracting all data!")
