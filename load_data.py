from utils.aws_util import upload_to_s3
from botocore.exceptions import NoCredentialsError
import os
from config import s3_bucket_name


def save_to_csv_and_upload_to_s3(df, s3_key):
    temp_output_path = "/temp"
    df.coalesce(1).write.csv(temp_output_path, header=True, mode='overwrite')
    
    try:
        for root, dirs, files in os.walk("/temp"):
            for file in files:
                local_file_path = os.path.join(root, file)
                upload_to_s3(local_file_path, s3_bucket_name, s3_key)
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")
    finally:
        os.rmdir("/temp")


