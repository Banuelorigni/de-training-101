from pyspark.sql.functions import col, count, sum, when
from config import s3_folder
from utils.spark_util import spark


base_file_path = "s3a://" + s3_folder


def transform_data_for_failure_count(year, quarter=None, date=None):
    if date:
        file_path = f"{base_file_path}/{year}/{quarter}/{date}.csv"
    else:
        file_path = f"{base_file_path}/{year}/*/*.csv"
    
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    df = df.withColumn("drive", when(col("model").startswith("CT"), "Crucial")
                    .when(col("model").startswith("DELLBOSS"), "Dell BOSS")
                    .when(col("model").startswith("HGST"), "HGST")
                    .when(col("model").startswith("Seagate"), "Seagate")
                    .when(col("model").startswith("ST"), "Seagate")
                    .when(col("model").startswith("TOSHIBA"), "Toshiba")
                    .when(col("model").startswith("WDC"), "Western Digital")
                    .otherwise("Others"))
    
    summary_df = df.groupBy("drive").agg(
        count("*").alias("total_drive_count"),
        sum(col("failure")).alias("total_drive_failures")
    )
    
    summary_df.show()

