from pyspark.sql import SparkSession


def run_extraction(file_path="/home/wonder1844/airflow/uber_project_dag/raw_data/ncr_ride_bookings.csv"):
    try:
        spark = SparkSession.builder.appName("UberETL").getOrCreate()
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        print("Data Extracted Successfully.")
        return df
    except Exception as e:
        print(f"An error occurred during extraction: {e}")
        return None
