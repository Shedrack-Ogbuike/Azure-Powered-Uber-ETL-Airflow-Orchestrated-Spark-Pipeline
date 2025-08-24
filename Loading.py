import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

def run_loading(bookings, customers, drivers, locations):
    try:

        # Output directory for local saving
        OUTPUT_DIR = "/home/wonder1844/airflow/uber_project_dag/data/cleaned"
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Dictionary of tables
        tables = {
            "bookings": bookings,
            "customers": customers,
            "drivers": drivers,
            "locations": locations,
        }

        # Save using Pandas (after converting from Spark)
        local_files = []
        for name, df in tables.items():
            path = os.path.join(OUTPUT_DIR, f"{name}.csv")
            df.toPandas().to_csv(path, index=False)
            local_files.append((path, f"cleaned/{name}.csv"))
            print(f"Saved {name}.csv locally at {path}")

        # Load environment variables
        load_dotenv("from azure.storage.blob import BlobServiceClient")
        connection_string = os.getenv("AZURE_CONNECTION_STRING")
        container_name = os.getenv("AZURE_CONTAINER")

        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        # Upload predictable filenames to Azure
        for local_path, blob_name in local_files:
            with open(local_path, "rb") as data:
                blob_client = container_client.get_blob_client(blob_name)
                blob_client.upload_blob(data, overwrite=True)
            print(f"Uploaded {blob_name} to Azure Blob Storage.")

    except Exception as e:
        print(f"An error occurred during loading: {e}")




