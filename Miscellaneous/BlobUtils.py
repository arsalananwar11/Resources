from azure.storage.blob import BlobClient
import pandas as pd


def download_file_from_blob(local_file_name, storage_account, container_name, blob_name):
    # download and save a blob file locally and return it as a dataframe       
    blob = BlobClient.from_connection_string(conn_str = storage_account, container_name = container_name, blob_name = blob_name)
    if(blob.exists()):
        with open(local_file_name, "wb") as localfile:
            blob_data = blob.download_blob()
            blob_data.readinto(localfile)
        if(blob_name.endswith("parquet")):
            df = pd.read_parquet(local_file_name)
        else:
            df = pd.read_csv(local_file_name)
        return df
    else:
        return pd.DataFrame()

        
def upload_file_to_blob(local_file_name, storage_account, container_name, blob_name):
    # uploads a local file to blob
    blob = BlobClient.from_connection_string(conn_str = storage_account, container_name = container_name, blob_name = blob_name)
    with open(local_file_name, "rb") as data:
        blob.upload_blob(data,overwrite=True)