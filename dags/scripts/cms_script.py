
import requests
from google.cloud import storage, bigquery
import pandas as pd
import numpy  as np
import os
import sys

type_data_list = ['OWNRSHP']
bucket_name = "zoomcamp-bucket-storage"  # Replace with your GCS bucket name

schema = {
    "Change_Type": str,
    "Physician_Profile_ID": str,
    "Physician_NPI": str,
    "Physician_First_Name": str,
    "Physician_Middle_Name": str,
    "Physician_Last_Name": str,
    "Physician_Name_Suffix": str,
    "Recipient_Primary_Business_Street_Address_Line1": str,
    "Recipient_Primary_Business_Street_Address_Line2": str,
    "Recipient_City": str,
    "Recipient_State": str,
    "Recipient_Zip_Code": str,
    "Recipient_Country": str,
    "Recipient_Province": str,
    "Recipient_Postal_Code": str,
    "Physician_Primary_Type": str,
    "Physician_Specialty": str,
    "Record_ID": str,
    "Program_Year": str,
    "Total_Amount_Invested_USDollars": float,
    "Value_of_Interest": float,
    "Terms_of_Interest": str,
    "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name": str,
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID": str,
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name": str,
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State": str,
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country": str,
    "Dispute_Status_for_Publication": str,
    "Interest_Held_by_Physician_or_an_Immediate_Family_Member": str
}

def upload_to_gcs(bucket_name, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    yearstart = 2017
    yearend = 2024
    for i in range(yearstart, yearend):
        source_file_name = f'/home/eman/virtual_env/files/cms_open/OWNRSHP_{i}_correctedPD.csv'

        blob.upload_from_filename(source_file_name)
        print(f"File {source_file_name} uploaded to gs://{bucket_name}/{destination_blob_name}")

def down_up_gcs():
    for type_data in type_data_list:
        for year in range(2017,2024):
            year = str(year)
            
            script_dir = os.path.dirname(os.path.abspath(__file__))
            baseurl = f"https://download.cms.gov/openpayments/PGYR{year}_P01302025_01212025/OP_DTL_{type_data}_PGYR{year}_P01302025_01212025.csv"
            local_filename = f"{type_data}_{year}.csv"
            destination_blob_name = f"cms_data/{local_filename}"
            files_dir = os.path.join(script_dir, "/home/eman/virtual_env/files/cms_open")
            local_filename = os.path.join(files_dir, local_filename)
            
            print(f"This is the type: {type_data}")
            print(f"This is the year: {year}")
            print(f"This is the base url: {baseurl}")
            print(f"This would be the local filename: {local_filename}")
            print(f"This would be the blob name: {destination_blob_name}")

            print(f"Processing: {baseurl}")  # Debugging print
            
            
            response = requests.get(baseurl, stream=True)
            if response.status_code == 200:
                with open(local_filename, "wb") as file:
                    chunk_count = 0
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)
                        chunk_count += 1
                        sys.stdout.write(f"\rChunks downloaded: {chunk_count}")
                        sys.stdout.flush()
                    print(f"File downloaded: {local_filename}")
            else:
                print(f"Failed to download file. Status code: {response.status_code}")
                raise ValueError(f"Failed to download file: {response.status_code}")

def convert_csv(filein, fileout):
    df = pd.read_csv(filein, dtype=schema, parse_dates=['Payment_Publication_Date'])
    df.fillna(' ', inplace=True)
    df['Terms_of_Interest'].replace({r'\n':' '}, regex=True, inplace=True)
    df.to_csv(fileout, index=False)
                
def correct_csv_format(yearbegin, yearend):
    """
    Corrects CSV file formatting by appending rows that are split across multiple lines for each year in the range.
    """
    for i in range(yearbegin, yearend):
        csv_correct_input = f'/home/eman/virtual_env/files/cms_open/OWNRSHP_{i}.csv'
        csv_corrected = f'/home/eman/virtual_env/files/cms_open/OWNRSHP_{i}_correctedPD.csv'

        try:
            convert_csv(csv_correct_input, csv_corrected)
        except FileNotFoundError:
            print(f"File not found: {csv_correct_input}")
        except Exception as e:
            print(f"An error occurred while processing {csv_correct_input}: {e}")

def GCS_to_BQ(bucket, project_id):
    bigquery_client = bigquery.Client(project=project_id)
    
    # Initialize Storage client
    storage_client = storage.Client(project=project_id)
    
    for year in range(2017,2024):
        source_uri = f"gs://{bucket}/cms_data/OWNRSHP_{year}_correctedPD.csv"
        table_id = f"{project_id}.Open_payments.owner_{year}"

        # Define the schema for the table
        schema = [
            bigquery.SchemaField("Change_Type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Physician_Profile_ID", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Physician_NPI", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Physician_First_Name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Physician_Middle_Name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Physician_Last_Name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Physician_Name_Suffix", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Recipient_Primary_Business_Street_Address_Line1", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Recipient_Primary_Business_Street_Address_Line2", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Recipient_City", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Recipient_State", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Recipient_Zip_Code", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Recipient_Country", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Recipient_Province", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Recipient_Postal_Code", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Physician_Primary_Type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Physician_Specialty", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Record_ID", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Program_Year", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Total_Amount_Invested_USDollars", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("Value_of_Interest", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("Terms_of_Interest", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Dispute_Status_for_Publication", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Interest_Held_by_Physician_or_an_Immediate_Family_Member", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Payment_Publication_Date", "DATE", mode="NULLABLE"),
        ]

        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        )

        # Load data from GCS to BigQuery
        load_job = bigquery_client.load_table_from_uri(
            source_uri,
            table_id,
            job_config=job_config
        )

        # Wait for the load job to complete
        load_job.result()
        print(f"Loaded {year} data into {table_id}")
            
    
    


