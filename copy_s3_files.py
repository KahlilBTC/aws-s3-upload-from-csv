import os
import pandas as pd
import requests
import boto3
from botocore.exceptions import NoCredentialsError

# Replace 'your_bucket' with your S3 bucket name
bucket_name = ''
s3 = boto3.client('s3')

# Set the row number to start from
start_from_row = 341602  # Change this to the row you want to start from

# Function to upload a file to S3
def upload_to_s3(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = file_name

    try:
        s3.upload_file(file_name, bucket, object_name)
        print(f'File {file_name} uploaded to {bucket}')
        return f"https://{bucket}.s3.amazonaws.com/{object_name}"
    except NoCredentialsError:
        print("No AWS credentials found")
        return None

# Read the CSV file
# Replace 'urls.csv' with your csv file name
df = pd.read_csv('my-files-to-download.csv')

# Create a new column in the DataFrame to hold the new S3 URLs
df['s3_url'] = 'newPath'

# Iterate over each row in the DataFrame starting from start_from_row
for index, row in df.iterrows():
    if index < start_from_row:
        continue
    # Get the URL of the PDF
    pdf_url = row['PDF Asset Path']

    # Use the requests library to get the PDF data
    response = requests.get(pdf_url)

    # Create a temporary file with the PDF data
    filename = pdf_url.split('/')[-1].split(';')[0].split('.pdf')[0]

    file_name = f'{filename}.pdf'
    with open(file_name, 'wb') as f:
        f.write(response.content)

    # Upload the PDF to S3 and get the new S3 URL
    s3_url = upload_to_s3(file_name, bucket_name, object_name=file_name)
    
    # Delete the temporary file
    os.remove(file_name)

    # Set the new S3 URL in the DataFrame
    df.at[index, 'newurl'] = s3_url

# Write the DataFrame back to the CSV
# Replace 'urls.csv' with your csv file name
df.to_csv('my-files-to-download.csv', index=False)
