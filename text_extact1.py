import json
import time
import boto3
import os
import urllib.parse
from io import BytesIO
# import fitz
# import PyPDF2
from PyPDF2 import PdfReader, PdfWriter
splitfiletexts=dict()

def start_job(client, s3_bucket_name, object_name):
    response = None
    response = client.start_document_text_detection(
        DocumentLocation={
            'S3Object': {
                'Bucket': s3_bucket_name,
                'Name': object_name
            }}
    )

    return response["JobId"]

def is_job_complete(client, job_id):
    time.sleep(1)
    response = client.get_document_text_detection(JobId=job_id)
    status = response["JobStatus"]
    print("Job status: {}".format(status))

    while(status == "IN_PROGRESS"):
        time.sleep(1)
        response = client.get_document_text_detection(JobId=job_id)
        status = response["JobStatus"]
        print("Job status: {}".format(status))

    return status

def get_job_results(client, job_id):
    pages = []
    time.sleep(1)
    response = client.get_document_text_detection(JobId=job_id)
    pages.append(response)
    print("Resultset page received: {}".format(len(pages)))
    next_token = None
    if 'NextToken' in response:
        next_token = response['NextToken']

    while next_token:
        time.sleep(1)
        response = client.get_document_text_detection(JobId=job_id, NextToken=next_token)
        pages.append(response)
        print("Resultset page received: {}".format(len(pages)))
        next_token = None
        if 'NextToken' in response:
            next_token = response['NextToken']

    return pages

def is_pdf_openable(bucket,key):
    return True
    # s3_client = boto3.client('s3')
    # try:
    #     response=s3_client.get_object(Bucket=bucket,Key=key)
    #     pdf_content=response['Body'].read()
    #     doc=fitz.open(stream=BytesIO(pdf_content))
    #     doc.close()
    #     return True
    # except Exception as e:
    #     print(f"error opening {key}: {str(e)}")
    #     return False
    
def read_pdf_files_from_folder(folder_path):
    s3_client = boto3.client('s3')
    s3_bucket_name = 's3sagemakerbucket'
    response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=folder_path)
    for obj in response['Contents']:
        if obj['Key'].endswith('.pdf'):
            print(obj['Key'])
            if is_pdf_openable(s3_bucket_name,obj['Key']):
                document_name = obj['Key']
                file=obj['Key'].split('/')[-1]

                client = boto3.client('textract',region_name=boto3.Session().region_name,aws_access_key_id='AKIARU3MIISFVHPQBHLV',aws_secret_access_key='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')
                job_id = start_job(client, s3_bucket_name, document_name)
                print("Started job with id: {}".format(job_id))
                if is_job_complete(client, job_id):
                    response = get_job_results(client, job_id)
                    lines=[]
                    for result_page in response:
                        for item in result_page["Blocks"]:
                            if item["BlockType"] == "LINE":
                                print(item["Text"])
                                lines.append(item["Text"])
                    splitfiletexts[file] = lines

def cleanup_local_file(directory):
    for file_name in os.listdir(directory):
        file_path = os.path.join(directory, file_name)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
        except Exception as e:
            print(f"Error deleting file {file_path}: {str(e)}")
    
    

def lambda_handler(event, context):
    # TODO implement
    print(event)
    filetexts=dict()
    s3_bucket_name=event['Records'][0]['s3']['bucket']['name']
    document_name=urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    
    print(s3_bucket_name,document_name)
    

    file=document_name.split('/')[-1]
    # client = boto3.client('textract',region_name=boto3.Session().region_name,aws_access_key_id='AKIARU3MIISFVHPQBHLV',aws_secret_access_key='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')
    client = boto3.client('textract')
    job_id = start_job(client, s3_bucket_name, document_name)
    print("Started job with id: {}".format(job_id))
    if is_job_complete(client, job_id):
        response = get_job_results(client, job_id)
    
    lines=[]
    for result_page in response:
        for item in result_page["Blocks"]:
            if item["BlockType"] == "LINE":
                print(item["Text"])
                lines.append(item["Text"])
    filetexts[file] = lines  
    
    for file, text in filetexts.items():
        file2 = file.split('.')[0]
        file2 = file.replace('.pdf','').replace('.','')
        with open(f'/tmp/{file2}.txt', 'w', encoding = "utf-8") as outfile:
            for line in lines:
                outfile.write(line + '\n')
            outfile.close()
        
    file=open(f'/tmp/{file2}.txt','rb')
    print(file.seek(0, os.SEEK_END))
    print("Size of file is :", file.tell(), "bytes")
    
    if(file.tell()>1):
        s3 = boto3.client('s3') 
        obj = s3.get_object(Bucket= s3_bucket_name, Key= document_name) 
        pdf_content=BytesIO(obj['Body'].read())
        reader = PdfReader(pdf_content)
        pdf_page_count = len(reader.pages)
        print(pdf_page_count)
        n=len(reader.pages)

        for i in range(0,n+1,2):
            output = PdfWriter()
            for j in range(i,i+3):
                if j<n:
                      output.add_page(reader.pages[j])
            if i<n:
                with open(f"/tmp/document-page{i}.pdf", "wb") as outputStream:
                    output.write(outputStream)
                s3.upload_file(Filename=f"/tmp/document-page{i}.pdf", Bucket=s3_bucket_name, Key=f"splitted_pdfs/document-page{i}.pdf")
        folder_name = 'splitted_pdfs/'
        read_pdf_files_from_folder(folder_name)
        
        for file, text in splitfiletexts.items():
            file2 = file.split('.')[0]
            file2 = file.replace('.pdf','').replace('.','')
            with open(f'/tmp/{file2}.txt', 'w', encoding = "utf-8") as outfile:
                for line in text:
                    outfile.write(line + '\n')
                outfile.close()
            s3.upload_file(Filename=f"/tmp/{file2}.txt", Bucket=s3_bucket_name, Key=f"splitted_text/{file2}.txt")
        
    else:
        s3 = boto3.client("s3")
        s3.upload_file(Filename=f"/tmp/{file2}.txt", Bucket=s3_bucket_name, Key=f"sagemaker-pipelines-nlp-demo/code/{file2}.txt")
    
    cleanup_local_file("/tmp")    
    return {
        'statusCode': 200,
        'body': json.dumps('text file written successfully!!')
    }
