from fastapi import FastAPI, UploadFile, File
import uvicorn
import asyncio
from concurrent.futures import ThreadPoolExecutor
import boto3
import os
from dotenv import load_dotenv
load_dotenv()
# pip install python-multipart fastapi "uvicorn[standard]" asyncio
app = FastAPI()

S3_BUCKET_NAME = "1229798494-in-bucket"
SDB_DOMAIN_NAME = "1229798494-simpleDB"

s3_client = boto3.client(
                service_name='s3',
                region_name='us-east-1',
                aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY") 
            )
sdb_client = boto3.client(
                service_name='sdb',
                region_name='us-east-1',
                aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY") 
            )
# AWS_BUCKET = s3.Bucket(S3_BUCKET_NAME)

executor = ThreadPoolExecutor(max_workers=20)

def save_image(file:UploadFile):
    try:
        s3_client.upload_fileobj(file.file,S3_BUCKET_NAME,file.filename)
        return {"message": f"File {file.filename} uploaded successfully"}
    except Exception as e:
        return {"error":str(e)}

def get_category(fileName: str):
    try:
        response = sdb_client.get_attributes(
            DomainName=SDB_DOMAIN_NAME,
            ItemName=fileName,
            AttributeNames=[
                'Category',
            ],
            ConsistentRead=False
        )
        return response
    except Exception as e:
        return {"error":str(e)}

@app.post("/")
async def create_upload_file(inputFile: UploadFile = File(...)):
    loop = asyncio.get_running_loop()
    saving = loop.run_in_executor(executor,save_image,inputFile)
    fileName = inputFile.filename.split('.')[0]
    classification = loop.run_in_executor(executor,get_category,fileName)
    
    result1, result2 = await asyncio.gather(saving,classification)
    print("result1:",result1)
    print("result2:",result2)
    
    if "Attributes" in result2 and result2["Attributes"]:
        category = result2["Attributes"][0]["Value"]
    else:
        category = "Unknown"
    return {fileName:category}

if __name__=="__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, workers=5,reload=True)