import boto3
import csv
import os
from dotenv import load_dotenv
load_dotenv() 

def put_attribute(sdb, domain, key, val):
    response = sdb.put_attributes(
        DomainName=domain,
        ItemName=key,
        Attributes=[
            {
                'Name': "Category",
                'Value': val,
                'Replace': True
            },
        ],
    )
    return response

if __name__=="__main__":
    DOMAIN_NAME = "1229798494-simpleDB"
    sdb = boto3.client(
            service_name= 'sdb',
            region_name =  os.getenv("AWS_REGION"),
            aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY") 
        )
    response = sdb.create_domain(
        DomainName=DOMAIN_NAME
    )
    print(response)
    with open('./classification_results.csv','r') as f:
        csvFile = csv.reader(f)
        for line in csvFile:
            put_attribute(sdb,DOMAIN_NAME,line[0],line[1])
    
    records = sdb.select(
        SelectExpression='select * from `%s`' % (DOMAIN_NAME),
    )

    print(records)