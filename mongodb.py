from pymongo import MongoClient
import csv
import boto3
import datetime
from datetime import timedelta
import configparser

#load the mongo_config files

parser = configparser.ConfigParser()
parser.read('Scripts/pipeline.conf')
hostname = parser.get('mongo_config', 'hostname')
username = parser.get('mongo_config', 'username')
password = parser.get('mongo_config', 'password')
database_name = parser.get('mongo_config', 'database')
collection_name = parser.get('mongo_config', 'collection')

#connect to the mongo client
mongo_client = MongoClient( "mongodb+srv://admin:admin@pmcluster.c8p57hj.mongodb.net/?retryWrites=true&w=majority")

#connect to the db where the collection resides
mongo_db = mongo_client[database_name]

#choose collection name
mongo_collection = mongo_db[collection_name]

start_date= datetime.datetime.today() + timedelta(days = -1)
end_date = start_date + timedelta(days=1)


mongo_query= { "$and":[{"bedrooms" :
{ "$gte": 4}}, {"bedrooms" :
{"$lt": 6}}]}




event_docs= mongo_collection.find(mongo_query, batch_size= 3000)


#create a blank list to store the results
all_events = []

#iterate through cursor
for doc in event_docs:
    summary = doc.get("summary", None)
    current_event =[]
    current_event.append(summary)

    all_events.append(current_event)

export_file = "mongoDB_export.csv"

with open (export_file, 'w') as fp:
    csvw= csv.writer(fp, delimiter='|')
    csvw.writerows(all_events)
fp.close()

#import into S3

#load in the boto parser files
parser = configparser.ConfigParser()
parser.read("Scripts/pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key= parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

S3= boto3.client("s3",
aws_access_key_id=access_key,
aws_secret_access_key=secret_key)


s3_file = 'myMongoDBfile.csv'

S3.upload_file(export_file, bucket_name, s3_file)
