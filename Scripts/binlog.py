from pymysqlreplication import BinLogStreamReader
from pymysqlreplication import row_event
import configparser
import pymysqlreplication
import csv
import boto3


#get the mysql query
parser = configparser.ConfigParser()
parser.read('Scripts/pipeline.conf')
port= parser.get('mysqlConfig', 'port')
username= parser.get('mysqlConfig', 'username')
password = parser.get('mysqlConfig', 'password')
host = parser.get('mysqlConfig', 'hostname')
database = parser.get('mysqlConfig', 'database')

mysql_settings = {
    "host" : host,
    "user" : username,
    "password" : password,
    "port" : int(port)
}


b_stream = BinLogStreamReader(
            connection_settings= mysql_settings,
            server_id=100,
            only_events=[row_event.DeleteRowsEvent, row_event.WriteRowsEvent, row_event.UpdateRowsEvent]
            )

for e in b_stream:
    ee= e.dump()

print(b_stream)

order_events =[]

for binlogevent in b_stream:
    for row in binlogevent.rows:
        if binlogevent.table == 'Orders':
            event = {}
            if isinstance(
                binlogevent,row_event.DeleteRowsEvent
                ):
                event["action"] = 'delete'
                event.update(row["values"].items())
            elif isinstance( binlogevent, row_event.UpdateRowsEvent):
                 event["action"] = 'update'
                 event.update(row["after_values"].items())
            elif isinstance(binlogevent, row_event.WriteRowsEvent):
                 event["action"] = 'insert'
                 event.update(row["values"].items())
        order_events.append(event)
        
b_stream.close()

keys= order_events[0].keys()
local_filename = 'binlog.csv'

with open (local_filename, 'w', newline= '') as output:
    dict_writer = csv.DictWriter(output, keys, delimiter='|')
    dict_writer.writerows(order_events)


#load in the boto parser files
parser = configparser.ConfigParser()
parser.read("Scripts/pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key= parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

S3= boto3.client("s3",
aws_access_key_id=access_key,
aws_secret_access_key=secret_key)


s3_file = local_filename

S3.upload_file(local_filename, bucket_name, s3_file)


