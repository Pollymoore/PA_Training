import pymysql
import csv
import boto3
import configparser

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("mysqlConfig", "hostname")
port = parser.get("mysqlConfig", "port")
username = parser.get("mysqlConfig", "username")
dbname = parser.get("mysqlConfig", "database")
password = parser.get("mysqlConfig", "password")


conn = pymysql.connect(host= hostname, 
                       user=username,
                       password=password,
                        db= dbname,
                        port=int(port))


if conn is None:
    print("Error connecting to the MySQL database")
else:
    print("MySQL connection established!")

m_query= "SELECT * FROM Orders;"
local_filename= "orders_extract.csv"

m_cursor= conn.cursor()
m_cursor.execute(m_query)
results= m_cursor.fetchall()

with open (local_filename, 'w') as fp:
    csv_w= csv.writer(fp, delimiter= '|')
    csv_w.writerows(results)

fp.close()
m_cursor.close()
conn.close()

#load the csv to s3 bucket
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key= parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

S3= boto3.client("s3",
aws_access_key_id=access_key,
aws_secret_access_key=secret_key)


s3_file = local_filename

S3.upload_file(local_filename, bucket_name, s3_file)