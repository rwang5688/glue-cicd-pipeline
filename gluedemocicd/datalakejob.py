import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.context import DynamicFrame
import time


args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME','sourcedatabase', 'destinationpath','region'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
sourcedatabase = args['sourcedatabase']
destinationpath = args['destinationpath']
region = args['region']
print("debug: sourcedatabase=%s, destinationpath=%s, region=%s." % (sourcedatabase, destinationpath, region))

glue = boto3.client('glue',region_name=region)

response = glue.get_tables(DatabaseName=sourcedatabase)
print("debug: response=%s" % (response))
if 'TableList' in response:
    for table in response['TableList']:
        sourcetable = table['Name']
        print("debug: reading from sourcedatabase=%s, sourcetable=%s." % (sourcedatabase, sourcetable))
        datasource0 = glueContext.create_dynamic_frame.from_catalog(database = sourcedatabase, table_name = sourcetable, transformation_ctx = "datasource0")
        if datasource0.toDF().head(1):
            print("debug: writing to path=%s." % (destinationpath+sourcetable))
            datasink = glueContext.write_dynamic_frame.from_options(frame = datasource0, connection_type = "s3", connection_options = {"path": destinationpath+sourcetable}, format = "parquet", transformation_ctx = "datasink4")
job.commit()