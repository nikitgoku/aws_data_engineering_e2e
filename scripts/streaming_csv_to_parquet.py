import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)

job.init(args['JOB_NAME'], args)

glue_dynamic_frame_initial = glueContext.create_dynamic_frame.from_catalog(database='streaming_history', table_name='streaming_csv')

df_spark = glue_dynamic_frame_initial.toDF()


def prepare_dataframe(df):
    """Rename the columns and drop unnecessary columns. Remove NULL records"""
    # Drop unnecessary columns
    df_dropped = df.drop("username") \
                   .drop("conn_country") \
                   .drop("ip_addr_decrypted") \
                   .drop("user_agent_decrypted") \
                   .drop("spotify_track_uri") \
                   .drop("episode_name") \
                   .drop("episode_show_name") \
                   .drop("spotify_episode_uri") \
                   .drop("reason_start") \
                   .drop("reason_end") \
                   .drop("shuffle") \
                   .drop("offline") \
                   .drop("offline_timestamp") \
                   .drop("incognito_mode")
                   
    # Rename columns with proper name               
    df_renamed = df_dropped.withColumnRenamed("ts", "time_stamp") \
                           .withColumnRenamed("platform", "device_used") \
                           .withColumnRenamed("ms_played", "minutes_played") \
                           .withColumnRenamed("master_metadata_track_name", "track_name") \
                           .withColumnRenamed("master_metadata_album_artist_name", "artist_name") \
                           .withColumnRenamed("master_metadata_album_album_name", "album_name") \
                           .withColumnRenamed("skipped", "if_skipped")
    
    # Extract month_name, date and time in hours from the data frame
    # drop redundant columns
    df_extract = df_renamed.withColumn("month_name", F.date_format(F.col("time_stamp"), "MMMM")) \
                           .withColumn("date", F.date_format(F.col("time_stamp"), "yyyy-MM-dd")) \
                           .withColumn('time_HH', F.date_format(F.col("time_stamp"), "HH")) \
                           .drop("month") \
                           .drop("time") \
                           .drop("time_stamp")
    
    return df_extract

df_final = prepare_dataframe(df_spark)
# From Spark dataframe to glue dynamic frame
glue_dynamic_frame_final = DynamicFrame.fromDF(df_final, glueContext, "glue_etl")

# Write the data in the DynamicFrame to a location in Amazon S3 and a table for it in the AWS Glue Data Catalog
s3output = glueContext.getSink(
    path="s3://aws-glue-job-spark-bucket/data/streaming_history/streaming_parquet/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output",
)

s3output.setCatalogInfo(
  catalogDatabase="streaming_history", catalogTableName="streaming_parquet"
)

s3output.setFormat("glueparquet")
s3output.writeFrame(glue_dynamic_frame_final)

job.commit()