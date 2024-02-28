import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Catalog Cleaned stats ref data
AWSGlueCatalogCleanedstatsrefdata_node1707305775994 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="youtube-project-db-cleaned",
        table_name="cleaned_statistics_reference_data",
        transformation_ctx="AWSGlueCatalogCleanedstatsrefdata_node1707305775994",
    )
)

# Script generated for node AWS Glue Catalog Raw Stats
AWSGlueCatalogRawStats_node1707305794087 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="youtube-project-db-cleaned",
        table_name="raw_statistics",
        transformation_ctx="AWSGlueCatalogRawStats_node1707305794087",
    )
)

# Script generated for node Join
Join_node1707305864067 = Join.apply(
    frame1=AWSGlueCatalogCleanedstatsrefdata_node1707305775994,
    frame2=AWSGlueCatalogRawStats_node1707305794087,
    keys1=["id"],
    keys2=["category_id"],
    transformation_ctx="Join_node1707305864067",
)

# Script generated for node Amazon S3
AmazonS3_node1707306318202 = glueContext.getSink(
    path="s3://youtube-analytics-joined-tables-bucket",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region"],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1707306318202",
)
AmazonS3_node1707306318202.setCatalogInfo(
    catalogDatabase="youtube-project-db-cleaned",
    catalogTableName="final_table_analytics",
)
AmazonS3_node1707306318202.setFormat("glueparquet", compression="snappy")
AmazonS3_node1707306318202.writeFrame(Join_node1707305864067)
job.commit()
