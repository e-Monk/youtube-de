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

# Script generated for node Amazon S3
AmazonS3_node1707300916636 = glueContext.create_dynamic_frame.from_catalog(
    database="youtube-project-raw_db",
    push_down_predicate="region == 'ca' OR region == 'gb' OR region == 'us'",
    table_name="raw_statistics",
    transformation_ctx="AmazonS3_node1707300916636",
)

# Script generated for node Change Schema
ChangeSchema_node1707301289819 = ApplyMapping.apply(
    frame=AmazonS3_node1707300916636,
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "long", "category_id", "bigint"),
        ("publish_time", "string", "publish_time", "string"),
        ("tags", "string", "tags", "string"),
        ("views", "long", "views", "bigint"),
        ("likes", "long", "likes", "bigint"),
        ("dislikes", "long", "dislikes", "bigint"),
        ("comment_count", "long", "comment_count", "bigint"),
        ("thumbnail_link", "string", "thumbnail_link", "string"),
        ("comments_disabled", "boolean", "comments_disabled", "boolean"),
        ("ratings_disabled", "boolean", "ratings_disabled", "boolean"),
        ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"),
        ("description", "string", "description", "string"),
        ("region", "string", "region", "string"),
    ],
    transformation_ctx="ChangeSchema_node1707301289819",
)

# Script generated for node Amazon S3
AmazonS3_node1707300940692 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1707301289819,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://youtube-project-bucket-cleansed/youtube/raw_statistics/",
        "partitionKeys": ["region"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1707300940692",
)

job.commit()
