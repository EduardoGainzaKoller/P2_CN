import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Capturamos los argumentos definidos en la plantilla YAML
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATABASE_NAME', 'OUTPUT_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Leemos los datos desde el catálogo de Glue (zona RAW)
datasource = glueContext.create_dynamic_frame.from_catalog(
    database = args['DATABASE_NAME'], 
    table_name = "raw" 
)

# Guardamos los datos en formato Parquet en la zona PROCESSED, particionados por tipo de evento
glueContext.write_dynamic_frame.from_options(
    frame = datasource,
    connection_type = "s3",
    format = "parquet",
    connection_options = {
        "path": args['OUTPUT_PATH'],
        "partitionKeys": ["event_type"]
    }
)

job.commit()