# Exemplo em src/transform/clean_silver.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Importando diretamente o contrato que definimos
from schemas import SCHEMA_BASICS_RAW, SCHEMA_RATINGS_RAW

spark = SparkSession.builder.appName("IMDbSilverPipeline").getOrCreate()

# Leitura com Schema Enforcement estrito (sem inferSchema!)
df_basics_raw = (
    spark.read
    .option("sep", "\t")
    .option("nullValue", "\\N")   # Trata nativamente o nulo do IMDb no Spark
    .schema(SCHEMA_BASICS_RAW)    # Aplica o contrato de dados
    .csv("gs://seu-bucket-bronze/title.basics.tsv.gz")
)

# Transformação Silver: normalizando genres para Array e tipando booleano
df_basics_silver = (
    df_basics_raw
    .withColumn("isAdult", F.col("isAdult") == 1)
    .withColumn("genres", F.split(F.col("genres"), ","))
)