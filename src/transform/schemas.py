"""
Módulo de Contratos de Dados (Schemas) para o IMDb Pipeline.
Define a estrutura estrita para as camadas Bronze (leitura) e Silver.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    BooleanType,
    ArrayType
)

# -------------------------------------------------------------------------
# 1. IMDB TITLE BASICS
# -------------------------------------------------------------------------
SCHEMA_BASICS_RAW = StructType([
    StructField("tconst", StringType(), nullable=False),
    StructField("titleType", StringType(), nullable=True),
    StructField("primaryTitle", StringType(), nullable=True),
    StructField("originalTitle", StringType(), nullable=True),
    StructField("isAdult", IntegerType(), nullable=True),
    StructField("startYear", IntegerType(), nullable=True),
    StructField("endYear", IntegerType(), nullable=True),
    StructField("runtimeMinutes", IntegerType(), nullable=True),
    StructField("genres", StringType(), nullable=True)  # Vem delimitado por vírgula no arquivo bruto
])

# -------------------------------------------------------------------------
# 2. IMDB TITLE RATINGS
# -------------------------------------------------------------------------
SCHEMA_RATINGS_RAW = StructType([
    StructField("tconst", StringType(), nullable=False),
    StructField("averageRating", FloatType(), nullable=True),
    StructField("numVotes", IntegerType(), nullable=True)
])

# -------------------------------------------------------------------------
# 3. IMDB TITLE EPISODES
# -------------------------------------------------------------------------
SCHEMA_EPISODES_RAW = StructType([
    StructField("tconst", StringType(), nullable=False),
    StructField("parentTconst", StringType(), nullable=False),
    StructField("seasonNumber", IntegerType(), nullable=True),
    StructField("episodeNumber", IntegerType(), nullable=True)
])