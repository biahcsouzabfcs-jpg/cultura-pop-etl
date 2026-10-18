import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Garante que o Python encontre o módulo de schemas na mesma pasta
CURRENT_DIR = Path(__file__).resolve().parent
sys.path.append(str(CURRENT_DIR))

from schemas import SCHEMA_BASICS_RAW

def main():
    print("Iniciando a SparkSession...")
    spark = (
        SparkSession.builder
        .appName("IMDbLocalTest")
        .master("local[*]")  # Usa todos os núcleos da CPU disponíveis
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    
    # Reduz o excesso de logs informativos do Spark no terminal
    spark.sparkContext.setLogLevel("WARN")

    # Localiza a amostra
    sample_file = CURRENT_DIR.parent.parent / "data" / "samples" / "sample_basics.tsv"

    print(f"Lendo amostra com Schema Enforcement: {sample_file.name}")
    
    # Leitura com contrato estrito e tratamento de nulos
    df_raw = (
        spark.read
        .option("sep", "\t")
        .option("header", "true")
        .option("nullValue", "\\N")
        .schema(SCHEMA_BASICS_RAW)
        .csv(str(sample_file))
    )

    print("\n--- Schema Aplicado ---")
    df_raw.printSchema()

    print("\n--- Primeiros 5 Registros Brutos ---")
    df_raw.select("tconst", "titleType", "primaryTitle", "startYear", "genres").show(5, truncate=False)

    # Primeira transformação Silver: array de gêneros e flag booleana
    df_silver = (
        df_raw
        .withColumn("isAdult", F.col("isAdult") == 1)
        .withColumn("genres_list", F.split(F.col("genres"), ","))
    )

    print("\n--- Transformação Silver (Array e Booleano) ---")
    df_silver.select("tconst", "primaryTitle", "isAdult", "genres_list").show(5, truncate=False)

    spark.stop()
    print("\nTeste concluído com sucesso!")

if __name__ == "__main__":
    main()