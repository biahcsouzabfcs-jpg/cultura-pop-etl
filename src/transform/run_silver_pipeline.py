import sys
from pathlib import Path
from pyspark.sql import SparkSession

# Garante a importação dos módulos locais
CURRENT_DIR = Path(__file__).resolve().parent
sys.path.append(str(CURRENT_DIR))

from schemas import SCHEMA_BASICS_RAW, SCHEMA_RATINGS_RAW, SCHEMA_EPISODES_RAW
from silver_series import clean_series_basics, identify_last_episodes, join_series_ratings


def main():
    print("Inicializando SparkSession...")
    spark = (
        SparkSession.builder
        .appName("IMDbSilverPipeline")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    samples_dir = CURRENT_DIR.parent.parent / "data" / "samples"
    output_dir = CURRENT_DIR.parent.parent / "data" / "silver"

    # 1. Leitura com Schema Enforcement
    print("Lendo arquivos da Camada Bronze (Amostras)...")
    df_basics = spark.read.option("sep", "\t").option("header", "true").option("nullValue", "\\N").schema(SCHEMA_BASICS_RAW).csv(str(samples_dir / "sample_basics.tsv"))
    df_ratings = spark.read.option("sep", "\t").option("header", "true").option("nullValue", "\\N").schema(SCHEMA_RATINGS_RAW).csv(str(samples_dir / "sample_ratings.tsv"))
    df_episodes = spark.read.option("sep", "\t").option("header", "true").option("nullValue", "\\N").schema(SCHEMA_EPISODES_RAW).csv(str(samples_dir / "sample_episodes.tsv"))

    # 2. Execução das Transformações Modulares
    print("Aplicando transformações da Camada Silver...")
    df_series_clean = clean_series_basics(df_basics)
    df_episodes_analyzed = identify_last_episodes(df_episodes)

    # 3. Junção Analítica (usando min_votes=0 para funcionar na amostra pequena)
    df_final_analytics = join_series_ratings(
        df_series=df_series_clean,
        df_episodes=df_episodes_analyzed,
        df_ratings=df_ratings,
        min_votes=0
    )

    print("\n=== Schema da Camada Silver Gerada ===")
    df_final_analytics.printSchema()

    print("\n=== Resultado da Transformação (Amostra) ===")
    df_final_analytics.show(truncate=False)

    # 4. Escrita em Formato Colunar Otimizado (Parquet)
    silver_target_path = output_dir / "series_finale_analytics.parquet"
    print(f"\nPersistindo dados em formato Parquet em: {silver_target_path}...")
    
    (
        df_final_analytics
        .write
        .mode("overwrite")
        .format("parquet")
        .save(str(silver_target_path))
    )

    print("Pipeline Silver executado e persistido com sucesso!")
    spark.stop()


if __name__ == "__main__":
    main()