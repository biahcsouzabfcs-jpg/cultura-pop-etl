import sys
from pathlib import Path
from pyspark.sql import SparkSession

CURRENT_DIR = Path(__file__).resolve().parent
sys.path.append(str(CURRENT_DIR))

from gold_analytics import build_series_drop_ranking, build_genre_performance_summary


def main():
    print("Inicializando SparkSession para processamento Gold...")
    spark = (
        SparkSession.builder
        .appName("IMDbGoldAnalytics")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    silver_path = CURRENT_DIR.parent.parent / "data" / "silver" / "series_finale_analytics.parquet"
    gold_dir = CURRENT_DIR.parent.parent / "data" / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)

    # 1. Leitura rápida da camada Silver (já em formato colunar Parquet otimizado)
    print(f"Lendo dados tratados da Silver em: {silver_path}...")
    df_silver = spark.read.parquet(str(silver_path))

    # 2. Geração da Tabela 1: Ranking das Piores Quedas (com thresholds de teste zerados para amostras)
    print("\nCalculando Tabela Gold 1: Ranking das Quedas...")
    df_ranking = build_series_drop_ranking(df_silver, min_series_votes=0, min_finale_votes=0)

    print("--- Top Resultados do Ranking ---")
    df_ranking.show(10, truncate=False)

    ranking_target = gold_dir / "gold_series_finale_ranking.parquet"
    print(f"Persistindo ranking em Parquet: {ranking_target}...")
    df_ranking.write.mode("overwrite").format("parquet").save(str(ranking_target))

    spark.stop()
    print("\nCamada Gold processada e persistida com sucesso!")


if __name__ == "__main__":
    main()