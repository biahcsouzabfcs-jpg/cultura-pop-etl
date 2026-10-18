"""
Módulo de Transformação Silver para Séries de TV e Avaliações.
Filtra produções de TV, calcula métricas e identifica episódios finais.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def clean_series_basics(df_basics: DataFrame) -> DataFrame:
    """Filtra apenas produções do tipo tvSeries e trata tipos básicos."""
    return (
        df_basics
        .filter(F.col("titleType") == "tvSeries")
        .withColumn("isAdult", F.col("isAdult") == 1)
        .withColumn("genres_list", F.split(F.col("genres"), ","))
        .select(
            F.col("tconst").alias("series_id"),
            F.col("primaryTitle").alias("series_title"),
            "startYear",
            "endYear",
            "genres_list"
        )
    )


def identify_last_episodes(df_episodes: DataFrame) -> DataFrame:
    """
    Utiliza Window Functions para numerar os episódios de trás para a frente.
    O registo com episode_rank == 1 corresponde ao episódio final da série.
    """
    # Janela particionada por série, ordenada por temporada e número de episódio descendente
    window_spec = (
        Window
        .partitionBy("parentTconst")
        .orderBy(
            F.col("seasonNumber").desc_nulls_last(),
            F.col("episodeNumber").desc_nulls_last()
        )
    )

    return (
        df_episodes
        .filter(F.col("seasonNumber").isNotNull() & F.col("episodeNumber").isNotNull())
        .withColumn("episode_rank", F.row_number().over(window_spec))
        .withColumn("is_series_finale", F.col("episode_rank") == 1)
        .select(
            F.col("tconst").alias("episode_id"),
            F.col("parentTconst").alias("series_id"),
            "seasonNumber",
            "episodeNumber",
            "is_series_finale"
        )
    )


def join_series_ratings(
    df_series: DataFrame,
    df_episodes: DataFrame,
    df_ratings: DataFrame,
    min_votes: int = 1000
) -> DataFrame:
    """
    Realiza as junções e calcula a diferença de avaliação entre a série e o final.
    """
    # 1. Obter a avaliação geral da série mãe
    series_with_ratings = (
        df_series
        .join(df_ratings, df_series.series_id == df_ratings.tconst, how="inner")
        .filter(F.col("numVotes") >= min_votes)
        .select(
            "series_id",
            "series_title",
            "startYear",
            F.col("averageRating").alias("series_avg_rating"),
            F.col("numVotes").alias("series_votes")
        )
    )

    # 2. Filtrar apenas os episódios finais com as suas respetivas avaliações
    final_episodes = (
        df_episodes
        .filter(F.col("is_series_finale") == True)
        .join(df_ratings, df_episodes.episode_id == df_ratings.tconst, how="inner")
        .select(
            "series_id",
            F.col("episode_id").alias("finale_episode_id"),
            "seasonNumber",
            "episodeNumber",
            F.col("averageRating").alias("finale_rating"),
            F.col("numVotes").alias("finale_votes")
        )
    )

    # 3. Cruzar a avaliação da série com a avaliação do episódio final
    return (
        series_with_ratings
        .join(final_episodes, on="series_id", how="inner")
        .withColumn(
            "rating_drop",
            F.round(F.col("finale_rating") - F.col("series_avg_rating"), 2)
        )
    )