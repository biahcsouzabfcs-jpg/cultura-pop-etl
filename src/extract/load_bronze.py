"""
Módulo de Extração e Ingestão para a Camada Bronze (GCS).
Transfere os arquivos compactados oficiais do IMDb diretamente para o bucket.
"""

from datetime import datetime
import requests
from google.cloud import storage

# Identificadores do seu ambiente GCP
BUCKET_NAME = "cultura-pop-lakehouse-541778347634"

# Endpoints oficiais dos arquivos compactados
IMDB_DATASETS = {
    "title.basics.tsv.gz": "https://datasets.imdbws.com/title.basics.tsv.gz",
    "title.ratings.tsv.gz": "https://datasets.imdbws.com/title.ratings.tsv.gz",
    "title.episode.tsv.gz": "https://datasets.imdbws.com/title.episode.tsv.gz",
}


def stream_url_to_gcs(url: str, bucket_name: str, destination_blob_name: str, chunk_size: int = 10 * 1024 * 1024):
    """
    Transfere um arquivo da web diretamente para o GCS via streaming em blocos.
    chunk_size padrão: 10 MB por bloco para eficiência de rede.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    print(f"Iniciando download/upload de: {url}")
    print(f"Destino no GCS: gs://{bucket_name}/{destination_blob_name}")

    with requests.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        
        # Upload em blocos diretamente para o GCS
        with blob.open("wb", chunk_size=chunk_size) as gcs_file:
            bytes_uploaded = 0
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    gcs_file.write(chunk)
                    bytes_uploaded += len(chunk)
                    mb_uploaded = round(bytes_uploaded / (1024 * 1024), 1)
                    print(f"-> {mb_uploaded} MB transferidos...", end="\r")

    print(f"\nUpload concluído com sucesso para: {destination_blob_name} ({round(bytes_uploaded / (1024 * 1024), 2)} MB)")


def main():
    # Data de partição da ingestão
    load_date = datetime.now().strftime("%Y-%m-%d")
    print(f"=== Iniciando Carga Bronze - Partição: {load_date} ===")

    for filename, url in IMDB_DATASETS.items():
        destination_path = f"bronze/load_date={load_date}/{filename}"
        stream_url_to_gcs(
            url=url,
            bucket_name=BUCKET_NAME,
            destination_blob_name=destination_path
        )

    print("\nTodos os datasets foram carregados na Camada Bronze!")


if __name__ == "__main__":
    main()