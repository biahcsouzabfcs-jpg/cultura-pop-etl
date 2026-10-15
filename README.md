Pipeline de Engenharia de Dados para análise de séries e comportamento de audiência vs. crítica utilizando dados abertos do IMDb.

## Arquitetura (Lakehouse)
- **Bronze (Raw):** Ingestão dos arquivos compactados (.tsv.gz) no Google Cloud Storage (GCS).
- **Silver:** Limpeza, tipagem estrita e particionamento em Parquet com PySpark.
- **Gold:** Tabelas analíticas para consulta via BigQuery.
