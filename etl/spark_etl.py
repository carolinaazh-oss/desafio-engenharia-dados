from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

OUTPUT_DIR = "output"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "movimento_flat.csv")

spark = (
    SparkSession.builder
    .appName("movimento-flat-etl")
    .master("local[*]")
    .getOrCreate()
)

jdbc_url = "jdbc:postgresql://localhost:5432/sicooperative"
jdbc_props = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

associado = spark.read.jdbc(jdbc_url, "associado", properties=jdbc_props)
conta     = spark.read.jdbc(jdbc_url, "conta", properties=jdbc_props)
cartao    = spark.read.jdbc(jdbc_url, "cartao", properties=jdbc_props)
movimento = spark.read.jdbc(jdbc_url, "movimento", properties=jdbc_props)

df = (
    movimento
    .join(cartao, movimento.id_cartao == cartao.id, "inner")
    .join(conta, cartao.id_conta == conta.id, "inner")
    .join(associado, cartao.id_associado == associado.id, "inner")
)

movimento_flat = df.select(
    col("associado.nome").alias("nome_associado"),
    col("associado.sobrenome").alias("sobrenome_associado"),
    col("associado.idade").cast("string").alias("idade_associado"),
    col("movimento.vl_transacao").cast("string").alias("vlr_transacao_movimento"),
    col("movimento.des_transacao").alias("des_transacao_movimento"),
    col("movimento.data_movimento").cast("string").alias("data_movimento"),
    col("cartao.num_cartao").alias("numero_cartao"),
    col("cartao.nome_impresso").alias("nome_impresso_cartao"),
    col("conta.data_criacao").cast("string").alias("data_criacao_cartao"),
    col("conta.tipo_conta").alias("tipo_conta"),
    col("conta.data_criacao").cast("string").alias("data_criacao_conta"),
)

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Converter para pandas e salvar CSV FINAL
movimento_flat.toPandas().to_csv(OUTPUT_FILE, index=False)

spark.stop()

print(f"CSV gerado com sucesso em: {OUTPUT_FILE}")