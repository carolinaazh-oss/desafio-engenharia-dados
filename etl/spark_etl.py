from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ==============================================================================
# Spark Session (não inclui o .jar aqui porque será passado via spark-submit)
# ==============================================================================

spark = (
    SparkSession.builder
    .appName("movimento_flat_etl")
    .master("local[*]")  # Roda localmente com todos os núcleos disponíveis
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ==============================================================================
# JDBC Configuration (PostgreSQL)
# ==============================================================================

JDBC_URL = "jdbc:postgresql://localhost:5432/sicooperative"

JDBC_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"  # Deve bater com o .jar passado via spark-submit
}

# ==============================================================================
# Leitura das tabelas via JDBC
# ==============================================================================

associado = spark.read.jdbc(JDBC_URL, "associado", properties=JDBC_PROPERTIES)
conta     = spark.read.jdbc(JDBC_URL, "conta", properties=JDBC_PROPERTIES)
cartao    = spark.read.jdbc(JDBC_URL, "cartao", properties=JDBC_PROPERTIES)
movimento = spark.read.jdbc(JDBC_URL, "movimento", properties=JDBC_PROPERTIES)

# ==============================================================================
# JOIN das tabelas
# ==============================================================================

df_flat = (
    movimento.alias("m")
    .join(cartao.alias("c"), col("m.id_cartao") == col("c.id"), "inner")
    .join(conta.alias("ct"), col("c.id_conta") == col("ct.id"), "inner")
    .join(associado.alias("a"), col("ct.id_associado") == col("a.id"), "inner")
)

# ==============================================================================
# Seleção e transformação das colunas
# ==============================================================================

df_flat = df_flat.select(
    col("a.nome").cast("string").alias("nome_associado"),
    col("a.sobrenome").cast("string").alias("sobrenome_associado"),
    col("a.idade").cast("string").alias("idade_associado"),
    col("m.vlr_transacao").cast("string").alias("vlr_transacao_movimento"),
    col("m.des_transacao").cast("string").alias("des_transacao_movimento"),
    col("m.data_movimento").cast("string").alias("data_movimento"),
    col("c.num_cartao").cast("string").alias("numero_cartao"),
    col("c.nome_impresso").cast("string").alias("nome_impresso_cartao"),
    col("c.data_criacao").cast("string").alias("data_criacao_cartao"),
    col("ct.tipo").cast("string").alias("tipo_conta"),
    col("ct.data_criacao").cast("string").alias("data_criacao_conta")
)

# ==============================================================================
# Escrita do resultado em CSV (único arquivo)
# ==============================================================================

(
    df_flat
    .coalesce(1)  # Garante saída em único arquivo
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv("output/movimento_flat")
)
