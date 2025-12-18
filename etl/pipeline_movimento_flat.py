import os
import glob
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB   = os.getenv("POSTGRES_DB", "desafio")
POSTGRES_USER = os.getenv("POSTGRES_USER", "desafio")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "desafio")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPS = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASS,
    "driver": "org.postgresql.Driver",
}

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/home/jovyan/output")
FINAL_CSV_NAME = "movimento_flat.csv"


def read_table(spark: SparkSession, table: str):
    return spark.read.jdbc(url=JDBC_URL, table=table, properties=JDBC_PROPS)


def write_single_csv(df, output_dir: str, final_name: str):
    """
    Spark escreve CSV como pasta com part-*.csv.
    Aqui: coalesce(1) + rename para ficar exatamente output/movimento_flat.csv
    """
    tmp_dir = os.path.join(output_dir, "_tmp_movimento_flat")

    # Limpa diretórios anteriores
    shutil.rmtree(tmp_dir, ignore_errors=True)
    os.makedirs(output_dir, exist_ok=True)

    (df.coalesce(1)
       .write
       .mode("overwrite")
       .option("header", "true")
       .option("sep", ",")
       .csv(tmp_dir))

    part_files = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
    if not part_files:
        raise RuntimeError("Nenhum arquivo part-*.csv encontrado. Falha ao escrever CSV.")

    final_path = os.path.join(output_dir, final_name)

    # Remove arquivo final antigo, se existir
    if os.path.exists(final_path):
        os.remove(final_path)

    # Move part para nome final
    shutil.move(part_files[0], final_path)

    # Limpa lixo (_SUCCESS, crc etc.)
    shutil.rmtree(tmp_dir, ignore_errors=True)


def main():
    spark = (SparkSession.builder
             .appName("etl-movimento-flat")
             .getOrCreate())

    # Leitura (distribuída) do Postgres
    associado = read_table(spark, "associado").alias("a")
    conta     = read_table(spark, "conta").alias("ct")
    cartao    = read_table(spark, "cartao").alias("c")
    movimento = read_table(spark, "movimento").alias("m")

    # JOINs
    joined = (movimento
              .join(cartao, col("m.id_cartao") == col("c.id"), "inner")
              .join(conta, col("c.id_conta") == col("ct.id"), "inner")
              .join(associado, col("c.id_associado") == col("a.id"), "inner"))

    # Dataset final (ajuste os nomes para o “flat”)
    movimento_flat = (joined.select(
        col("a.nome").cast("string").alias("nome_associado"),
        col("a.sobrenome").cast("string").alias("sobrenome_associado"),
        col("a.idade").cast("string").alias("idade_associado"),

        col("m.vlr_transacao").cast("string").alias("vlr_transacao_movimento"),
        col("m.des_transacao").cast("string").alias("des_transacao_movimento"),
        date_format(col("m.data_movimento"), "yyyy-MM-dd HH:mm:ss").alias("data_movimento"),

        col("c.num_cartao").cast("string").alias("numero_cartao"),
        col("c.nom_impresso").cast("string").alias("nome_impresso_cartao"),

        col("ct.tipo").cast("string").alias("tipo_conta"),
        date_format(col("ct.data_criacao"), "yyyy-MM-dd HH:mm:ss").alias("data_criacao_conta"),
    ))

    # Escrita em CSV único
    write_single_csv(movimento_flat, OUTPUT_DIR, FINAL_CSV_NAME)

    spark.stop()
    print(f"✅ Gerado: {os.path.join(OUTPUT_DIR, FINAL_CSV_NAME)}")


if __name__ == "__main__":
    main()
