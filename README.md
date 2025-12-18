# Desafio Técnico – Engenharia de Dados

Este projeto implementa um **pipeline de ETL (Extract, Transform, Load)** utilizando **Apache Spark (PySpark)** para processar dados relacionais armazenados em **PostgreSQL**. O objetivo é gerar o arquivo **`movimento_flat.csv`**, consolidando informações de **movimentos financeiros**, **cartões**, **contas** e **associados**, conforme especificação do desafio técnico.

A solução foi desenvolvida utilizando **PostgreSQL** como banco transacional, **Apache Spark** para processamento e transformação dos dados (joins distribuídos), **Docker Compose** para orquestração do ambiente e **GitHub Codespaces** como ambiente de desenvolvimento. O uso do Spark permite aplicar conceitos reais de **ETL**, **joins** e geração de **dataset flat**, simulando um cenário prático de engenharia de dados.

---

## Estrutura do Projeto

    docker-compose.yml
    etl/
      pipeline_movimento_flat.py
    sql/
      01_schema.sql
      02_insert.sql
    jars/
      postgresql-42.7.3.jar
    output/
      movimento_flat.csv
    README.md

---

## Modelo de Dados

O modelo relacional é composto pelas tabelas:

- `associado`
- `conta`
- `cartao`
- `movimento`

O Spark lê os dados do PostgreSQL via **JDBC**, realiza os **joins** entre as entidades e persiste o resultado final em formato **CSV** no diretório `output`.

---

## Ambiente e Orquestração

O **Docker Compose** é utilizado para:

- Subir o PostgreSQL com **um único comando**
- Garantir **padronização** e **reprodutibilidade** do ambiente
- Executar automaticamente os scripts de criação e carga inicial do banco

O Spark é executado como **job sob demanda**, padrão utilizado em pipelines de dados modernos.

---

## Passo a passo para execução do projeto

### 1) Subir o banco PostgreSQL (Docker Compose)

Cria e inicializa o banco **`desafio`**, executando automaticamente os scripts `sql/01_schema.sql` e `sql/02_insert.sql`.

    docker compose up -d
    docker compose ps

Opcional: validar tabelas criadas.

    docker exec -it desafio-postgres psql -U desafio -d desafio -c "\dt"

---

### 2) Garantir o driver JDBC do PostgreSQL

Baixe o driver JDBC para permitir que o Spark conecte no Postgres via JDBC.

    mkdir -p jars
    curl -L -o jars/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

---

### 3) Executar o ETL com Spark (PySpark)

Executa o job `etl/pipeline_movimento_flat.py`, realiza a leitura via JDBC, aplica os joins e grava o arquivo CSV flat no diretório `output`.

    docker run --rm \
      --network desafio-engenharia-dados_default \
      -v $(pwd)/etl:/home/jovyan/work \
      -v $(pwd)/output:/home/jovyan/output \
      -v $(pwd)/jars:/home/jovyan/jars \
      -e POSTGRES_HOST=desafio-postgres \
      -e POSTGRES_PORT=5432 \
      -e POSTGRES_DB=desafio \
      -e POSTGRES_USER=desafio \
      -e POSTGRES_PASSWORD=desafio \
      jupyter/pyspark-notebook:spark-3.4.1 \
      spark-submit \
      --master local[*] \
      --jars /home/jovyan/jars/postgresql-42.7.3.jar \
      /home/jovyan/work/pipeline_movimento_flat.py

---

### 4) Validar o resultado

Verifique se o arquivo foi gerado e visualize as primeiras linhas.

    ls -lah output
    head -n 5 output/movimento_flat.csv
