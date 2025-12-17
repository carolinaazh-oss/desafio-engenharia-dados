# Desafio Técnico – Engenharia de Dados

Este projeto implementa a modelagem, carga e transformação de dados relacionais utilizando Apache Spark, conforme especificação do desafio técnico. O objetivo principal é gerar o arquivo movimento_flat.csv, consolidando informações de movimentos financeiros, cartões, contas e associados a partir de um banco PostgreSQL.

A solução foi desenvolvida utilizando PostgreSQL como banco transacional, Apache Spark para o processamento e transformação dos dados, Docker para orquestração do ambiente e GitHub Codespaces como ambiente de desenvolvimento. O uso do Spark permite aplicar conceitos de ETL, joins distribuídos e geração de datasets flat, simulando um cenário real de engenharia de dados.

Estrutura do projeto:

.
├── docker-compose.yml
├── etl
│   └── spark_etl.py
├── sql
│   ├── schema.sql
│   ├── insert.sql
│   └── movimento_flat.sql
├── output
│   └── movimento_flat.csv
└── README.md

O modelo de dados é composto pelas tabelas associado, conta, cartao e movimento. Os dados são lidos do PostgreSQL pelo Spark via JDBC, transformados através de joins entre as entidades e, ao final, persistidos em formato CSV no diretório output.

Passo a passo para execução do projeto:

1. Subir o banco de dados PostgreSQL:
docker-compose up -d

2. Criar as tabelas no banco:
docker exec -i postgres_sicooperative psql -U postgres -d sicooperative < sql/schema.sql

3. Inserir os dados de exemplo:
docker exec -i postgres_sicooperative psql -U postgres -d sicooperative < sql/insert.sql

4. Executar o job Spark para geração do dataset flat:
spark-submit etl/spark_etl.py --output output

O job Spark realiza a leitura das tabelas via JDBC, executa os joins necessários, seleciona e renomeia as colunas conforme o layout especificado e grava o resultado final no arquivo movimento_flat.csv.

Estrutura do arquivo movimento_flat.csv:

nome_associado  
sobrenome_associado  
idade_associado  
vlr_transacao_movimento  
des_transacao_movimento  
data_movimento  
numero_cartao  
nome_impresso_cartao  
data_criacao_cartao  
tipo_conta  
data_criacao_conta  

Ao final da execução, o arquivo movimento_flat.csv estará disponível na pasta output do repositório, pronto para avaliação.
