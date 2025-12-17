DROP TABLE IF EXISTS movimento;
DROP TABLE IF EXISTS cartao;
DROP TABLE IF EXISTS conta;
DROP TABLE IF EXISTS associado;

CREATE TABLE associado (
  id INT PRIMARY KEY,
  nome VARCHAR(100),
  sobrenome VARCHAR(100),
  idade INT,
  email VARCHAR(100)
);

CREATE TABLE conta (
  id INT PRIMARY KEY,
  tipo VARCHAR(50),
  data_criacao TIMESTAMP,
  id_associado INT REFERENCES associado(id)
);

CREATE TABLE cartao (
  id INT PRIMARY KEY,
  num_cartao BIGINT,
  nom_impresso VARCHAR(100),
  id_conta INT REFERENCES conta(id),
  id_associado INT REFERENCES associado(id)
);

CREATE TABLE movimento (
  id INT PRIMARY KEY,
  vlr_transacao DECIMAL(10,2),
  des_transacao VARCHAR(200),
  data_movimento TIMESTAMP,
  id_cartao INT REFERENCES cartao(id)
);
