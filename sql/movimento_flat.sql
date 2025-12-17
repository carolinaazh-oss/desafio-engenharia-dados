SELECT
    a.nome::text                      AS nome_associado,
    a.sobrenome::text                 AS sobrenome_associado,
    a.idade::text                     AS idade_associado,
    m.vlr_transacao::text             AS vlr_transacao_movimento,
    m.des_transacao::text             AS des_transacao_movimento,
    m.data_movimento::text            AS data_movimento,
    c.num_cartao::text                AS numero_cartao,
    c.nom_impresso::text              AS nome_impresso_cartao,
    ct.data_criacao::text             AS data_criacao_cartao,
    ct.tipo::text                     AS tipo_conta,
    ct.data_criacao::text             AS data_criacao_conta
FROM movimento m
JOIN cartao c     ON m.id_cartao = c.id
JOIN conta ct     ON c.id_conta = ct.id
JOIN associado a  ON ct.id_associado = a.id;
