-- associados
INSERT INTO associado VALUES
(1, 'Ana', 'Silva', 29, 'ana.silva@email.com'),
(2, 'Bruno', 'Souza', 42, 'bruno.souza@email.com');

-- contas 
INSERT INTO conta VALUES
(10, 'corrente', '2023-01-10 10:00:00', 1),
(11, 'poupanca', '2023-02-15 09:30:00', 1),
(20, 'corrente', '2023-03-20 14:45:00', 2);

-- cartoes 
INSERT INTO cartao VALUES
(100, 1111222233334444, 'ANA SILVA', 10, 1),
(101, 5555666677778888, 'ANA SILVA', 11, 1),
(200, 9999000011112222, 'BRUNO SOUZA', 20, 2);

-- movimentos 
INSERT INTO movimento VALUES
(1000, 120.50, 'Supermercado', '2024-01-05 18:20:00', 100),
(1001, 35.90,  'Restaurante',  '2024-01-06 12:10:00', 100),
(1002, 250.00, 'Eletronicos',  '2024-01-07 20:30:00', 101),
(2000, 89.90,  'Farmacia',     '2024-01-08 09:00:00', 200);
