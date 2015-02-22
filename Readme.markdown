# Teste Simbiose

Python 2.7.6
Cassandra 2.1.2
ElasticSearch 1.4.4

Dependências: pip install cassandra-driver

## Screenshot

![](http://i62.tinypic.com/23nx2r.png)

## Como funciona:

1 - Criar no cassandra um keyspace chamado syncdb

2 - Dentro do syncdb criar no cassandra uma columnFamily chamada synctables

CREATE TABLE syncTables(id uuid , name_table text, last_sync timestamp, active_sync boolean, PRIMARY KEY(id));

para acionar a replicação de uma tabela basta cadastra-la nesta columnFamily e utilizar active_sync como True, False para cancelar a replicação

ex : INSERT INTO synctables (id, name_table, last_sync, active_sync) VALUES (5ac3de37-740c-4b32-b73e-9f041c394d14,'cliente',dateof(now()), True);

3 - depois crie a tabela acima no cassandra:

CREATE TABLE cliente(id uuid ,
          nome text,
          idade int,
          row_time timestamp ,
          PRIMARY KEY(id,row_time));

IMPORTANTE : é obrigatório por convenção que todas tabelas sincronizadas do cassandra, terem um campo chamado id to tipo uuid e um campo chamado row_time do tipo timestamp e serem chaves compostas.

No Elastic Search, por convenção é necessário ter um campo chamado row_time.

*** TIMESTAMP NO ELASTIC SEARCH SERA TRATADO COMO Timestamp in milliseconds

Depois destas configurações a sincronização de dados da tabela cliente esta pronta para uso.

Como executar o daemon:

python sync.py -h #PARA AJUDA

python sync.py -t 3600 # Rodará a cada 1 hora, -t recebe o tempo em segundos

Depois é só inserir dados no cassandra e elastic search.

Caso queira inserir para teste: INSERT INTO cliente (id, idade , nome , row_time ) VALUES ( 3efef09a-79c4-4380-bae5-dae7fbe19da8,23,'RAFAEL',dateof(now()));

Caso queira incluir dados de outras tabelas é só repitir o passo 2,3

OBS: É sempre necessário criar a column family no cassandra e cadastra-la na synctables para funcionar o sincronismo. 

Qualquer dúvida ou sugestão, estamos á disposição.
