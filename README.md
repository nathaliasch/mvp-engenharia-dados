# Objetivo: construir um pipeline de dados (busca, coleta, modelagem, carga e análise) utilizando tecnologias na nuvem.

## Optei por utilizar o Microsoft Azure. E a partir de dados sobre exportação extraídos do Kaggle será analisado:

### 1. Evolução das exportações ao longo do tempo.
### 2. Qual a principal mercadorias exportada.
### 3. Quais as vias mais utilizadas para exportação e importação no país.
### 4. Quas as unidades federattivas mais ativas

# Detalhamento

## Busca pelos dados

https://www.kaggle.com/datasets/daniellecd/dados-de-exportao-e-importao-do-brasil

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/0372ccae-366e-47c0-89e5-2466df619a21)

## Coleta

Alguns dados foram baixados para a máquina local e inseridos manualmente a partir da criação de um container dentro da conta de armazenamento:

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/8c10db03-e957-43a5-b627-3858adc4df04)

Foi feito o upload dos arquivos no formato .csv para o BLOB STORAGE:

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/e5b933df-015a-48c6-a051-6a777c0bc5da)

Foi criado um BANCO DE DADOS SQL:

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/2c754312-fe78-44cb-aac8-9770f0aca3f4)

E também o DATA FACTORY:

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/2d78fdc9-667a-4bbc-91bc-a4a08b840af3)

No Data Factory criei os conjuntos de dados, o fluxo de dados e o pipeline:

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/f00766c5-85ef-4b20-9c5d-f6286293a169)

## Modelagem

No ambiente do Databricks, com base na arquitetura medalhão, criei os schemas bronze, silver e gold para organizar os dados e melhorar incremental e progressiva a estrutura e a qualidade destes no Lakehouse.

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/028392e5-4f58-4dc9-9f54-a139b4dc5360)

### Camada Bronze

Nessa camada os dados estão brutos.

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/dedc5ab9-6bbf-4458-b089-822a210e0c5c)

### Camada Silver

Aqui foram excluídos os dados duplicados que foram encontrados em algumas tabelas, alterados os nomes das colunas, transformadas as colunas de valor de string para decimal e outros tratamentos que podem ser observados nos notebooks que foram anexados. 

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/3b65401b-dd6f-481f-99b6-a50cb93e36ae)

Neste exemplo aqui foram excluídas as colunas no idioma espanhol e inglês da tabela CO_NCM.

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/6ba6578c-28e5-4d33-bfae-9a8224b8f621)

### Camada Gold

Para adotar o esquema estrela aqui foram criadas as tabelas dimensões a partir das tabelas CO_NCM, CO_PAIS, CO_UNID, CO_URF, CO_VIA e NCM_UNIDADE. 

Foi criada também a tabela fato f_exportacao_importacao_gold a partir do UNION ALL das tabelas imp_completa_silver e exp_completa_silver.

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/3a0c1724-9595-4c9f-9d29-aca6f4dd11c0)

## Análise dos dados

Após todo o processo de ETL o objetivo foi criar um dashboard no Power BI para responder os questionamentos.

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/ba96276b-2280-4922-87bb-5e92bff318d2)

Foram feitos os relacionamentos.


![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/9ac61bb9-35a7-488d-b96b-cc03ca70c612)

E criado o dashboard.

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/60223a78-f0ab-4ba0-9c24-8843374693e4)




