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

### Camada Silver

Aqui foram excluídos dados duplicados, alterados os nomes das colunas, transformados os valores de colunas de string para decimal e outros tratamentos que podem ser observados nos notebooks.



