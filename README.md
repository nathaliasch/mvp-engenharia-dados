# Objetivo: construir um pipeline de dados (busca, coleta, modelagem, carga e análise) utilizando tecnologias na nuvem.

## Optei por utilizar o Microsoft Azure. E a partir de dados sobre exportação extraídos do Kaggle será analisado:

### 1. Evolução das exportações ao longo do tempo
### 2. Quais as principais mercadorias exportadas
### 3.

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

O passo seguinte foi a criação dos conjuntos de dados, fluxo de dados e pipeline:

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/f00766c5-85ef-4b20-9c5d-f6286293a169)


