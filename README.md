# Objetivo: construir um pipeline de dados (busca, coleta, modelagem, carga e análise) utilizando tecnologias na nuvem.

## Optei por utilizar o Microsoft Azure. E a partir de dados sobre exportação extraídos do Kaggle será analisado:

### 1. Evolução das exportações ao longo do tempo (de 2012 a 2022).
### 2. Qual a principal mercadoria exportada neste período.
### 3. Quais as vias mais utilizadas para exportação e importação no país neste período.
### 4. Quas as unidades federativas mais ativas neste período.

# Detalhamento

## Busca pelos dados

Utilizei a base de dados do Kaggle contendo informações sobre exportação e importação brasileira, detalhadas pelo Código NCM - Nomenclatura Comum Mercosul, no período de 1997 a 2022.

https://www.kaggle.com/datasets/daniellecd/dados-de-exportao-e-importao-do-brasil

Total de 8 arquivos no formato .csv:

* CO_NCM.csv
* CO_PAIS.csv
* CO_UNID.csv
* CO_URF.csv
* CO_VIA.csv
* EXP_COMPLETA.csv
* IMP_COMPLETA.csv
* NCM_UNIDADE.csv

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

E fiz a conexão com o AZURE DATA STUDIO onde eu poderia visualizar e tratar as tabelas.

## Modelagem

Optei por fazer o tratamento dos dados no ambiente do Databricks, com base na arquitetura medalhão. 
Criei os schemas bronze, silver e gold para organizar os dados e melhorar incremental e progressivamente a estrutura e a qualidade destes no Lakehouse.

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/028392e5-4f58-4dc9-9f54-a139b4dc5360)

### Camada Bronze

Nessa camada os dados estão brutos.

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/dedc5ab9-6bbf-4458-b089-822a210e0c5c)

As tabelas que serão usadas como fato possuem as seguintes colunas:

* CO_ANO: ano da operação
* CO_MES: mês da operação no formato 1 a 12
* CO_NCM: Código da Nomenclatura Comum Mercosul - Utilizada para controle e identificação das mercadorias comercializadas no Brasil e nos outros países do Mercosul (cada NCM representa um produto diferente)
* CO_UNID: Código da Unidade de Medida Estatística que é uma unidade de medida padrão para cada NCM, podendo ter valores como quilograma, metro, litro, pares, tonelada e outros.
* CO_PAIS: Código do nome do país com a qual foi realizada a operação comercial (importação ou exportação)
* SG_UF_NCM: (sigla UF origem/destino da NCM): Código da Unidade Federativa (estado) de origem (exportação) ou destino (importação) da mercadoria.
* CO_VIA: Código para identificação do meio de transporte utilizado (aéreo, marítimo, rodoviária, ferroviária e outros). Na exportação, é o método utilizado para o transporte de mercadorias entre o último local de embarque para o exterior. Na importação, configura-se através dos meios de acesso para os bens do primeiro ponto de entrada no território nacional.
* CO_URF(Unidade da Receita Federal): Código da agência responsável pela execução dos procedimentos necessários para o desembaraço aduaneiro da mercadoria importada/exportada
* QT_ESTAT: No detalhamento por NCM, cada produto tem sua unidade estatística. Grande parte dos produtos tem como unidade estatística o peso em quilogramas, mas existem outras: quilograma líquido, número (unidades), pares, dúzias, milheiro, tonelada. A tabela completa que relaciona cada NCM com sua unidade estatística pode ser encontrada em na tabela “NCM _ UNIDADE”. É importante ressaltar que não se deve somar quantidades estatísticas de NCMs que contenham unidades estatísticas diferentes.
* KG_LIQUIDO: Medida que expressa o peso líquido da mercadoria. Mesmo produtos com quantidades estatísticas diferentes do quilograma também possuem disponível a medida em quilograma, referindo-se ao peso líquido da mercadoria, ou seja, mercadoria desconsiderando embalagens, caixas ou quaisquer outros adicionais de transporte. Vale relembrar que essa informação, bem como as demais informadas nas operações de comércio exterior, é de livre preenchimento e de responsabilidade exclusiva dos operadores de comércio exterior.
* VL_FOB: O valor FOB indica o preço da mercadoria em dólares americanos (US$) sob o Incoterm FOB (Free on Board), modalidade na qual o vendedor é responsável por embarcar a mercadoria enquanto o comprador assume o pagamento do frete, seguros e demais custos pós embarque.


### Camada Silver

Aqui foram excluídos os dados duplicados que foram encontrados em algumas tabelas, alterados os nomes das colunas, transformadas as colunas de valor de string para decimal e outros tratamentos que podem ser observados nos notebooks que foram anexados. 

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/3b65401b-dd6f-481f-99b6-a50cb93e36ae)

Neste exemplo aqui foram excluídas as colunas no idioma espanhol e inglês da tabela CO_NCM, deixando apenas a coluna que trazia a nomenclatura no idioma português. A ideia foi excluir dados que não seriam utilizados a fim de melhorar a performance.

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/6ba6578c-28e5-4d33-bfae-9a8224b8f621)

### Camada Gold

Para adotar o esquema estrela foram criadas as tabelas dimensões a partir das tabelas CO_NCM, CO_PAIS, CO_UNID, CO_URF, CO_VIA e NCM_UNIDADE. 

Foi criada também a tabela fato f_exportacao_importacao_gold a partir do UNION ALL das tabelas imp_completa_silver e exp_completa_silver. Como em uma das tabelas havia colunas que não existiam na outra tabela foi necessário criar essas colunas com o valor zero.

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/3a0c1724-9595-4c9f-9d29-aca6f4dd11c0)

## Análise dos dados

Após todo o processo de ETL o objetivo foi criar um dashboard no Power BI para responder os questionamentos acima.

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/ba96276b-2280-4922-87bb-5e92bff318d2)

Foram criados os relacionamentos (1 para muitos) entre as tabelas com base nas chaves.

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/9ac61bb9-35a7-488d-b96b-cc03ca70c612)

Ao final foi criado um dashboard com as respostas. Podemos concluir que a via mais utilizada para as operações é a marítima, que o item mais exportado é o Minério de Ferro e seus concentrados,   ue a evolução e a importação tiveram uma evolução parecida no período de 2012 a 2022, no entanto, em termos de quantidade, o volume é praticamente quatro vezes maior na exportação e que o Porto de Rio Grande (RS) é o que tem a maior movimentação.

![image](https://github.com/nathaliasch/mvp-engenharia-dados/assets/108892573/60223a78-f0ab-4ba0-9c24-8843374693e4)




