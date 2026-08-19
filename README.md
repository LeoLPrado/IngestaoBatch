# IngestaoBatch

Pipeline de ingestão batch de dados de transações, construído com **Apache Airflow**, **Apache Spark (PySpark)** e **Delta Lake**, seguindo a arquitetura em camadas (medallion) **Bronze → Silver**.

## 📌 Sobre o projeto

Este projeto simula um pipeline de dados real: arquivos CSV diários de um "sistema de transações" são ingeridos, deduplicados e mesclados incrementalmente em tabelas Delta, orquestrados por uma DAG do Airflow.

## 🏗️ Arquitetura

O projeto utiliza um **Data Lake local**, representado por uma pasta `datalake/`. Dentro dela existem três outras pastas — **source**, **bronze** e **silver** — cada uma representando uma camada do pipeline:

```
datalake/
├── source/
│   └── transaction_system/
│       ├── transaction_data_2026_08_01.csv
│       ├── transaction_data_2026_08_02.csv
│       └── ...
│
├── bronze/
│   └── transaction_data/
│       ├── _delta_log/
│       │   ├── 00000000000000000000.json
│       │   └── 00000000000000000001.json
│       ├── part-00000-xxxxxxxx.snappy.parquet
│       └── ...
│
└── silver/
    └── transaction_data/
        ├── _delta_log/
        │   ├── 00000000000000000000.json
        │   └── 00000000000000000001.json
        ├── part-00000-xxxxxxxx.snappy.parquet
        └── ...
```

- **source/**: dados brutos em **CSV** — um arquivo por dia de processamento, gerados pelo `mock_data_spliter.py`.
- **bronze/**: dados já em formato **Delta** — arquivos `.parquet` com os dados, mais a pasta `_delta_log/`, que guarda o log transacional (histórico de versões) da tabela.
- **silver/**: mesma estrutura da bronze (`.parquet` + `_delta_log/`), porém já com os dados deduplicados e mesclados.

### 🔍 Como funciona o processamento

**Source → Bronze** (`ingest_source_to_bronze`)
1. Lê o CSV do dia processado (`data_processamento`, recebido via `{{ ds }}` do Airflow) na pasta `source/`.
2. Verifica se a tabela Delta da bronze já existe.
3. **Se já existir**: primeiro apaga (`delete`) qualquer registro já gravado com aquela mesma `transaction_date` — isso torna o reprocessamento idempotente, ou seja, rodar a DAG de novo para o mesmo dia não gera duplicidade. Depois, grava o novo lote em modo `append`, com `mergeSchema` habilitado (permite que novas colunas apareçam sem quebrar o schema).
4. **Se ainda não existir** (a leitura da tabela dispara a exceção `DELTA_MISSING_DELTA_TABLE`): cria a tabela do zero em modo `overwrite`, usando o próprio CSV como base.

**Bronze → Silver** (`ingest_bronze_to_silver`)
1. Lê a tabela Delta completa da bronze.
2. Verifica se a tabela Delta da silver já existe.
3. **Se já existir**: busca a maior `transaction_date` já presente na silver e filtra da bronze **apenas os registros a partir dessa data** — assim, a cada execução, só o que é novo (ou mais recente) precisa ser processado, em vez de reprocessar todo o histórico. Em seguida, é feito um `merge` (upsert) entre silver e bronze usando `transaction_id` como chave: registros que já existem são atualizados (`whenMatchedUpdateAll`, cobrindo correções tardias) e registros novos são inseridos (`whenNotMatchedInsertAll`).
4. **Se ainda não existir**: cria a tabela do zero a partir de todos os dados da bronze, em modo `overwrite`.

## 📁 Estrutura do repositório

```
IngestaoBatch/
├── dags/
│   └── ingest_transaction_data.py   # DAG do Airflow (orquestra Source -> Bronze -> Silver)
├── scripts/
│   ├── ingest_source_to_bronze.py   # Lógica de ingestão Source -> Bronze
│   ├── ingest_bronze_to_silver.py   # Lógica de merge Bronze -> Silver
│   └── mock_data_spliter.py         # Gera CSVs diários simulados a partir do mock data
├── mock_data/
│   └── MOCK_DATA.csv                # Dataset fake de transações
└── requirements.txt
```

## 🚀 Tecnologias

- Python
- Apache Airflow (orquestração)
- Apache Spark / PySpark
- Delta Lake
- Pandas

## ⚙️ Como rodar

**1. Clone o repositório e instale as dependências**

```bash
git clone https://github.com/LeoLPrado/IngestaoBatch.git
cd IngestaoBatch
pip install -r requirements.txt
```

> O Apache Airflow não está listado no `requirements.txt` — instale-o separadamente ([guia oficial](https://airflow.apache.org/docs/apache-airflow/stable/start.html)) caso queira rodar o pipeline via DAG.

**2. Configure o caminho do data lake**

Ajuste a variável `DATALAKE_PATH` nos arquivos abaixo para o caminho local desejado:
- `dags/ingest_transaction_data.py`
- `scripts/ingest_source_to_bronze.py`
- `scripts/ingest_bronze_to_silver.py`
- `scripts/mock_data_spliter.py`

**3. Gere os dados simulados**

O script abaixo divide o `MOCK_DATA.csv` em 10 arquivos CSV, um por dia, simulando a chegada diária de dados na camada Source:

```bash
python scripts/mock_data_spliter.py
```

**4. Execute o pipeline**

- **Via Airflow**: copie `dags/ingest_transaction_data.py` para a pasta `dags` do seu Airflow e dispare a DAG `ingest transaction data`.
- **Via scripts isolados**: rode `scripts/ingest_source_to_bronze.py` seguido de `scripts/ingest_bronze_to_silver.py`.

## 📊 Dados

O `MOCK_DATA.csv` contém transações fictícias com as colunas `transaction_id`, `customer_id` e `amount`, usadas para simular a chegada diária de novos dados no data lake. Os dados mockados foram gerados no ([mockaroo](https://mockaroo.com/))

> **📝 Observação:**
> - O Airflow deste projeto foi subido em modo **standalone**, já que o repositório tem fins de **estudo e aprendizado**, não havendo necessidade de uma configuração produtiva (executor distribuído, banco de metadados externo, etc.).
> - As funções usadas na DAG (`ingest_source_to_bronze` e `ingest_bronze_to_silver`) foram escritas **diretamente dentro do arquivo da DAG**, função por função, também por motivo didático — para deixar tudo visível em um único lugar. Em um ambiente real, o ideal seria manter essas funções em um **diretório de funções separado** (ex.: `functions/` ou `utils/`), importando-as na DAG, deixando o código mais organizado e reutilizável.

## 👤 Autor

**Leonardo Lopes Prado**
- GitHub: [@LeoLPrado](https://github.com/LeoLPrado)
- LinkedIn: [linkedin.com/in/leonardo-lprado](https://linkedin.com/in/leonardo-lprado)
