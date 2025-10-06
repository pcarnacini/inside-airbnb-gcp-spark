# Projeto de Análise de Dados "Inside Airbnb" com Spark e Google Cloud

## 1. Resumo do Projeto
Este projeto implementa um pipeline de dados ponta-a-ponta, escalável e nativo em nuvem, para processar e analisar os dados públicos do **Inside Airbnb**. A solução foi arquitetada utilizando tecnologias de Big Data no **Google Cloud Platform (GCP)**, com **Apache Spark** como motor de processamento distribuído.

O objetivo principal foi evoluir de um pipeline baseado em scripts locais (Python/Pandas) e Hive para uma arquitetura moderna que utiliza o poder do processamento distribuído no **Google Dataproc**, armazenamento em um Data Lake no **Google Cloud Storage (GCS)** e consultas analíticas performáticas com a API de DataFrames do PySpark.

## 2. Fontes de Dados
- **Inside Airbnb (dados abertos):**
  - `listings.csv.gz` → Informações detalhadas sobre os anúncios, anfitriões e propriedades.
  - `calendar.csv.gz` → Disponibilidade diária e preços para cada anúncio ao longo do ano.
  - `reviews.csv.gz` → Avaliações textuais e de nota deixadas pelos hóspedes.

- **Configuração:**
  - O arquivo `config/cities.yaml` centraliza a definição das cidades, snapshots de data e URLs base para a extração dos dados, tornando o pipeline flexível e fácil de estender para novas localidades.

## 3. Arquitetura da Solução no GCP

A arquitetura foi desenhada para ser robusta, escalável e gerenciada, aproveitando os principais serviços do Google Cloud para engenharia de dados.

**Tecnologias Core:**
* **Google Cloud Storage (GCS):** Atua como nosso Data Lake central, armazenando o código-fonte, os dados processados em formato Parquet e os resultados das análises.
* **Google Dataproc:** Fornece clusters Spark efêmeros e gerenciados, permitindo que o poder de processamento seja provisionado sob demanda apenas quando o pipeline está em execução.
* **Apache Spark:** É o coração do processamento. Usamos PySpark para orquestrar os jobs de ETL e análise, aproveitando sua capacidade de processamento distribuído em memória.
* **Hive Metastore:** Integrado ao Dataproc, gerencia os metadados das nossas tabelas, permitindo que os arquivos Parquet no GCS sejam consultados como se fossem tabelas de um banco de dados relacional.

**Fluxo de Dados:**

```
1. Orquestração (Shell Script Local)
     |
     v
2. Upload de Código (main_etl.py, run_spark_analyses.py) --> GCS (Bucket de Código)
     |
     v
3. Submissão de Jobs --> Google Dataproc (Cluster Spark)
     |
     +--> Job 1: ETL (main_etl.py)
     |      |
     |      +--> E: Baixa dados da Web
     |      +--> T: Transforma com Spark
     |      +--> L: Salva em Parquet no GCS e registra no Hive Metastore
     |
     +--> Job 2: Análise (run_spark_analyses.py)
            |
            +--> Lê tabelas do Hive Metastore (dados no GCS)
            +--> Executa análises com DataFrame API
            +--> Salva resultados em Parquet no GCS
```

## 4. Pipeline de Dados

O pipeline é orquestrado pelo script `run_spark_pipeline.sh` e consiste em dois jobs Spark principais:

### Job 1: ETL (`main_etl.py`)
Este job consolida as etapas de Extração, Transformação e Carga.
- **Extração (Extract):** O driver Spark baixa os arquivos `.csv.gz` da web, descomprime-os em memória e os distribui para os nós do cluster para serem paralelizados em DataFrames.
- **Transformação (Transform):** Toda a lógica de limpeza de dados (preços, textos, datas), conversão de tipos e enriquecimento (adição de colunas de partição) é realizada de forma distribuída utilizando a API de DataFrames do Spark, substituindo o Pandas para ganho de escala.
- **Carga (Load):** Os DataFrames transformados são salvos no Google Cloud Storage em formato **Parquet**, um formato colunar otimizado que oferece compressão superior e performance de leitura muito mais rápida para consultas analíticas. As partições (`city`, `snapshot_date`) são criadas automaticamente e as tabelas são registradas no Hive Metastore.

### Job 2: Análise (`run_spark_analyses.py`)
Este job é responsável por executar as consultas analíticas sobre os dados já processados.
- **Leitura:** Carrega as tabelas (`listings`, `calendar`, `reviews`) a partir do Hive Metastore.
- **Análise:** Em vez de usar um arquivo `.sql`, este script executa uma série de funções em Python, onde cada função implementa uma consulta de negócio complexa usando a **API de DataFrames do Spark**. Essa abordagem oferece maior flexibilidade, testabilidade e integração com o ecossistema Python.
- **Salvamento:** O resultado de cada análise é salvo em um diretório específico no GCS, também em formato Parquet, pronto para ser consumido por ferramentas de BI ou para análises mais aprofundadas.

## 5. Análises Realizadas
O pipeline responde a diversas perguntas de negócio, incluindo duas novas análises estratégicas:

- **(NOVA) Análise de Hosts Veteranos:** Quem são os anfitriões mais antigos e quantos anúncios eles gerenciam?
- **(NOVA) Análise de Sazonalidade de Preços:** Como o preço médio das diárias varia mês a mês ao longo do ano?
- **Preço Médio por Bairro e Tipo de Quarto:** Quais são as áreas e tipos de acomodação mais caros?
- **Disponibilidade Diária:** Qual o percentual de imóveis disponíveis em um determinado período?
- **Impacto dos Superhosts:** Superhosts realmente têm melhores avaliações e preços diferentes?
- **Estimativa de Taxa de Ocupação:** Quais listings são mais reservados?
- **E muitas outras...** (Top anfitriões, distribuição de preço por tipo de propriedade, etc.)

## 6. Estrutura do Projeto
A estrutura de arquivos foi simplificada para refletir a abordagem baseada em jobs Spark.

```
/inside_airbnb_spark_gcp
|
├── config/
│   └── cities.yaml              # Arquivo de configuração de cidades e URLs
|
├── main_etl.py                    # Job Spark para Extração, Transformação e Carga (ETL)
├── run_spark_analyses.py          # Job Spark para executar todas as análises
|
├── run_spark_pipeline.sh          # Script principal para orquestrar os jobs no Dataproc
└── README.md                      # Esta documentação
```

## 7. Como Replicar o Projeto

Siga os passos abaixo para executar o pipeline completo no seu próprio ambiente Google Cloud.

### Pré-requisitos
1.  Uma conta Google Cloud com um projeto ativo e faturamento habilitado.
2.  O [Google Cloud SDK (`gcloud`)](https://cloud.google.com/sdk/docs/install) instalado e autenticado (`gcloud auth login`).
3.  Um bucket no Google Cloud Storage (GCS).

### Passo 1: Configurar o Ambiente
1.  Clone este repositório para sua máquina local.
2.  Abra o arquivo `run_spark_pipeline.sh` e atualize as seguintes variáveis com seus próprios valores:
    ```bash
    REGION="us-central1" # Ou sua região de preferência
    CLUSTER="airbnb-spark-cluster"
    BUCKET_NAME="seu-nome-de-bucket-aqui"
    PROJECT_ID="seu-project-id-aqui"
    ```

### Passo 2: Criar o Cluster Dataproc
Antes de rodar o pipeline, você precisa de um cluster Spark ativo. Execute o comando abaixo no seu terminal. **Este é um passo único de setup.**

```bash
gcloud dataproc clusters create airbnb-spark-cluster \
  --project=seu-project-id-aqui \
  --region=us-central1 \
  --image-version=2.1-debian11 \
  --master-machine-type=n1-standard-4 \
  --worker-machine-type=n1-standard-4 \
  --num-workers=2 \
  --enable-component-gateway \
  --optional-components=JUPYTER
```
*Aguarde alguns minutos até que o cluster esteja com o status `RUNNING`.*

### Passo 3: Executar o Pipeline Completo
Com o cluster ativo, execute o script principal. Ele cuidará de todo o resto.

```bash
bash run_spark_pipeline.sh
```
Este comando irá:
1.  Fazer o upload dos scripts PySpark para seu bucket no GCS.
2.  Submeter o job de ETL (`main_etl.py`) para o cluster.
3.  Após a conclusão do ETL, submeter o job de análise (`run_spark_analyses.py`).

### Passo 4: Verificar os Resultados
Após a execução, você pode encontrar os artefatos nos seguintes locais do seu bucket GCS:
- **Dados Processados:** `gs://<seu-bucket>/processed/` (em formato Parquet, particionado)
- **Resultados das Análises:** `gs://<seu-bucket>/results/` (cada análise em sua própria pasta)

## 8. Próximos Passos e Melhorias
- **Orquestração com Cloud Composer (Airflow):** Substituir o `run_spark_pipeline.sh` por um DAG no Airflow para ter agendamento, retentativas e monitoramento robustos.
- **Uso de Dataproc Serverless:** Migrar os jobs para o Dataproc Serverless para eliminar a necessidade de gerenciar a infraestrutura do cluster.
- **Visualização de Dados:** Conectar os resultados salvos no GCS a uma ferramenta de BI como o **Looker Studio** para criar dashboards interativos.
- **Qualidade de Dados:** Implementar um passo de verificação de qualidade dos dados no pipeline usando bibliotecas como `pydeequ`.
- **CI/CD:** Automatizar o deploy de novas versões do pipeline usando Cloud Build e GitHub Actions.