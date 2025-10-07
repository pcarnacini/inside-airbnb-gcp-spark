# Projeto de Análise de Dados "Inside Airbnb" com Spark e Google Cloud

## 1. Resumo do Projeto
Este projeto implementa um pipeline de dados ponta-a-ponta, escalável e nativo em nuvem, para processar e analisar os dados públicos do **Inside Airbnb**. A solução foi arquitetada utilizando tecnologias de Big Data no **Google Cloud Platform (GCP)**, com **Apache Spark** como motor de processamento distribuído.

O objetivo foi evoluir de um processo ETL tradicional para uma arquitetura moderna que utiliza o poder do processamento distribuído no **Google Dataproc**, armazenamento em um Data Lake no **Google Cloud Storage (GCS)** e consultas analíticas performáticas com a API de DataFrames do PySpark.

## 2. Fontes de Dados
- **Inside Airbnb (dados abertos):**
  - `listings.csv.gz` → Informações detalhadas sobre os anúncios, anfitriões e propriedades.
  - `calendar.csv.gz` → Disponibilidade diária e preços para cada anúncio ao longo do ano.
  - `reviews.csv.gz` → Avaliações textuais e de nota deixadas pelos hóspedes.

- **Configuração:**
  - O arquivo `config/cities.yaml` centraliza a definição das cidades, snapshots de data e URLs para a extração dos dados.

## 3. Arquitetura da Solução no GCP

A arquitetura foi desenhada para ser robusta, escalável e gerenciada, aproveitando os principais serviços do Google Cloud para engenharia de dados.

**Tecnologias Core:**
* **Google Cloud Storage (GCS):** Atua como nosso Data Lake central, armazenando o código-fonte, os dados processados em formato Parquet e os resultados das análises.
* **Google Dataproc:** Fornece clusters Spark efêmeros e gerenciados, permitindo que o poder de processamento seja provisionado sob demanda.
* **Apache Spark (PySpark):** É o coração do processamento. Usamos PySpark para orquestrar os jobs de ETL e análise.
* **Hive Metastore:** Integrado ao Dataproc, gerencia os metadados das nossas tabelas, permitindo que os arquivos Parquet no GCS sejam consultados como tabelas de um banco de dados relacional.

**Fluxo de Dados:**
```
1. Orquestração (Shell Script Local)
     |
     v
2. Upload de Código (main_etl.py, etc.) --> GCS (Bucket de Código)
     |
     v
3. Submissão de Jobs --> Google Dataproc (Cluster Spark)
     |
     +--> Job 1: ETL (main_etl.py)
     |      |
     |      +--> E: Baixa dados da Web
     |      +--> T: Transforma com PySpark
     |      +--> L: Salva em Parquet no GCS e registra no Hive Metastore
     |
     +--> Job 2: Análise (run_spark_analyses.py)
            |
            +--> Lê tabelas do Hive Metastore
            +--> Executa análises com DataFrame API
            +--> Salva resultados em Parquet no GCS
```

## 4. Análises e Resultados
O pipeline responde a diversas perguntas de negócio. Após a execução, os resultados são salvos no GCS e podem ser facilmente lidos e analisados, como mostram os exemplos abaixo (dados para o Rio de Janeiro).

### Análise 1: Hosts com Mais Reviews
**Pergunta:** Quem são os anfitriões que mais recebem avaliações e quantos imóveis eles gerenciam?
**Insight:** Os dados mostram uma clara profissionalização da plataforma. Hosts como "Omar Do Rio" não são pessoas alugando um quarto extra, mas sim empresas ou indivíduos gerenciando centenas de propriedades, concentrando milhares de reviews.

| host_id   | host_name       | total_reviews | total_listings |
|:----------|:----------------|:--------------|:---------------|
| 6000862   | Omar Do Rio     | 5684          | 214            |
| 310319158 | Suellen         | 4527          | 122            |
| 74463624  | Mozart          | 4155          | 86             |
| 46664224  | Rafael          | 4048          | 52             |
| 2513825   | Newton          | 3576          | 65             |
| 36744189  | Monica          | 3466          | 41             |
| 15533343  | Yuri            | 3325          | 44             |
| 1982737   | Estadia         | 2806          | 124            |
| 347487511 | Rodrigo         | 2710          | 79             |
| 29475411  | Bruno & Ricardo | 2571          | 30             |

### Análise 2: Preço Médio por Bairro e Tipo de Quarto
**Pergunta:** Quais são as combinações de bairro e tipo de acomodação mais caras no Rio de Janeiro?
**Insight:** A análise revela alguns possíveis outliers (como um "Shared room" de R$25.000, que mereceria uma investigação), mas também confirma tendências esperadas. Bairros nobres como Joá, São Conrado, Leblon e Ipanema dominam o topo da lista para "Entire home/apt", com preços médios significativamente elevados.

| room_type       | neighbourhood_cleansed | avg_price | total_listings |
|:----------------|:-----------------------|:----------|:---------------|
| Shared room     | São Conrado            | 25000.00  | 1              |
| Entire home/apt | Estácio                | 13113.97  | 39             |
| Hotel room      | Copacabana             | 12688.00  | 2              |
| Entire home/apt | Joá                    | 7194.78   | 135            |
| Entire home/apt | Coelho Neto            | 5680.00   | 2              |
| Entire home/apt | São Conrado            | 4427.25   | 200            |
| Private room    | Itanhangá              | 2696.15   | 66             |
| Entire home/apt | Itanhangá              | 2624.72   | 101            |
| Entire home/apt | Alto da Boa Vista      | 2378.50   | 32             |
| Entire home/apt | Leblon                 | 1053.87   | 1712           |
| Entire home/apt | Ipanema                | 1040.19   | 3231           |

... e muitas outras análises, como sazonalidade de preços, impacto dos Superhosts, etc.

## 5. Como Visualizar os Resultados
Após a execução do pipeline, os resultados de cada análise são salvos em formato Parquet no seu bucket do GCS, no diretório `/results/`.

A maneira mais fácil de visualizar esses dados é usando um notebook, como o **Google Colab**. O código abaixo conecta-se ao seu GCS, lê o resultado de uma análise específica e o exibe em um DataFrame do Pandas.

### Notebook no Google Colab
Abra um novo notebook no [Google Colab](https://colab.research.google.com/) e execute as células abaixo:

**Célula 1: Autenticação**
```python
from google.colab import auth
auth.authenticate_user()
print('Authenticated successfully!')
```

**Célula 2: Instalação de Bibliotecas**
```python
!pip install -q gcsfs pyarrow
print('Libraries installed!')
```

**Célula 3: Leitura e Visualização dos Dados**
```python
import pandas as pd

# 1. ATUALIZE COM O NOME DO SEU BUCKET
BUCKET_NAME = 'airbnb-data-pedro'

# 2. ESCOLHA QUAL ANÁLISE VOCÊ QUER VER
#    (o nome deve ser o mesmo da pasta criada no GCS)
analysis_name = 'most_reviewed_hosts' # Ex: 'price_seasonality_analysis', 'top_listings_by_reviews', etc.

# Constrói o caminho completo para os dados no GCS
gcs_path = f'gs://{BUCKET_NAME}/results/{analysis_name}'
print(f"Reading data from: {gcs_path}")

# Lê os arquivos Parquet diretamente para um DataFrame Pandas
try:
    df = pd.read_parquet(gcs_path)

    # Mostra o DataFrame!
    print(f"\nDisplaying results for: {analysis_name}")
    display(df)

except Exception as e:
    print(f"\nAn error occurred: {e}")
    print("Please check if the bucket name and analysis name are correct.")
```

## 6. Como Replicar o Projeto

### Pré-requisitos
1.  Uma conta Google Cloud com um projeto ativo e faturamento habilitado.
2.  O [Google Cloud SDK (`gcloud`)](https://cloud.google.com/sdk/docs/install) instalado e autenticado.
3.  Um bucket no Google Cloud Storage (GCS).

### Passo 1: Configurar o Ambiente
1.  Clone este repositório.
2.  Abra o arquivo `run_spark_pipeline.sh` e atualize as variáveis `REGION`, `CLUSTER`, `BUCKET_NAME` e `PROJECT_ID` com seus próprios valores.

### Passo 2: Criar o Cluster Dataproc
Execute o comando abaixo no seu terminal para criar o cluster. **Este é um passo único de setup.**
```bash
gcloud dataproc clusters create airbnb-spark-cluster \
  --project=seu-project-id-aqui \
  --region=us-central1 \
  --image-version=2.1-debian11 \
  --master-machine-type=n1-standard-4 \
  --worker-machine-type=n1-standard-4 \
  --num-workers=2 \
  --enable-component-gateway
```
*Aguarde alguns minutos até que o cluster esteja com o status `RUNNING`.*

### Passo 3: Executar o Pipeline Completo
Com o cluster ativo, execute o script principal.
```bash
bash run_spark_pipeline.sh
```
Isso irá executar o job de ETL e, em seguida, o job de análises.

## 7. Próximos Passos
- **Orquestração com Cloud Composer (Airflow):** Substituir o `run_spark_pipeline.sh` por um DAG no Airflow para ter agendamento, retentativas e monitoramento robustos.
- **Uso de Dataproc Serverless:** Migrar os jobs para o Dataproc Serverless para eliminar a necessidade de gerenciar a infraestrutura do cluster.
- **Visualização de Dados:** Conectar os resultados salvos no GCS a uma ferramenta de BI como o **Looker Studio** ou o **Power BI** para criar dashboards interativos.