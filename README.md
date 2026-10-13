# Comparação entre Spark e Flink

Testes de carga para comparar a latência dos eventos de entrada e saída de veículos em áreas de descarte irregular, gerados por pipelines implementados com Apache Spark Structured Streaming e Apache Flink, utilizando Apache Sedona para operações geoespaciais e Apache Kafka para transmissão dos dados

## Pré-requisitos

- Python 3;
- Docker Desktop com Docker Compose, em execução;
- Portas `9092` (Kafka), `8080` (Spark) e `8081` (Flink) disponíveis.

## Instalação e execução

Execute os comandos abaixo **na ordem indicada**:

```bash
# Baixar o projeto
git clone https://github.com/estayce/stream-processing.git
cd stream-processing

# Construir as imagens Docker 
docker compose -f producer/docker-compose.yml build
docker compose -f consumer_spark/docker-compose.yml build
docker compose -f consumer_flink/docker-compose.yml build

# Iniciar o Kafka e carregar as áreas
python kafka/iniciar_kafka.py

# Instalar as dependências dos scripts
pip install -r scripts/requirements.txt

# Executar os testes de carga
python scripts/executar_experimento.py
```

O script inicia e encerra automaticamente os contêineres necessários, publica as trajetórias GPS e coleta os resultados. São realizadas **50 execuções**: cinco cargas de veículos (`15`, `31`, `53`, `105` e `1209`), dois mecanismos e cinco repetições. A ordem entre Spark e Flink é invertida a cada repetição.

## Resultados

Os resultados são salvos em `resultados/AAAA-MM-DD_HH-MM-SS/`:

- `metricas_latencia.csv`: latência dos eventos de entrada e saída;
- `gps.jsonl`: mensagens GPS coletadas;
- `alertas.jsonl`: eventos gerados pelos mecanismos.

## Executar novamente

Se as imagens Docker já estiverem construídas, basta executar:

```bash
python kafka/iniciar_kafka.py
python scripts/executar_experimento.py
```

Para encerrar o Kafka após os testes:

```bash
docker compose -f kafka/docker-compose.yml down
```

