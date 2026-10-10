from datetime import datetime
from pathlib import Path


RAIZ = Path(__file__).resolve().parents[1]

ID_EXECUCAO = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
DIRETORIO_RESULTADOS = RAIZ / "resultados" / ID_EXECUCAO

DIRETORIO_SPARK = RAIZ / "consumer_spark"
DIRETORIO_FLINK = RAIZ / "consumer_flink"
DIRETORIO_PRODUCER = RAIZ / "producer"

CONTEINER_KAFKA = "kafka"
ENDERECO_KAFKA_INTERNO = "kafka:19092"
ENDERECO_KAFKA_LOCAL = "localhost:9092"
URL_REST_FLINK = "http://localhost:8081"
URL_REST_SPARK = "http://127.0.0.1:4040/api/v1"

TOPICO_GPS = "gps"
TOPICO_ALERTAS = "alertas"

CARGAS_VEICULOS = (15, 31, 53, 105, 1209)
PONTOS_POR_TRAJETORIA = 50

ALERTAS_ESPERADOS = 2
EVENTOS_ENTRADA_ESPERADOS = 1
EVENTOS_SAIDA_ESPERADOS = 1

TEMPO_LIMITE_S = 900
AQUECIMENTO_S = 8
PAUSA_S = 10

NUMERO_REPETICOES = 5
MECANISMOS = ("spark", "flink")
