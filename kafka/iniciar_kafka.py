import json
import socket
import subprocess
import time
from pathlib import Path


PASTA = Path(__file__).resolve().parent
ARQUIVO_AREAS = PASTA / "26_areas.geojson"

CONTEINER = "kafka"
ENDERECO_BROKER = "localhost:19092"
ENDERECO_PORTA = ("localhost", 9092)
REDE = "stream-network"

TOPICOS = [
    ("gps", True),
    ("alertas", True),
    ("areas", False),
]


def executar(comando, entrada=None, verificar=True):
    return subprocess.run(
        comando,
        cwd=PASTA,
        input=entrada,
        text=True,
        encoding="utf-8",
        check=verificar,
        capture_output=True if verificar else False,
    )


def aguardar_porta_kafka(
    endereco,
    porta,
    tempo_limite_total=60,
    intervalo=2,
):
    inicio = time.time()

    while time.time() - inicio < tempo_limite_total:
        try:
            with socket.create_connection(
                (endereco, porta),
                timeout=intervalo,
            ):
                return True
        except (OSError, ConnectionRefusedError):
            time.sleep(intervalo)

    return False


def main():
    subprocess.run(
        ["docker", "network", "create", REDE],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    print("Iniciando o Docker Compose...")
    executar(["docker", "compose", "up", "-d"])

    print("Aguardando conectividade com o Kafka...")
    if not aguardar_porta_kafka(
        ENDERECO_PORTA[0],
        ENDERECO_PORTA[1],
    ):
        raise RuntimeError(
            "Kafka não ficou disponível na porta TCP esperada."
        )

    time.sleep(5)

    for nome, usar_log_append in TOPICOS:
        comando = [
            "docker",
            "exec",
            CONTEINER,
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            ENDERECO_BROKER,
            "--create",
            "--if-not-exists",
            "--topic",
            nome,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ]

        if usar_log_append:
            comando.extend(
                ["--config", "message.timestamp.type=LogAppendTime"]
            )

        executar(comando)

    dados = json.loads(ARQUIVO_AREAS.read_text(encoding="utf-8"))
    feicoes = dados.get("features", [])

    if len(feicoes) != 26:
        raise ValueError(
            f"O arquivo deve conter 26 áreas "
            f"(encontradas {len(feicoes)})."
        )

    conteudo_json = (
        json.dumps(dados, ensure_ascii=False, separators=(",", ":"))
        + "\n"
    )

    executar(
        [
            "docker",
            "exec",
            "-i",
            CONTEINER,
            "/opt/kafka/bin/kafka-console-producer.sh",
            "--bootstrap-server",
            ENDERECO_BROKER,
            "--topic",
            "areas",
        ],
        entrada=conteudo_json,
    )

    print("Kafka pronto.")


if __name__ == "__main__":
    main()
