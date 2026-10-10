import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer


DIRETORIO_TRAJETORIAS = Path(
    os.getenv("DIRETORIO_TRAJETORIAS", "/app/trajetoria")
)
SERVIDORES_KAFKA = os.getenv("KAFKA_SERVIDORES", "kafka:19092")
TOPICO_GPS = os.getenv("KAFKA_TOPICO", "gps")

QUANTIDADE_VEICULOS = int(os.getenv("QUANTIDADE_VEICULOS", "15"))
PONTOS_POR_TRAJETORIA = int(os.getenv("PONTOS_POR_TRAJETORIA", "50"))
INTERVALO_SEGUNDOS = float(os.getenv("INTERVALO_SEGUNDOS", "1.0"))


def id_veiculo(arquivo):
    return int(arquivo.stem.rsplit("_", 1)[1])


def carregar_trajetorias():
    if QUANTIDADE_VEICULOS <= 0:
        raise ValueError("QUANTIDADE_VEICULOS deve ser maior que zero.")

    arquivos = sorted(
        DIRETORIO_TRAJETORIAS.glob("trajetoria_veiculo_*.geojson"),
        key=id_veiculo,
    )

    if len(arquivos) < QUANTIDADE_VEICULOS:
        raise ValueError(
            f"Esperadas pelo menos {QUANTIDADE_VEICULOS} trajetórias em "
            f"{DIRETORIO_TRAJETORIAS}, mas foram encontradas {len(arquivos)}."
        )

    arquivos = arquivos[:QUANTIDADE_VEICULOS]
    trajetorias = []

    for arquivo in arquivos:
        dados = json.loads(arquivo.read_text(encoding="utf-8"))
        pontos = [
            {
                "id_veiculo": int(feicao["properties"]["vehicle_id"]),
                "ordem": int(feicao["properties"]["ordem"]),
                "latitude": float(feicao["geometry"]["coordinates"][1]),
                "longitude": float(feicao["geometry"]["coordinates"][0]),
            }
            for feicao in dados["features"]
        ]
        pontos.sort(key=lambda ponto: ponto["ordem"])

        if len(pontos) != PONTOS_POR_TRAJETORIA:
            raise ValueError(
                f"{arquivo.name}: esperados {PONTOS_POR_TRAJETORIA} pontos, "
                f"mas foram encontrados {len(pontos)}."
            )

        trajetorias.append(pontos)

    return trajetorias


def main():
    trajetorias = carregar_trajetorias()
    producer = KafkaProducer(
        bootstrap_servers=SERVIDORES_KAFKA,
        acks="all",
        retries=5,
    )

    enviados = 0
    print(
        f"Iniciando: {QUANTIDADE_VEICULOS} veículos | "
        f"{PONTOS_POR_TRAJETORIA} pontos | "
        f"{INTERVALO_SEGUNDOS}s | tópico={TOPICO_GPS}"
    )

    try:
        for ordem in range(PONTOS_POR_TRAJETORIA):
            inicio = time.perf_counter()

            for trajetoria in trajetorias:
                ponto = trajetoria[ordem]
                evento = {
                    **ponto,
                    "data_hora_evento": datetime.now(timezone.utc)
                    .isoformat(timespec="milliseconds")
                    .replace("+00:00", "Z"),
                }

                producer.send(
                    TOPICO_GPS,
                    key=str(ponto["id_veiculo"]).encode("utf-8"),
                    value=json.dumps(evento).encode("utf-8"),
                )
                enviados += 1

            producer.flush()
            print(
                f"Rodada {ordem + 1}/{PONTOS_POR_TRAJETORIA} | "
                f"{QUANTIDADE_VEICULOS} eventos | total={enviados}"
            )

            espera = INTERVALO_SEGUNDOS - (time.perf_counter() - inicio)
            if espera > 0 and ordem < PONTOS_POR_TRAJETORIA - 1:
                time.sleep(espera)
    finally:
        producer.close()

    print(f"Finalizado: {enviados} eventos enviados.")


if __name__ == "__main__":
    main()
