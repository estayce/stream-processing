import json

from kafka import KafkaConsumer, TopicPartition
from shapely.geometry import shape


def poligono_para_wkt(geometria: dict) -> str:
    return shape(geometria).wkt


def carregar_areas(
    servidores_kafka: str,
    topico: str,
) -> list[tuple[str, str]]:
    consumer = KafkaConsumer(
        bootstrap_servers=servidores_kafka,
        enable_auto_commit=False,
        group_id=None,
        request_timeout_ms=30000,
        value_deserializer=lambda valor: json.loads(
            valor.decode("utf-8")
        ),
    )

    try:
        particao = TopicPartition(topico, 0)
        consumer.assign([particao])

        offset_final = consumer.end_offsets([particao])[particao]

        if offset_final == 0:
            raise RuntimeError(f"Tópico {topico!r} vazio.")

        consumer.seek(particao, offset_final - 1)

        registros = consumer.poll(
            timeout_ms=10000,
            max_records=1,
        )
        mensagens = registros.get(particao, [])

        if not mensagens:
            raise RuntimeError(
                f"Não foi possível ler o tópico {topico!r}."
            )

        dados = mensagens[0].value

    finally:
        consumer.close()

    return [
        (
            str(feicao["properties"]["id"]),
            poligono_para_wkt(feicao["geometry"]),
        )
        for feicao in dados["features"]
    ]
