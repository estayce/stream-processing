import json
import time

from kafka import KafkaConsumer, TopicPartition

from comandos import kafka
from configuracoes import (
    ALERTAS_ESPERADOS,
    ENDERECO_KAFKA_LOCAL,
    TEMPO_LIMITE_S,
    TOPICO_ALERTAS,
    TOPICO_GPS,
)


TOPICOS = [TOPICO_GPS, TOPICO_ALERTAS]


def listar_topicos():
    return kafka(
        "kafka-topics.sh",
        "--list",
        capturar=True,
    ).stdout.splitlines()


def recriar_topicos():
    existentes = listar_topicos()

    for topico in TOPICOS:
        if topico in existentes:
            kafka("kafka-topics.sh", "--delete", "--topic", topico)

    limite = time.time() + 120

    while any(topico in listar_topicos() for topico in TOPICOS):
        if time.time() >= limite:
            raise RuntimeError("Tempo limite removendo tópicos Kafka.")
        time.sleep(2)

    for topico in TOPICOS:
        kafka(
            "kafka-topics.sh",
            "--create",
            "--topic",
            topico,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
            "--config",
            "message.timestamp.type=LogAppendTime",
        )


def contar_mensagens(topico):
    consumer = KafkaConsumer(
        bootstrap_servers=ENDERECO_KAFKA_LOCAL,
        group_id=None,
        enable_auto_commit=False,
    )

    try:
        particoes = consumer.partitions_for_topic(topico)
        if not particoes:
            return 0

        topicos_particoes = [
            TopicPartition(topico, particao) for particao in particoes
        ]
        inicios = consumer.beginning_offsets(topicos_particoes)
        finais = consumer.end_offsets(topicos_particoes)

        return sum(
            finais[item] - inicios[item] for item in topicos_particoes
        )

    finally:
        consumer.close()


def esperar_alertas():
    limite = time.time() + TEMPO_LIMITE_S

    while time.time() < limite:
        total = contar_mensagens(TOPICO_ALERTAS)
        print(
            f"  alertas: {total}/{ALERTAS_ESPERADOS}",
            end="\r",
            flush=True,
        )

        if total >= ALERTAS_ESPERADOS:
            print()
            return

        time.sleep(1)

    raise RuntimeError("Tempo limite esperando os alertas.")


def ler_topico(topico):
    consumer = KafkaConsumer(
        bootstrap_servers=ENDERECO_KAFKA_LOCAL,
        group_id=None,
        enable_auto_commit=False,
    )

    try:
        particoes = consumer.partitions_for_topic(topico)
        if not particoes:
            return []

        topicos_particoes = [
            TopicPartition(topico, particao)
            for particao in sorted(particoes)
        ]
        consumer.assign(topicos_particoes)
        consumer.seek_to_beginning(*topicos_particoes)

        finais = consumer.end_offsets(topicos_particoes)
        registros = []

        while any(
            consumer.position(item) < finais[item]
            for item in topicos_particoes
        ):
            lotes = consumer.poll(timeout_ms=1000)

            for mensagens in lotes.values():
                for mensagem in mensagens:
                    valor_bruto = mensagem.value.decode("utf-8")
                    registros.append(
                        {
                            "instante_ms": mensagem.timestamp,
                            "valor_bruto": valor_bruto,
                            "valor": json.loads(valor_bruto),
                        }
                    )

        return registros

    finally:
        consumer.close()
