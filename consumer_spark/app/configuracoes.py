import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Configuracoes:
    servidores_kafka: str
    topico_entrada: str
    topico_saida: str
    topico_areas: str
    diretorio_checkpoint: str


def carregar_configuracoes() -> Configuracoes:
    return Configuracoes(
        servidores_kafka=os.environ["KAFKA_SERVIDORES"],
        topico_entrada=os.environ["KAFKA_TOPICO_ENTRADA"],
        topico_saida=os.environ["KAFKA_TOPICO_SAIDA"],
        topico_areas=os.environ["KAFKA_TOPICO_AREAS"],
        diretorio_checkpoint=os.environ["DIRETORIO_CHECKPOINT"],
    )
