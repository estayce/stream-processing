from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaRecordSerializationSchema,
    KafkaSink,
)


def criar_destino_kafka(
    eventos,
    servidores_kafka: str,
    topico_saida: str,
):
    serializador = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(topico_saida)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )

    destino = (
        KafkaSink.builder()
        .set_bootstrap_servers(servidores_kafka)
        .set_record_serializer(serializador)
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    eventos.sink_to(destino)
    return eventos.get_execution_environment()


def executar_aplicacao(aplicacao):
    aplicacao.execute("alertas-areas-gps")
