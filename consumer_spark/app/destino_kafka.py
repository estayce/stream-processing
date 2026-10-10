def criar_destino_kafka(
    eventos,
    servidores_kafka: str,
    topico_saida: str,
):
    return (
        eventos
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", servidores_kafka)
        .option("topic", topico_saida)
        .outputMode("update")
        .start()
    )


def executar_aplicacao(aplicacao):
    aplicacao.awaitTermination()
