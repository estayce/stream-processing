import time

from coletar_kafka import esperar_alertas, ler_topico, recriar_topicos
from comandos import compose, docker, verificar_docker
from configuracoes import (
    AQUECIMENTO_S,
    CARGAS_VEICULOS,
    CONTEINER_KAFKA,
    DIRETORIO_FLINK,
    DIRETORIO_PRODUCER,
    DIRETORIO_RESULTADOS,
    DIRETORIO_SPARK,
    MECANISMOS,
    NUMERO_REPETICOES,
    PAUSA_S,
    TOPICO_ALERTAS,
    TOPICO_GPS,
)
from mecanismos import iniciar_mecanismo, parar_mecanismo
from resultados import salvar_dados_topico, salvar_metricas_latencia
from validacao import validar


ARQUIVO_LATENCIA = DIRETORIO_RESULTADOS / "metricas_latencia.csv"
ARQUIVO_GPS = DIRETORIO_RESULTADOS / "gps.jsonl"
ARQUIVO_ALERTAS = DIRETORIO_RESULTADOS / "alertas.jsonl"


def executar_condicao(repeticao, condicao, carga_veiculos):
    print(
        f"{repeticao} | {condicao.capitalize()} | "
        f"{carga_veiculos} veículos"
    )

    recriar_topicos()
    estado_mecanismo = None

    try:
        estado_mecanismo = iniciar_mecanismo(condicao)

        time.sleep(AQUECIMENTO_S)

        compose(
            DIRETORIO_PRODUCER,
            "run",
            "--rm",
            "-e",
            f"QUANTIDADE_VEICULOS={carga_veiculos}",
            "producer",
        )

        esperar_alertas()

        gps = ler_topico(TOPICO_GPS)
        alertas = ler_topico(TOPICO_ALERTAS)

        validar(gps, alertas, carga_veiculos)

        salvar_dados_topico(
            gps,
            ARQUIVO_GPS,
            repeticao,
            condicao,
            carga_veiculos,
        )
        salvar_dados_topico(
            alertas,
            ARQUIVO_ALERTAS,
            repeticao,
            condicao,
            carga_veiculos,
        )
        salvar_metricas_latencia(
            gps,
            alertas,
            ARQUIVO_LATENCIA,
            repeticao,
            condicao,
            carga_veiculos,
        )

        print("  execução válida")

    finally:
        if estado_mecanismo is not None:
            parar_mecanismo(condicao, estado_mecanismo)


def preparar_experimento():
    verificar_docker()
    docker("start", CONTEINER_KAFKA, verificar=False)

    DIRETORIO_RESULTADOS.mkdir(parents=True, exist_ok=True)

    compose(DIRETORIO_SPARK, "down", "-v", verificar=False)
    compose(DIRETORIO_FLINK, "down", "-v", verificar=False)


def main():
    preparar_experimento()

    execucoes = []

    for repeticao in range(1, NUMERO_REPETICOES + 1):
        ordem_mecanismos = (
            MECANISMOS if repeticao % 2 else MECANISMOS[::-1]
        )

        for carga_veiculos in CARGAS_VEICULOS:
            for condicao in ordem_mecanismos:
                execucoes.append(
                    (f"R{repeticao:02d}", condicao, carga_veiculos)
                )

    print(
        f"Experimento: {NUMERO_REPETICOES} repetições x "
        f"{len(CARGAS_VEICULOS)} cargas x {len(MECANISMOS)} mecanismos "
        f"({len(execucoes)} execuções no total)"
    )

    for indice, execucao in enumerate(execucoes):
        executar_condicao(*execucao)

        if indice < len(execucoes) - 1:
            time.sleep(PAUSA_S)

    print("\nEXPERIMENTO CONCLUÍDO")
    print(f"Métricas de latência: {ARQUIVO_LATENCIA}")
    print(f"GPS: {ARQUIVO_GPS}")
    print(f"Alertas: {ARQUIVO_ALERTAS}")


if __name__ == "__main__":
    main()
