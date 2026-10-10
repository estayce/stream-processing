from areas import carregar_areas
from configuracoes import carregar_configuracoes
from destino_kafka import criar_destino_kafka, executar_aplicacao
from estado_areas import aplicar_estado_areas
from fluxo import classificar_posicoes, criar_ambiente, criar_fonte


def main():
    configuracoes = carregar_configuracoes()

    areas = carregar_areas(
        configuracoes.servidores_kafka,
        configuracoes.topico_areas,
    )

    ambiente = criar_ambiente(
        configuracoes.diretorio_checkpoint,
    )

    posicoes = criar_fonte(
        ambiente,
        configuracoes.servidores_kafka,
        configuracoes.topico_entrada,
    )

    classificadas = classificar_posicoes(
        ambiente,
        posicoes,
        areas,
    )

    eventos = aplicar_estado_areas(classificadas)

    aplicacao = criar_destino_kafka(
        eventos,
        configuracoes.servidores_kafka,
        configuracoes.topico_saida,
    )

    executar_aplicacao(aplicacao)


if __name__ == "__main__":
    main()
