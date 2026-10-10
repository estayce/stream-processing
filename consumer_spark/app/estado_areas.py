import pandas as pd
from pyspark.sql.types import StringType, StructField, StructType

from eventos import construir_eventos_transicao


ESQUEMA_SAIDA = StructType([
    StructField("value", StringType(), False),
])

ESQUEMA_ESTADO = StructType([
    StructField("area_anterior", StringType(), True),
])


def processar_veiculo(_chave, iterador_pdf, estado):
    area_anterior = estado.get[0] if estado.exists else None
    lotes = [pdf for pdf in iterador_pdf if not pdf.empty]

    if not lotes:
        return

    linhas = pd.concat(lotes, ignore_index=True).sort_values(
        by="ordem",
        kind="stable",
    )
    linhas_saida = []

    for linha in linhas.itertuples(index=False):
        id_area = None if pd.isna(linha.id_area) else str(linha.id_area)

        linhas_saida.extend(
            construir_eventos_transicao(
                area_anterior,
                id_area,
                linha.id_veiculo,
                linha.ordem,
                linha.latitude,
                linha.longitude,
                linha.ponto_wkt,
                linha.data_hora_evento,
            )
        )

        area_anterior = id_area

    if area_anterior is None:
        estado.remove()
    else:
        estado.update((area_anterior,))

    if linhas_saida:
        yield pd.DataFrame({"value": linhas_saida})


def aplicar_estado_areas(fluxo_classificado):
    return (
        fluxo_classificado
        .groupBy("id_veiculo")
        .applyInPandasWithState(
            processar_veiculo,
            outputStructType=ESQUEMA_SAIDA,
            stateStructType=ESQUEMA_ESTADO,
            outputMode="Update",
            timeoutConf="NoTimeout",
        )
    )
