import json


TIPO_AREA = "DESCARTE_IRREGULAR"


def construir_evento(
    id_veiculo,
    ordem,
    id_area,
    latitude,
    longitude,
    ponto_wkt,
    data_hora_evento,
    tipo_evento,
) -> str:
    return json.dumps(
        {
            "id_veiculo": str(id_veiculo),
            "ordem": int(ordem),
            "area_violada": id_area,
            "tipo_area": TIPO_AREA,
            "tipo_evento": tipo_evento,
            "latitude": float(latitude),
            "longitude": float(longitude),
            "ponto_wkt": ponto_wkt,
            "data_hora_evento": data_hora_evento,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def construir_eventos_transicao(
    area_anterior,
    id_area,
    id_veiculo,
    ordem,
    latitude,
    longitude,
    ponto_wkt,
    data_hora_evento,
):
    if area_anterior == id_area:
        return []

    eventos = []

    if area_anterior is not None:
        eventos.append(
            construir_evento(
                id_veiculo,
                ordem,
                area_anterior,
                latitude,
                longitude,
                ponto_wkt,
                data_hora_evento,
                "SAIDA",
            )
        )

    if id_area is not None:
        eventos.append(
            construir_evento(
                id_veiculo,
                ordem,
                id_area,
                latitude,
                longitude,
                ponto_wkt,
                data_hora_evento,
                "ENTRADA",
            )
        )

    return eventos
