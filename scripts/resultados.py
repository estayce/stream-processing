import csv
import json


CAMPOS_LATENCIA = [
    "repeticao",
    "condicao",
    "carga_veiculos",
    "id_veiculo",
    "ordem",
    "tipo_evento",
    "instante_gps_ms",
    "instante_alerta_ms",
    "latencia_ms",
]


def salvar_dados_topico(
    registros,
    arquivo,
    repeticao,
    condicao,
    carga_veiculos,
):
    with arquivo.open("a", encoding="utf-8", newline="\n") as saida:
        for registro in registros:
            linha = {
                "repeticao": repeticao,
                "condicao": condicao,
                "carga_veiculos": carga_veiculos,
                "instante_ms": registro["instante_ms"],
                **registro["valor"],
            }
            saida.write(
                json.dumps(
                    linha,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                + "\n"
            )


def salvar_metricas_latencia(
    gps,
    alertas,
    arquivo,
    repeticao,
    condicao,
    carga_veiculos,
):
    gps_por_evento = {
        (
            int(registro["valor"]["id_veiculo"]),
            int(registro["valor"]["ordem"]),
        ): registro["instante_ms"]
        for registro in gps
    }

    novo = not arquivo.exists()

    with arquivo.open("a", encoding="utf-8-sig", newline="") as saida:
        gravador = csv.DictWriter(saida, fieldnames=CAMPOS_LATENCIA)

        if novo:
            gravador.writeheader()

        for registro in alertas:
            valor = registro["valor"]
            chave = (int(valor["id_veiculo"]), int(valor["ordem"]))

            if chave not in gps_por_evento:
                raise RuntimeError(f"GPS não encontrado para {chave}.")

            instante_gps_ms = gps_por_evento[chave]
            instante_alerta_ms = registro["instante_ms"]

            gravador.writerow(
                {
                    "repeticao": repeticao,
                    "condicao": condicao,
                    "carga_veiculos": carga_veiculos,
                    "id_veiculo": chave[0],
                    "ordem": chave[1],
                    "tipo_evento": valor.get("tipo_evento", ""),
                    "instante_gps_ms": instante_gps_ms,
                    "instante_alerta_ms": instante_alerta_ms,
                    "latencia_ms": instante_alerta_ms - instante_gps_ms,
                }
            )
