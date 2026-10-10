from collections import Counter

from configuracoes import (
    ALERTAS_ESPERADOS,
    EVENTOS_ENTRADA_ESPERADOS,
    EVENTOS_SAIDA_ESPERADOS,
    PONTOS_POR_TRAJETORIA,
)


def validar(gps, alertas, quantidade_veiculos):
    gps_esperados = quantidade_veiculos * PONTOS_POR_TRAJETORIA

    if len(gps) != gps_esperados:
        raise RuntimeError(f"GPS inválido: {len(gps)}/{gps_esperados}")

    if len(alertas) != ALERTAS_ESPERADOS:
        raise RuntimeError(
            f"Alertas inválidos: {len(alertas)}/{ALERTAS_ESPERADOS}"
        )

    tipos = Counter(
        registro["valor"].get("tipo_evento", "")
        for registro in alertas
    )

    if (
        tipos["ENTRADA"] != EVENTOS_ENTRADA_ESPERADOS
        or tipos["SAIDA"] != EVENTOS_SAIDA_ESPERADOS
    ):
        raise RuntimeError(
            f"Quantidade de ENTRADA/SAIDA inválida. "
            f"Esperado: ENTRADA={EVENTOS_ENTRADA_ESPERADOS}, "
            f"SAIDA={EVENTOS_SAIDA_ESPERADOS}. "
            f"Obtido: ENTRADA={tipos['ENTRADA']}, "
            f"SAIDA={tipos['SAIDA']}."
        )
