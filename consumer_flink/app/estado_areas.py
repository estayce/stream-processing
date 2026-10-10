from pyflink.common import Types
from pyflink.datastream import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor

from eventos import construir_eventos_transicao


class FuncaoEstadoAreas(KeyedProcessFunction):
    def open(self, contexto_execucao):
        descritor = ValueStateDescriptor(
            "area_anterior",
            Types.STRING(),
        )
        self.area_anterior = contexto_execucao.get_state(descritor)

    def process_element(self, valor, _contexto):
        id_veiculo = valor[0]
        ordem = valor[1]
        latitude = valor[2]
        longitude = valor[3]
        data_hora_evento = valor[4]
        id_area = valor[5]
        ponto_wkt = valor[6]
        area_anterior = self.area_anterior.value()

        yield from construir_eventos_transicao(
            area_anterior,
            id_area,
            id_veiculo,
            ordem,
            latitude,
            longitude,
            ponto_wkt,
            data_hora_evento,
        )

        if id_area is None:
            self.area_anterior.clear()
        else:
            self.area_anterior.update(id_area)


def aplicar_estado_areas(fluxo_classificado):
    return (
        fluxo_classificado
        .key_by(
            lambda linha: linha[0],
            key_type=Types.LONG(),
        )
        .process(
            FuncaoEstadoAreas(),
            output_type=Types.STRING(),
        )
    )
