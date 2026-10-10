from pathlib import Path

from pyflink.common import Configuration
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings
from sedona.flink import SedonaContext


ID_GRUPO_KAFKA = "grupo-sedona-areas"
INTERVALO_CHECKPOINT_MS = 10000


def escapar_sql(valor: str) -> str:
    return valor.replace("'", "''")


def criar_ambiente(diretorio_checkpoint: str):
    configuracao = Configuration()
    configuracao.set_string(
        "state.checkpoints.dir",
        Path(diretorio_checkpoint).as_uri(),
    )

    ambiente = StreamExecutionEnvironment.get_execution_environment(
        configuracao
    )
    ambiente.enable_checkpointing(INTERVALO_CHECKPOINT_MS)
    ambiente.get_checkpoint_config().set_checkpointing_mode(
        CheckpointingMode.AT_LEAST_ONCE
    )

    return SedonaContext.create(
        ambiente,
        EnvironmentSettings.in_streaming_mode(),
    )


def criar_fonte(
    ambiente,
    servidores_kafka: str,
    topico_entrada: str,
):
    ambiente.execute_sql(
        f"""
        CREATE TABLE posicoes_gps (
            id_veiculo BIGINT,
            ordem BIGINT,
            latitude DOUBLE,
            longitude DOUBLE,
            data_hora_evento STRING,
            posicao_geom AS ST_Point(longitude, latitude)
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{escapar_sql(topico_entrada)}',
            'properties.bootstrap.servers' = '{escapar_sql(servidores_kafka)}',
            'properties.group.id' = '{ID_GRUPO_KAFKA}',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
        """
    )

    return "posicoes_gps"


def construir_case_areas(
    areas: list[tuple[str, str]],
) -> str:
    clausulas = [
        (
            f"WHEN ST_Contains("
            f"ST_GeomFromWKT('{escapar_sql(wkt)}'), "
            f"posicao_geom"
            f") THEN '{escapar_sql(id_area)}'"
        )
        for id_area, wkt in areas
    ]

    return f"CASE {' '.join(clausulas)} ELSE NULL END"


def classificar_posicoes(
    ambiente,
    posicoes,
    areas: list[tuple[str, str]],
):
    case_areas = construir_case_areas(areas)

    tabela_classificada = ambiente.sql_query(
        f"""
        SELECT
            id_veiculo,
            ordem,
            latitude,
            longitude,
            data_hora_evento,
            {case_areas} AS id_area,
            ST_AsText(posicao_geom) AS ponto_wkt
        FROM {posicoes}
        """
    )

    return ambiente.to_data_stream(tabela_classificada)
