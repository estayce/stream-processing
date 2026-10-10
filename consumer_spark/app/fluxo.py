from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json
from sedona.spark import SedonaContext


ESQUEMA_BRUTO = """
id_veiculo BIGINT,
ordem BIGINT,
latitude DOUBLE,
longitude DOUBLE,
data_hora_evento STRING
"""


def escapar_sql(valor: str) -> str:
    return valor.replace("'", "''")


def criar_ambiente(diretorio_checkpoint: str):
    ambiente = (
        SparkSession.builder
        .appName("alertas-areas-gps")
        .config("spark.sql.streaming.checkpointLocation", diretorio_checkpoint)
        .getOrCreate()
    )

    SedonaContext.create(ambiente)
    return ambiente


def criar_fonte(
    ambiente,
    servidores_kafka: str,
    topico_entrada: str,
):
    fluxo_entrada = (
        ambiente.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", servidores_kafka)
        .option("subscribe", topico_entrada)
        .option("startingOffsets", "earliest")
        .load()
    )

    posicoes = (
        fluxo_entrada
        .select(
            from_json(
                col("value").cast("string"),
                ESQUEMA_BRUTO,
            ).alias("dados")
        )
        .select("dados.*")
        .withColumn(
            "posicao_geom",
            expr("ST_Point(longitude, latitude)"),
        )
    )

    posicoes.createOrReplaceTempView("posicoes_gps")
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

    return ambiente.sql(
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
