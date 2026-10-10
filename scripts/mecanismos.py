import json
import re
import subprocess
import time
import urllib.request

from comandos import compose, docker
from configuracoes import (
    DIRETORIO_FLINK,
    DIRETORIO_SPARK,
    TEMPO_LIMITE_S,
    URL_REST_FLINK,
    URL_REST_SPARK,
)


CAMINHO_CONSUMER = "/opt/stream/app/consumer.py"

CONTEINERES_POR_MECANISMO = {
    "spark": ("spark-master", "spark-worker"),
    "flink": ("flink-jobmanager", "flink-taskmanager"),
}


def limpar_checkpoint(conteiner):
    docker(
        "exec",
        conteiner,
        "sh",
        "-lc",
        "find /opt/stream/checkpoints -mindepth 1 -maxdepth 1 -exec rm -rf {} +",
    )


def esperar_conteineres(conteineres, tempo_limite=30):
    limite = time.time() + tempo_limite

    while time.time() < limite:
        resultado = docker(
            "ps",
            "--format",
            "{{.Names}}",
            capturar=True,
            verificar=False,
        )
        ativos = set(resultado.stdout.splitlines())

        if set(conteineres).issubset(ativos):
            return

        time.sleep(1)

    raise RuntimeError(
        f"Tempo limite aguardando os contêineres: {conteineres}"
    )


def ler_json_http(url):
    with urllib.request.urlopen(url, timeout=3) as resposta:
        return json.loads(resposta.read().decode("utf-8"))


def ler_json_conteiner(conteiner, url):
    codigo = (
        "import sys,urllib.request;"
        "r=urllib.request.urlopen(sys.argv[1],timeout=3);"
        "print(r.read().decode('utf-8'))"
    )

    resultado = docker(
        "exec",
        conteiner,
        "python3",
        "-c",
        codigo,
        url,
        capturar=True,
        verificar=False,
    )

    if resultado.returncode != 0:
        detalhe = (resultado.stderr or resultado.stdout or "").strip()
        raise RuntimeError(
            detalhe or f"Falha ao acessar {url} em {conteiner}."
        )

    return json.loads(resultado.stdout)


def esperar_spark_pronto(processo):
    limite = time.time() + TEMPO_LIMITE_S
    ultimo_erro = None

    while time.time() < limite:
        if processo.poll() is not None:
            raise RuntimeError(
                f"spark-submit encerrou durante a inicialização "
                f"(código {processo.returncode}). "
                f"Verifique as configurações do Spark."
            )

        try:
            aplicacoes = ler_json_conteiner(
                "spark-master",
                f"{URL_REST_SPARK}/applications",
            )

            if aplicacoes:
                id_aplicacao = aplicacoes[0].get("id")

                if id_aplicacao:
                    executores = ler_json_conteiner(
                        "spark-master",
                        f"{URL_REST_SPARK}/applications/"
                        f"{id_aplicacao}/executors",
                    )

                    if any(
                        executor.get("id") != "driver"
                        and executor.get("isActive", True)
                        for executor in executores
                    ):
                        return

        except Exception as excecao:
            ultimo_erro = excecao

        time.sleep(2)

    detalhe = f" Último erro: {ultimo_erro}" if ultimo_erro else ""
    raise RuntimeError(
        f"Spark não ficou pronto dentro do tempo limite "
        f"({TEMPO_LIMITE_S}s).{detalhe}"
    )


def iniciar_spark():
    processo = None

    try:
        compose(DIRETORIO_SPARK, "up", "-d")
        esperar_conteineres(["spark-master", "spark-worker"])
        limpar_checkpoint("spark-master")

        comando = [
            "docker",
            "exec",
            "spark-master",
            "/opt/spark/bin/spark-submit",
            "--master",
            "spark://spark-master:7077",
            "--deploy-mode",
            "client",
            "--conf",
            "spark.driver.extraJavaOptions=-Dlog4j.threshold=WARN",
            "--conf",
            "spark.executor.extraJavaOptions=-Dlog4j.threshold=WARN",
            CAMINHO_CONSUMER,
        ]

        processo = subprocess.Popen(
            comando,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        esperar_spark_pronto(processo)
        return processo

    except Exception:
        parar_spark(processo)
        raise


def parar_spark(processo):
    if processo and processo.poll() is None:
        processo.terminate()

        try:
            processo.wait(timeout=5)
        except subprocess.TimeoutExpired:
            processo.kill()
            processo.wait(timeout=3)

    compose(DIRETORIO_SPARK, "down", "-v", verificar=False)


def esperar_flink(caminho, pronto):
    limite = time.time() + TEMPO_LIMITE_S
    ultimo_erro = None

    while time.time() < limite:
        try:
            if pronto(ler_json_http(URL_REST_FLINK + caminho)):
                return
        except Exception as excecao:
            ultimo_erro = excecao

        time.sleep(2)

    detalhe = f" Último erro: {ultimo_erro}" if ultimo_erro else ""
    raise RuntimeError(
        f"Flink não ficou pronto dentro do tempo limite "
        f"({TEMPO_LIMITE_S}s).{detalhe}"
    )


def iniciar_flink():
    id_job = None

    try:
        compose(DIRETORIO_FLINK, "up", "-d")
        esperar_conteineres(["flink-jobmanager", "flink-taskmanager"])
        esperar_flink(
            "/taskmanagers",
            lambda dados: bool(dados.get("taskmanagers")),
        )
        limpar_checkpoint("flink-jobmanager")

        resultado = docker(
            "exec",
            "flink-jobmanager",
            "/opt/flink/bin/flink",
            "run",
            "-d",
            "-py",
            CAMINHO_CONSUMER,
            capturar=True,
        )

        texto = (resultado.stdout or "") + (resultado.stderr or "")
        correspondencia = re.search(
            r"JobID\s*[: ]*\s*([0-9a-fA-F]{32})",
            texto,
        )

        if not correspondencia:
            raise RuntimeError("JobID do Flink não encontrado.")

        id_job = correspondencia.group(1)

        esperar_flink(
            f"/jobs/{id_job}",
            lambda dados: dados.get("state") == "RUNNING",
        )

        return id_job

    except Exception:
        parar_flink(id_job)
        raise


def parar_flink(id_job):
    if id_job:
        docker(
            "exec",
            "flink-jobmanager",
            "/opt/flink/bin/flink",
            "cancel",
            id_job,
            capturar=True,
            verificar=False,
        )

    compose(DIRETORIO_FLINK, "down", "-v", verificar=False)


def iniciar_mecanismo(condicao):
    if condicao == "spark":
        return iniciar_spark()

    if condicao == "flink":
        return iniciar_flink()

    raise ValueError(f"Condição desconhecida: {condicao}")


def parar_mecanismo(condicao, estado):
    if condicao == "spark":
        parar_spark(estado)
        return

    if condicao == "flink":
        parar_flink(estado)
        return

    raise ValueError(f"Condição desconhecida: {condicao}")
