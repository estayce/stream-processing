import subprocess

from configuracoes import CONTEINER_KAFKA, ENDERECO_KAFKA_INTERNO


def executar(comando, diretorio=None, capturar=False, verificar=True):
    return subprocess.run(
        comando,
        cwd=diretorio,
        capture_output=capturar,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=verificar,
    )


def docker(*argumentos, capturar=False, verificar=True):
    return executar(
        ["docker", *argumentos],
        capturar=capturar,
        verificar=verificar,
    )


def compose(diretorio, *argumentos, capturar=False, verificar=True):
    arquivo = diretorio / "docker-compose.yml"

    return executar(
        ["docker", "compose", "-f", str(arquivo), *argumentos],
        diretorio=diretorio,
        capturar=capturar,
        verificar=verificar,
    )


def kafka(nome_script, *argumentos, capturar=False, verificar=True):
    return docker(
        "exec",
        CONTEINER_KAFKA,
        f"/opt/kafka/bin/{nome_script}",
        "--bootstrap-server",
        ENDERECO_KAFKA_INTERNO,
        *argumentos,
        capturar=capturar,
        verificar=verificar,
    )


def verificar_docker():
    try:
        resultado = docker("info", capturar=True, verificar=False)
    except FileNotFoundError as excecao:
        raise RuntimeError("Docker não está instalado.") from excecao

    if resultado.returncode != 0:
        raise RuntimeError("Docker não está ativo. Abra o Docker Desktop.")
