"""Diagnóstico do ambiente (bibliotecas, autenticação Databricks e sessão Spark).

Verifica:
    1. Versão do Python (3.10, 3.11 ou 3.12).
    2. Pacotes Python obrigatórios (databricks-connect, databricks-sdk, delta, pandas) e conflitos com pyspark avulso.
    3. Configuração do Databricks (variáveis de ambiente ou ~/.databrickscfg).
    4. Autenticação real no workspace Databricks (valida credenciais/token/OAuth via databricks-sdk ou CLI).
    5. (Opcional, --connect) Teste real de criação de sessão Databricks Connect / Spark.
"""

from __future__ import annotations

import configparser
import importlib.metadata
import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

Json = dict[str, Any]

_PACKAGES = [
    (
        "databricks-connect",
        "databricks.connect",
        True,
        "Obrigatório para capturar planos via Databricks Connect (já inclui pyspark e delta)",
    ),
    (
        "databricks-sdk",
        "databricks.sdk",
        True,
        "Obrigatório para autenticação e comunicação com o workspace Databricks",
    ),
    (
        "delta.tables (via databricks-connect)",
        "delta.tables",
        True,
        "Módulo Delta Table embutido no databricks-connect para interceptar MERGE",
    ),
    ("pandas", "pandas", True, "Obrigatório para suporte a df.toPandas() durante a captura"),
]

_ENV_VARS = (
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "DATABRICKS_AUTH_TYPE",
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
    "DATABRICKS_CLUSTER_ID",
    "DATABRICKS_SERVERLESS_COMPUTE_ID",
    "DATABRICKS_CONFIG_PROFILE",
    "SPARK_REMOTE",
)

# Mapa de compatibilidade Python -> Databricks Runtime / databricks-connect
_DB_RUNTIME_BY_PY = {
    (3, 10): "DBR 13.3 LTS / 14.x (databricks-connect ~= 13.3 ou 14.3)",
    (3, 11): "DBR 15.4 LTS (databricks-connect ~= 15.4)",
    (3, 12): "DBR 16.x / Serverless atual (databricks-connect ~= 16.x)",
}
_RECOMMENDED_PY = (3, 12)
_CLI_TIMEOUT_S = 10
_ICON = {"ok": "[OK]  ", "warn": "[WARN]", "error": "[ERRO]"}
_INDENT = "       "


def _has_module(import_name: str) -> bool:
    try:
        return importlib.util.find_spec(import_name) is not None
    except (ModuleNotFoundError, ValueError):
        return False


def _dist_version(dist_name: str) -> str | None:
    try:
        return importlib.metadata.version(dist_name)
    except importlib.metadata.PackageNotFoundError:
        return None


def _active_profile(profile: str | None) -> str:
    return profile or os.environ.get("DATABRICKS_CONFIG_PROFILE") or "DEFAULT"


# --------------------------------------------------------------------------- #
# Python e pacotes
# --------------------------------------------------------------------------- #


def _check_python() -> Json:
    v = sys.version_info
    key = (v.major, v.minor)
    if key == _RECOMMENDED_PY:
        status, detail = "ok", f"Python 3.12 compatível com {_DB_RUNTIME_BY_PY[key]}"
    elif key in _DB_RUNTIME_BY_PY:
        status = "warn"
        detail = f"Compatível apenas com {_DB_RUNTIME_BY_PY[key]}. Para Serverless / DBR 16+, recomenda-se Python 3.12."
    else:
        status = "error"
        detail = (
            f"Python {v.major}.{v.minor} NÃO é suportado pelo databricks-connect "
            "(que exige Python 3.10, 3.11 ou 3.12 — recomendado: Python 3.12 para Serverless/DBR 16+)."
        )
    return {
        "name": "python",
        "status": status,
        "version": f"{v.major}.{v.minor}.{v.micro}",
        "executable": sys.executable,
        "detail": detail,
    }


def _check_package(dist_name: str, import_name: str, required: bool, desc: str) -> Json:
    version = _dist_version(dist_name)
    if version is None and import_name == "delta.tables":
        version = _dist_version("databricks-connect")
    if _has_module(import_name) or version is not None:
        status, detail = "ok", f"Instalado ({version or 'versão desconhecida'})"
    else:
        status, detail = ("error" if required else "warn"), f"Não instalado — {desc}"
    return {
        "name": dist_name,
        "import": import_name,
        "required": required,
        "status": status,
        "version": version,
        "detail": detail,
    }


# --------------------------------------------------------------------------- #
# Configuração Databricks
# --------------------------------------------------------------------------- #


def _read_profile(cfg_path: Path, active_profile: str) -> tuple[list[str], dict[str, str] | None]:
    # Perfis disponíveis no ~/.databrickscfg e as chaves próprias do perfil ativo (sem herdar [DEFAULT]).
    cp = configparser.ConfigParser(interpolation=None, default_section="__spark_lineage_none__")
    cp.read(cfg_path, encoding="utf-8")
    profiles = list(cp.sections())
    if cp.has_section(active_profile):
        return profiles, dict(cp.items(active_profile))
    return profiles, None


def _profile_auth_type(items: dict[str, str]) -> str | None:
    if items.get("auth_type"):
        return items["auth_type"]
    if items.get("token"):
        return "pat (token)"
    if items.get("client_id"):
        return "oauth-m2m"
    return None


def _env_settings() -> dict[str, str | None]:
    env = os.environ
    auth_type = env.get("DATABRICKS_AUTH_TYPE")
    if not auth_type:
        if env.get("DATABRICKS_TOKEN"):
            auth_type = "pat"
        elif env.get("DATABRICKS_CLIENT_ID"):
            auth_type = "oauth-m2m"
    return {
        "host": env.get("DATABRICKS_HOST"),
        "auth_type": auth_type,
        "compute": env.get("DATABRICKS_CLUSTER_ID")
        or env.get("DATABRICKS_SERVERLESS_COMPUTE_ID")
        or env.get("SPARK_REMOTE"),
    }


def _merge_profile(settings: dict[str, str | None], items: dict[str, str]) -> None:
    # Completa as configurações vindas do ambiente com as do perfil (o ambiente tem precedência).
    settings["host"] = settings["host"] or items.get("host")
    settings["auth_type"] = settings["auth_type"] or _profile_auth_type(items)
    settings["compute"] = settings["compute"] or items.get("cluster_id") or items.get("serverless_compute_id")


def _config_issues(settings: dict[str, str | None]) -> list[str]:
    spark_remote = bool(os.environ.get("SPARK_REMOTE"))
    issues = []
    if not settings["host"] and not spark_remote:
        issues.append("host ausente (defina DATABRICKS_HOST ou configure em ~/.databrickscfg)")
    if not settings["auth_type"] and not spark_remote:
        issues.append("credencial ausente (defina token/auth_type em ~/.databrickscfg ou DATABRICKS_TOKEN)")
    if not settings["compute"]:
        issues.append("cluster_id ou serverless_compute_id ausente (necessário para Databricks Connect)")
    return issues


def _check_databricks_config(profile: str | None = None) -> Json:
    env_present = {
        k: ("<definido>" if ("TOKEN" in k or "SECRET" in k) else os.environ[k])
        for k in _ENV_VARS
        if os.environ.get(k)
    }
    active_profile = _active_profile(profile)
    cfg_path = Path(os.environ.get("DATABRICKS_CONFIG_FILE") or (Path.home() / ".databrickscfg"))
    settings = _env_settings()
    profiles_found: list[str] = []
    profile_keys: list[str] = []

    if cfg_path.is_file():
        try:
            profiles_found, items = _read_profile(cfg_path, active_profile)
        except (configparser.Error, OSError, UnicodeDecodeError) as exc:
            return {
                "name": "databricks_config",
                "status": "error",
                "config_file": str(cfg_path),
                "profile": active_profile,
                "profiles_available": [],
                "env_vars": env_present,
                "detail": f"Erro ao ler {cfg_path}: {exc!r}",
            }
        if items is not None:
            profile_keys = sorted(items)
            _merge_profile(settings, items)

    issues = _config_issues(settings)
    if issues:
        status, detail = "error", "; ".join(issues)
    else:
        status = "ok"
        detail = (
            f"Perfil '{active_profile}' configurado "
            f"(host={settings['host']}, auth={settings['auth_type']}, compute={settings['compute']})"
        )
    return {
        "name": "databricks_config",
        "status": status,
        "config_file": str(cfg_path) if cfg_path.is_file() else None,
        "profile": active_profile,
        "profiles_available": profiles_found,
        "profile_keys": profile_keys,
        "host": settings["host"],
        "auth_type": settings["auth_type"],
        "compute": settings["compute"],
        "env_vars": env_present,
        "detail": detail,
    }


# --------------------------------------------------------------------------- #
# Autenticação
# --------------------------------------------------------------------------- #


def _auth_result(status: str, method: str | None, profile: str, detail: str, **extra: Any) -> Json:
    return {
        "name": "databricks_auth",
        "status": status,
        "method": method,
        "profile": profile,
        **extra,
        "detail": detail,
    }


def _auth_via_sdk(profile: str | None, active_profile: str) -> Json:
    # Valida o login com `WorkspaceClient.current_user.me()` (usado internamente pelo databricks-connect).
    try:
        from databricks.sdk import WorkspaceClient

        no_env_creds = not os.environ.get("DATABRICKS_HOST") and not os.environ.get("DATABRICKS_TOKEN")
        w = WorkspaceClient(profile=active_profile) if profile or no_env_creds else WorkspaceClient()
        me = w.current_user.me()
        user_name = getattr(me, "user_name", None) or getattr(me, "display_name", "autenticado")
        host, auth_type = w.config.host, w.config.auth_type
    except Exception as exc:  # noqa: BLE001 - qualquer falha do SDK vira diagnóstico
        return _auth_result(
            "error",
            "databricks-sdk",
            active_profile,
            f"Falha na autenticação Databricks (perfil '{active_profile}'): {exc}. "
            f"Dica: execute `databricks auth login --profile {active_profile}` se usar OAuth/SSO.",
            user=None,
        )
    return _auth_result(
        "ok",
        "databricks-sdk",
        active_profile,
        f"Autenticado com sucesso como '{user_name}' em {host} (via {auth_type})",
        user=user_name,
        host=host,
        auth_type=auth_type,
    )


def _auth_via_cli(db_cli: str, active_profile: str) -> Json:
    # Valida o login via `databricks current-user me` (fallback quando o SDK não está instalado).
    try:
        proc = subprocess.run(  # noqa: S603 - executável resolvido via shutil.which, sem shell
            [db_cli, "current-user", "me", "--profile", active_profile],
            capture_output=True,
            text=True,
            timeout=_CLI_TIMEOUT_S,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return _auth_result("error", "databricks-cli", active_profile, f"Erro ao invocar Databricks CLI: {exc!r}")
    if proc.returncode == 0:
        return _auth_result(
            "ok", "databricks-cli", active_profile, f"Autenticado via Databricks CLI (perfil '{active_profile}')"
        )
    lines = (proc.stderr or proc.stdout or "").strip().splitlines()
    err_msg = lines[0] if lines else "erro desconhecido"
    return _auth_result(
        "error", "databricks-cli", active_profile, f"Falha na autenticação via Databricks CLI: {err_msg}"
    )


def _check_databricks_auth(profile: str | None = None) -> Json:
    # Valida a autenticação no Databricks via databricks-sdk ou, na falta dele, via Databricks CLI.
    active_profile = _active_profile(profile)
    if _has_module("databricks.sdk"):
        return _auth_via_sdk(profile, active_profile)
    db_cli = shutil.which("databricks")
    if db_cli:
        return _auth_via_cli(db_cli, active_profile)
    return _auth_result(
        "error",
        None,
        active_profile,
        "Não foi possível validar o auth: instale 'databricks-sdk' / 'databricks-connect' "
        "(pip install -r requirements.txt).",
    )


def _check_spark_connection() -> Json:
    from .capture_plans import get_spark

    try:
        spark = get_spark()
    except Exception as exc:  # noqa: BLE001 - qualquer falha de conexão vira diagnóstico
        return {
            "name": "spark_session",
            "status": "error",
            "session_type": None,
            "spark_version": None,
            "detail": f"Falha ao iniciar sessão Spark: {exc}",
        }
    version = getattr(spark, "version", "desconhecida")
    return {
        "name": "spark_session",
        "status": "ok",
        "session_type": type(spark).__name__,
        "spark_version": str(version),
        "detail": f"Sessão {type(spark).__name__} conectada (Spark {version})",
    }


# --------------------------------------------------------------------------- #
# Orquestração e relatório
# --------------------------------------------------------------------------- #


def _pyspark_conflict_warnings() -> list[str]:
    # Verifica se o pacote 'pyspark' avulso está instalado conflitando com 'databricks-connect'.
    standalone_pyspark = _dist_version("pyspark")
    if not standalone_pyspark:
        return []
    db_connect_ver = _dist_version("databricks-connect")
    if not db_connect_ver:
        return [
            f"O pacote 'pyspark' ({standalone_pyspark}) está instalado de forma avulsa, mas 'databricks-connect' "
            "não está. Para instalar o 'databricks-connect' sem conflitos, remova primeiro o pyspark: "
            "`pip uninstall pyspark && pip install -r requirements.txt`."
        ]
    return [
        f"Ambos os pacotes 'pyspark' ({standalone_pyspark}) e 'databricks-connect' ({db_connect_ver}) constam no pip. "
        "Como o databricks-connect já embute o pyspark, ter os dois instalados pode causar conflitos; "
        "se notar erros, rode: `pip uninstall pyspark databricks-connect && pip install databricks-connect`."
    ]


def _overall_status(checks: list[Json | None], warnings: list[str]) -> str:
    statuses = {c["status"] for c in checks if c}
    if "error" in statuses:
        return "error"
    if "warn" in statuses or warnings:
        return "warn"
    return "ok"


def run_doctor(connect: bool = False, profile: str | None = None, check_auth: bool = True) -> Json:
    """Executa todos os diagnósticos e devolve um dicionário estruturado.

    Args:
        connect: Se ``True``, testa a criação real da sessão Spark / Databricks Connect.
        profile: Perfil do ~/.databrickscfg a validar (padrão: DEFAULT ou DATABRICKS_CONFIG_PROFILE).
        check_auth: Se ``True``, valida a autenticação no workspace.

    Returns:
        Relatório com o status geral e o resultado de cada verificação.
    """
    py_check = _check_python()
    pkg_checks = [_check_package(*p) for p in _PACKAGES]
    db_cfg = _check_databricks_config(profile=profile)
    db_auth = _check_databricks_auth(profile=profile) if check_auth else None
    warnings = _pyspark_conflict_warnings()
    conn_check = _check_spark_connection() if connect else None
    return {
        "status": _overall_status([py_check, *pkg_checks, db_cfg, db_auth, conn_check], warnings),
        "python": py_check,
        "packages": pkg_checks,
        "databricks_config": db_cfg,
        "databricks_auth": db_auth,
        "connection": conn_check,
        "warnings": warnings,
    }


def _render_python(py: Json) -> list[str]:
    return [f"{_ICON[py['status']]} Python {py['version']} ({py['executable']})", f"{_INDENT}{py['detail']}", ""]


def _render_packages(packages: list[Json]) -> list[str]:
    lines = ["-- Bibliotecas Python (requirements.txt) --"]
    for p in packages:
        req_tag = "obrigatório" if p["required"] else "opcional"
        lines.append(f"{_ICON[p['status']]} {p['name']} [{req_tag}]: {p['detail']}")
    return [*lines, ""]


def _render_databricks(db: Json, auth: Json | None) -> list[str]:
    lines = [
        "-- Configuração & Autenticação Databricks --",
        f"{_ICON[db['status']]} Configuração (perfil '{db['profile']}'): {db['detail']}",
    ]
    if db.get("config_file"):
        profiles = ", ".join(db.get("profiles_available") or []) or "nenhum"
        lines.append(f"{_INDENT}arquivo: {db['config_file']} (perfis: {profiles})")
    if db.get("env_vars"):
        env_str = ", ".join(f"{k}={v}" for k, v in db["env_vars"].items())
        lines.append(f"{_INDENT}variáveis de ambiente: {env_str}")
    if auth is not None:
        lines.append(f"{_ICON[auth['status']]} Autenticação Workspace: {auth['detail']}")
    return lines


def _render_optional(report: Json) -> list[str]:
    lines: list[str] = []
    conn = report["connection"]
    if conn is not None:
        lines += ["", "-- Teste de Sessão Spark / Databricks Connect --", f"{_ICON[conn['status']]} {conn['detail']}"]
    if report["warnings"]:
        lines += ["", "-- Avisos Adicionais --"] + [f"- {w}" for w in report["warnings"]]
    return lines


def render_doctor_text(report: Json) -> str:
    """Formata o relatório do ``run_doctor`` como texto legível no terminal.

    Args:
        report: Relatório produzido por ``run_doctor``.

    Returns:
        O texto formatado, terminado em quebra de linha.
    """
    lines = [
        "=== spark-lineage doctor ===",
        "",
        *_render_python(report["python"]),
        *_render_packages(report["packages"]),
        *_render_databricks(report["databricks_config"], report.get("databricks_auth")),
        *_render_optional(report),
        "",
        f"Resultado geral: {report['status'].upper()}",
    ]
    return "\n".join(lines) + "\n"
