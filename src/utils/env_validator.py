import os
from pathlib import Path

from dotenv import dotenv_values, load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent.parent
ENV_PATH = BASE_DIR / ".env"


def validate_env(required_vars=None):
    """Valida que las variables críticas del entorno estén bien configuradas."""

    print("\nValidando variables del entorno...")

    load_dotenv(dotenv_path=ENV_PATH, override=False)
    env_values = {**dotenv_values(ENV_PATH), **os.environ}
    required_vars = required_vars or ["ENVIRONMENT", "IP_SERVER_FRONT"]

    missing = []
    invalid = []

    for var in required_vars:
        value = env_values.get(var)
        if not value:
            missing.append(var)
        elif var == "IP_SERVER_FRONT":
            items = [item.strip() for item in str(value).split(",") if item.strip()]
            if not items:
                invalid.append(var)

    if missing:
        print(f"Faltan variables en el entorno: {', '.join(missing)}")
    if invalid:
        print(f"Variables con formato incorrecto: {', '.join(invalid)}")

    if not missing and not invalid:
        print("Todas las variables de entorno están correctamente configuradas.\n")
    else:
        print("Revisa el archivo .env o las variables del contenedor antes de iniciar Flask.\n")

    return env_values
