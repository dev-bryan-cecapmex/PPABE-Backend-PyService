import os
from dotenv import dotenv_values

def validate_env(required_vars=None):
    """
    Valida que las variables críticas del entorno estén bien configuradas.
    """

    print("\n🔍 Validando variables del entorno...")

    # Carga cruda del .env (sin depender del Config)
    env_values = dotenv_values(".env")

    # Variables críticas por defecto
    required_vars = required_vars or ["ENVIRONMENT", "IP_SERVER_FRONT"]

    missing = []
    invalid = []

    for var in required_vars:
        if var not in env_values or not env_values[var]:
            missing.append(var)
        elif var == "IP_SERVER_FRONT":
            # Validar formato de lista separada por comas
            items = [v.strip() for v in env_values[var].split(",") if v.strip()]
            if not items:
                invalid.append(var)

    # Resultados
    if missing:
        print(f"❌ Faltan variables en el archivo .env: {', '.join(missing)}")
    if invalid:
        print(f"⚠️  Variables con formato incorrecto: {', '.join(invalid)}")

    if not missing and not invalid:
        print("✅ Todas las variables de entorno están correctamente configuradas.\n")
    else:
        print("💡 Revisa el archivo .env antes de iniciar Flask.\n")

    # Retorna el diccionario de variables cargadas
    return env_values
