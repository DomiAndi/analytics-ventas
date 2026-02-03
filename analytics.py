# -----------------------------------------------------------------------------
# 1. Variables y Estructuras de Datos
# -----------------------------------------------------------------------------

# Variables globales
RUTA_CSV = "data/ventas.csv"      # Ruta al archivo de ventas
RUTA_JSON = "data/clientes.json"  # Ruta al archivo de clientes
REGION_FILTRO = "Norte"

# Lista de categorías válidas
CATEGORIAS_VALIDAS = ["Tecnología", "Periféricos", "Software"]

# Diccionario de conversión de moneda
TASAS_CAMBIO = {
    "USD": 1.0,
    "EUR": 0.85,
    "CLP": 900
}

# -----------------------------------------------------------------------------
# 2. Lectura de Datos con Manejo de Errores
# -----------------------------------------------------------------------------

import pandas as pd


def cargar_datos(ruta_csv: str, ruta_json: str) -> tuple:
    """
    Carga los archivos de ventas (CSV) y clientes (JSON).

    Args:
        ruta_csv:  Ruta al archivo CSV de ventas.
        ruta_json: Ruta al archivo JSON de clientes.

    Returns:
        Tupla (ventas: DataFrame, clientes: DataFrame).
        Retorna (None, None) si ocurre un error durante la lectura.
    """
    try:
        ventas = pd.read_csv(ruta_csv, parse_dates=["fecha"])
        clientes = pd.read_json(ruta_json)
        return ventas, clientes

    except FileNotFoundError as e:
        print(f"Error: {e}. Verifica las rutas de los archivos.")
        return None, None

    except Exception as e:
        print(f"Error inesperado: {e}")
        return None, None


# -----------------------------------------------------------------------------
# Ejecución principal
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    ventas, clientes = cargar_datos(RUTA_CSV, RUTA_JSON)

    if ventas is not None and clientes is not None:
        print("✓ Datos cargados exitosamente.")
    else:
        print("✗ No se pudieron cargar los datos.")
