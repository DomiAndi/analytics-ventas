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
# 3. Función de Transformación Personalizada
# -----------------------------------------------------------------------------


def transformar_datos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra, transforma y agrega los datos de ventas.

    Pasos internos:
        - WHERE:    Filtra por región (REGION_FILTRO) y categorías válidas (CATEGORIAS_VALIDAS).
        - SELECT:   Calcula la columna 'venta_total' (cantidad * precio).
        - GROUP BY: Agrupa por región y categoría, calculando suma y promedio.

    Args:
        df: DataFrame original de ventas.

    Returns:
        DataFrame agregado con columnas: region, categoria, total_ventas, promedio_precio.
    """
    # WHERE: Filtrar por región y categorías válidas
    mask = (df["region"] == REGION_FILTRO) & (df["categoria"].isin(CATEGORIAS_VALIDAS))
    df_filtrado = df.loc[mask].copy()  # .copy() evita SettingWithCopyWarning

    # SELECT: Crear nueva columna de venta total
    df_filtrado["venta_total"] = df_filtrado["cantidad"] * df_filtrado["precio"]

    # GROUP BY: Ventas agregadas por región y categoría
    df_agregado = df_filtrado.groupby(["region", "categoria"]).agg(
        total_ventas=("venta_total", "sum"),
        promedio_precio=("precio", "mean")
    ).reset_index()

    return df_agregado


# -----------------------------------------------------------------------------
# Ejecución principal
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    ventas, clientes = cargar_datos(RUTA_CSV, RUTA_JSON)

    if ventas is not None and clientes is not None:
        print("✓ Datos cargados exitosamente.")

        df_ventas = transformar_datos(ventas)
        print("✓ Transformación completada.")
        print(f"  - Filas resultantes: {df_ventas.shape[0]}")
    else:
        print("✗ No se pudieron cargar los datos.")
