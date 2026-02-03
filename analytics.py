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
# 4. JOINs y Window Functions
# -----------------------------------------------------------------------------


def analizar_clientes(ventas: pd.DataFrame, clientes: pd.DataFrame) -> pd.DataFrame:
    """
    Combina ventas con clientes y obtiene el top 3 por segmento.

    Pasos internos:
        - LEFT JOIN: Cruza ventas con clientes usando 'region' como clave.
        - WINDOW:    Calcula el ranking denso de cada cliente por total_ventas
                     dentro de su segmento.
        - CTE:       Filtra solo los registros con rank <= 3 (top 3 por segmento).

    Args:
        ventas:    DataFrame de ventas (salida de transformar_datos).
        clientes:  DataFrame de clientes (cargado desde JSON).

    Returns:
        DataFrame con el top 3 de clientes por segmento.
    """
    # LEFT JOIN: Ventas + Clientes
    merged = pd.merge(ventas, clientes, how="left", on="region")

    # Window Function: Ranking denso de clientes por ventas dentro de cada segmento
    merged["rank_ventas"] = (
        merged.groupby("segmento")["total_ventas"]
        .rank(method="dense", ascending=False)
    )

    # CTE (Subconsulta equivalente): filtrar top 3 por segmento
    cte = merged[merged["rank_ventas"] <= 3].copy()

    return cte


# -----------------------------------------------------------------------------
# Ejecución principal
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    ventas, clientes = cargar_datos(RUTA_CSV, RUTA_JSON)

    if ventas is not None and clientes is not None:
        try:
            print("✓ Datos cargados exitosamente.")
            print(f"  - Ventas:   {ventas.shape[0]} filas, {ventas.shape[1]} columnas")
            print(f"  - Clientes: {clientes.shape[0]} filas, {clientes.shape[1]} columnas\n")

            # Transformación principal
            df_ventas = transformar_datos(ventas)
            print("✓ Transformación completada.")
            print(f"  - Región filtrada:   {REGION_FILTRO}")
            print(f"  - Categorías:        {CATEGORIAS_VALIDAS}")
            print(f"  - Filas resultantes: {df_ventas.shape[0]}\n")

            # Análisis avanzado (JOINs + Window Functions)
            top_clientes = analizar_clientes(df_ventas, clientes)
            print("✓ Análisis de clientes completado.")
            print(f"  - Top clientes por segmento: {top_clientes.shape[0]} registros\n")

            # Guardar resultados
            RUTA_RESULTADOS = "resultados"
            df_ventas.to_csv(f"{RUTA_RESULTADOS}/ventas_por_categoria.csv", index=False)
            top_clientes.to_parquet(f"{RUTA_RESULTADOS}/top_clientes.parquet")
            print("✓ Archivos exportados exitosamente.")
            print(f"  - {RUTA_RESULTADOS}/ventas_por_categoria.csv")
            print(f"  - {RUTA_RESULTADOS}/top_clientes.parquet")

        except Exception as e:
            print(f"✗ Error en el procesamiento: {e}")
    else:
        print("✗ No se pudieron cargar los datos. Revisa las rutas configuradas.")
