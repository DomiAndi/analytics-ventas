# Analytics Ventas

Pipeline de procesamiento y análisis de datos de ventas construido con Python y Pandas.

## Descripción

Este proyecto implementa un flujo ETL (Extract, Transform, Load) que carga datos de ventas y clientes, aplica filtros y transformaciones usando lógica equivalente a SQL, y exporta los resultados en archivos CSV y Parquet.

## Estructura del proyecto

```
analytics-ventas/
│
├── analytics.py          # Script principal del pipeline
├── Data/                 # Archivos de entrada
│   ├── ventas.csv        # Datos de ventas
│   └── clientes.json     # Datos de clientes
├── resultados/           # Archivos de salida generados
│   ├── ventas_por_categoria.csv
│   └── top_clientes.parquet
└── README.md
```

## Requisitos

- Python 3.8+
- Pandas

Instalar dependencias:

```bash
pip install pandas pyarrow
```

## Ejecución

```bash
python analytics.py
```

## Cómo funciona

El pipeline se divide en 4 etapas principales:

**1. Variables y Configuración** — Se definen las rutas de archivos, la región de filtro y las categorías válidas como variables globales centralizadas.

**2. Carga de Datos** — La función `cargar_datos()` lee el CSV y el JSON con manejo de errores para `FileNotFoundError` y excepciones inesperadas.

**3. Transformación** — La función `transformar_datos()` aplica tres operaciones equivalentes a SQL:
- `WHERE`: Filtra por región y categorías válidas.
- `SELECT`: Calcula `venta_total` como producto de cantidad por precio.
- `GROUP BY`: Agrupa por región y categoría, calculando suma y promedio.

**4. Análisis Avanzado** — La función `analizar_clientes()` implementa:
- `LEFT JOIN`: Cruza ventas con clientes usando la columna `region`.
- `Window Function`: Calcula el ranking denso por ventas dentro de cada segmento.
- `CTE`: Filtra el top 3 de clientes por segmento.

## Salidas

| Archivo | Formato | Contenido |
|---|---|---|
| ventas_por_categoria.csv | CSV | Ventas agregadas por región y categoría |
| top_clientes.parquet | Parquet | Top 3 clientes por segmento |

## Historial de desarrollo

| Commit | Descripción |
|---|---|
| `feat: carga inicial de datos` | Secciones 1 y 2: variables y lectura de archivos |
| `feat: agregar funciones de transformación` | Sección 3: WHERE, SELECT, GROUP BY |
| `feat: implementar análisis de clientes` | Sección 4: JOIN, Window Function, CTE |
| `merge: resolver conflicto` | Fusión de feature/data-processing → main |

## Autor

Leslie Jimenez
