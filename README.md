# Superprograma Portafolio V3.5

App de escritorio (PyQt6 + matplotlib) para cargar y visualizar portafolio, operaciones y análisis. Persistencia en SQLite (`data/portfolio.db`). Los CSV legacy ya no se usan; solo se migran si la base está vacía.

## Estructura rápida
- `PortafolioFinalV4.py`: ventana principal, wiring de pestañas, formularios y gráficas. Orquesta llamadas a servicios/DB.
- `db_utils.py`: acceso a SQLite (init, CRUD de journal/analysis/portfolio, guardar/leer mercado).
- `market_data.py`: descarga con Selenium y guarda datos de mercado en SQLite.
- `app/ui/analysis_tab.py`: pestaña de Análisis (tabla de símbolos, revisiones, gráfico TradingView).
- `services/portfolio.py`: lógica pura de cálculos (cash por broker, holdings, reconstrucción FIFO de operaciones finalizadas).
- `requirements.txt`: dependencias.
- `data/portfolio.db`: base de datos.

## Uso
1) Crear y activar entorno: `python -m venv .venv` y `.venv\Scripts\activate` (Windows).
2) Instalar: `pip install -r requirements.txt`.
3) Ejecutar: `python PortafolioFinalV4.py`.

## Notas
- Los CSV (`journal/portfolio/Analisis/Combined_Market_Data`) pueden borrarse; sólo se leen para migrar si las tablas están vacías.
- Ajusta la ruta de `chromedriver` en `market_data.py` si usas la descarga automática de mercado.
