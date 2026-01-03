# Portfolio App (base)

Pequeña app para seguimiento de portafolio con arquitectura modular en Python.

## Estructura
- `portfolio_app/models`: entidades de dominio (activos, operaciones, portafolio).
- `portfolio_app/data_providers`: proveedor de precios (`yfinance`).
- `portfolio_app/services`: lógica de negocio y métricas.
- `portfolio_app/storage`: almacenamiento en memoria (intercambiable).
- `portfolio_app/cli.py`: ejemplo de uso.

## Setup
1. Crear entorno: `python -m venv .venv` y activar (`.venv\Scripts\activate` en Windows).
2. Instalar dependencias: `pip install -r requirements.txt`.
3. Ejecutar demo: `python -m portfolio_app.cli` (desde la raíz del proyecto).

## Próximos pasos
- Añadir persistencia (CSV/SQLite) reemplazando `InMemoryStore`.
- Agregar tests para `PortfolioService` y `MetricsService`.
- Extender métricas (volatilidad, Sharpe, drawdown) usando histórico de precios.
