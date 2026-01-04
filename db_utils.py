import os
import sqlite3
import shutil
from contextlib import contextmanager
from datetime import datetime

import pandas as pd

DB_PATH = os.path.join("data", "portfolio.db")

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS journal (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fecha TEXT NOT NULL,
    tipo TEXT NOT NULL,
    tipo_operacion TEXT NOT NULL,
    simbolo TEXT,
    detalle TEXT,
    cantidad REAL NOT NULL,
    precio REAL NOT NULL,
    rendimiento REAL NOT NULL,
    total_sin_desc REAL NOT NULL,
    comision REAL NOT NULL,
    iva_21 REAL NOT NULL,
    derechos REAL NOT NULL,
    iva_derechos REAL NOT NULL,
    total_descuentos REAL NOT NULL,
    costo_total REAL NOT NULL,
    ingreso_total REAL NOT NULL,
    balance REAL NOT NULL,
    broker TEXT NOT NULL,
    moneda TEXT NOT NULL,
    tc_usd_ars REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS analysis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tipo TEXT NOT NULL,
    simbolo TEXT,
    descripcion TEXT,
    revision TEXT,
    ultima_revision TEXT,
    comentario TEXT
);

CREATE TABLE IF NOT EXISTS portfolio (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    simbolo TEXT NOT NULL,
    broker TEXT NOT NULL,
    tipo TEXT NOT NULL,
    moneda TEXT NOT NULL,
    cantidad REAL NOT NULL,
    precio_prom REAL NOT NULL
);
"""


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    os.makedirs("data", exist_ok=True)
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        conn.commit()


def import_journal_from_csv(csv_path):
    import csv
    rows = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    with get_conn() as conn:
        conn.execute("DELETE FROM journal")
        for r in rows:
            conn.execute(
                """
                INSERT INTO journal (
                    fecha, tipo, tipo_operacion, simbolo, detalle,
                    cantidad, precio, rendimiento, total_sin_desc,
                    comision, iva_21, derechos, iva_derechos,
                    total_descuentos, costo_total, ingreso_total, balance,
                    broker, moneda, tc_usd_ars
                ) VALUES (
                    :Fecha, :Tipo, :Tipo_Operacion, :Simbolo, :Detalle,
                    CAST(:Cantidad AS REAL), CAST(:Precio AS REAL),
                    CAST(:Rendimiento AS REAL), CAST(:Total_Sin_Descuentos AS REAL),
                    CAST(:Comision AS REAL), CAST(:IVA_21 AS REAL),
                    CAST(:Derechos AS REAL), CAST(:IVA_Derechos AS REAL),
                    CAST(:Total_Descuentos AS REAL), CAST(:Costo_Total AS REAL),
                    CAST(:Ingreso_Total AS REAL), CAST(:Balance AS REAL),
                    :Broker, :Moneda, CAST(:TC_USD_ARS AS REAL)
                )
                """,
                r,
            )
        conn.commit()


def import_analysis_from_csv(csv_path):
    import csv
    rows = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    with get_conn() as conn:
        conn.execute("DELETE FROM analysis")
        for r in rows:
            conn.execute(
                """
                INSERT INTO analysis (tipo, simbolo, descripcion, revision, ultima_revision, comentario)
                VALUES (:Tipo, :Simbolo, :Descripcion, :Revision, :UltimaRevision, :Comentario)
                """,
                r,
            )
        conn.commit()


def import_portfolio_from_csv(csv_path):
    import csv
    rows = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    with get_conn() as conn:
        conn.execute("DELETE FROM portfolio")
        for r in rows:
            conn.execute(
                """
                INSERT INTO portfolio (simbolo, broker, tipo, moneda, cantidad, precio_prom)
                VALUES (:Simbolo, :Broker, :Tipo,
                        COALESCE(:Moneda,'ARS'),
                        CAST(:Cantidad AS REAL),
                        CAST(:Precio_Promedio AS REAL))
                """,
                r,
            )
        conn.commit()


def backup_csv_files(csv_paths):
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_dir = os.path.join("data", "backup")
    os.makedirs(backup_dir, exist_ok=True)
    for path in csv_paths:
        if os.path.exists(path):
            base = os.path.basename(path)
            dest = os.path.join(backup_dir, f"{base}.{timestamp}.bak")
            shutil.copy2(path, dest)


def fetch_journal():
    with get_conn() as conn:
        cur = conn.execute("SELECT * FROM journal ORDER BY date(fecha)")
        return [dict(row) for row in cur.fetchall()]


def insert_journal_row(row):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO journal (
                fecha, tipo, tipo_operacion, simbolo, detalle,
                cantidad, precio, rendimiento, total_sin_desc,
                comision, iva_21, derechos, iva_derechos,
                total_descuentos, costo_total, ingreso_total, balance,
                broker, moneda, tc_usd_ars
            ) VALUES (
                :fecha, :tipo, :tipo_operacion, :simbolo, :detalle,
                :cantidad, :precio, :rendimiento, :total_sin_desc,
                :comision, :iva_21, :derechos, :iva_derechos,
                :total_descuentos, :costo_total, :ingreso_total, :balance,
                :broker, :moneda, :tc_usd_ars
            )
            """,
            row,
        )
        conn.commit()


def save_market_data(df):
    """Guarda los datos de mercado combinados en SQLite."""
    if df is None:
        return
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_to_save = df.copy()
    df_to_save["updated_at"] = timestamp
    with get_conn() as conn:
        # Reemplazar la tabla completa en cada actualización
        conn.execute("DROP TABLE IF EXISTS market_data")
        df_to_save.to_sql("market_data", conn, if_exists="replace", index=False)
        conn.commit()


def fetch_market_data():
    """Devuelve DataFrame con datos de mercado y timestamp de actualización."""
    with get_conn() as conn:
        try:
            df = pd.read_sql("SELECT * FROM market_data", conn)
        except Exception:
            return None, None
    last_update = None
    if df is not None and not df.empty and "updated_at" in df.columns:
        last_update = df["updated_at"].iloc[0]
        df = df.drop(columns=["updated_at"])
    return df, last_update


def delete_journal_row_by_id(row_id):
    with get_conn() as conn:
        conn.execute("DELETE FROM journal WHERE id = ?", (row_id,))
        conn.commit()


def fetch_analysis():
    with get_conn() as conn:
        cur = conn.execute("SELECT * FROM analysis")
        return [dict(row) for row in cur.fetchall()]


def save_analysis(rows):
    with get_conn() as conn:
        conn.execute("DELETE FROM analysis")
        for r in rows:
            conn.execute(
                """
                INSERT INTO analysis (tipo, simbolo, descripcion, revision, ultima_revision, comentario)
                VALUES (:tipo, :simbolo, :descripcion, :revision, :ultima_revision, :comentario)
                """,
                r,
            )
        conn.commit()


def replace_portfolio(rows):
    with get_conn() as conn:
        conn.execute("DELETE FROM portfolio")
        for r in rows:
            conn.execute(
                """
                INSERT INTO portfolio (simbolo, broker, tipo, moneda, cantidad, precio_prom)
                VALUES (:simbolo, :broker, :tipo, :moneda, :cantidad, :precio_prom)
                """,
                r,
            )
        conn.commit()
