# -*- coding: utf-8 -*-
import os
import threading
import json
import smtplib
import ssl
import math
import pandas as pd
import urllib.parse
from datetime import datetime, timedelta
import numpy as np
import sys
import sqlite3
import winreg
import re
import urllib.request
from types import SimpleNamespace

# SOLUCION AL PROBLEMA DE MATPLOTLIB/PYQT6
import matplotlib
matplotlib.use('QtAgg')
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg
from matplotlib.figure import Figure
import matplotlib.pyplot as plt

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QTabWidget, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QLineEdit, QPushButton, QComboBox, QScrollArea, QFrame, QTableWidget,
    QTableWidgetItem, QHeaderView, QAbstractItemView, QMessageBox, QRadioButton,
    QButtonGroup, QGroupBox, QAbstractScrollArea, QSizePolicy, QSplitter,
    QStyleFactory, QCheckBox, QDateEdit, QListWidget, QListWidgetItem
)
from PyQt6.QtCore import Qt, QSize, QUrl, QTimer, QDate, QEvent
from PyQt6.QtGui import QColor, QFont, QBrush, QIcon, QPixmap
from PyQt6.QtWebEngineWidgets import QWebEngineView
from PyQt6.QtWebEngineCore import QWebEngineSettings

from collections import deque
from db_utils import (
    get_conn,
    init_db,
    fetch_journal,
    insert_journal_row,
    delete_journal_row_by_id,
    fetch_analysis,
    save_analysis,
    replace_portfolio,
    import_journal_from_csv,
    import_analysis_from_csv,
    import_portfolio_from_csv,
    upsert_fx_rate,
    fetch_fx_rate,
    fetch_fx_rate_on_or_before,
    fetch_fx_date_bounds,
    upsert_fx_rates_bulk,
    upsert_crypto_price,
    fetch_crypto_price,
    fetch_crypto_prices,
    upsert_crypto_map,
    fetch_crypto_map,
)
from app.ui.analysis_tab import AnalysisTab
from app.ui.threads import DownloadThread
from services.portfolio import (
    compute_cash_by_broker,
    compute_finished_operations,
    compute_holdings_by_broker,
    recompute_portfolio_rows,
    next_plazo_fijo_number,
    calcular_operacion,
    compute_bmb_monthly_volume,
    get_bmb_tier,
    calcular_descuentos_y_totales,
)
from services.market import load_market_data, update_market_data

# Configuracion de datos
DATA_DIR = "data"
LEGACY_JOURNAL = os.path.join(DATA_DIR, "journal.csv")
LEGACY_PORTFOLIO = os.path.join(DATA_DIR, "portfolio.csv")
LEGACY_ANALYSIS = os.path.join(DATA_DIR, "Analisis.csv")

# Brokers disponibles
BROKERS = ["IOL", "BMB", "COCOS", "BALANZ", "BINANCE", "KUCOIN", "BYBIT", "BINGX"]
CURRENCIES = ["ARS", "USD"]
DOLARHOY_URLS = {
    "mep": "https://dolarhoy.com/cotizacion-dolar-mep",
    "ccl": "https://dolarhoy.com/cotizacion-dolar-contado-con-liqui",
    "oficial": "https://dolarhoy.com/cotizacion-dolar-oficial",
    "blue": "https://dolarhoy.com/cotizacion-dolar-blue",
}

AMBITO_ENDPOINTS = {
    "mep": "https://mercados.ambito.com/dolarrava/mep/historico-general/",
    "ccl": "https://mercados.ambito.com/dolarrava/cl/historico-general/",
    "blue": "https://mercados.ambito.com/dolar/informal/historico-general/",
    "cripto": "https://mercados.ambito.com/dolarcripto/grafico/",
}
FX_SOURCE_BY_KIND = {
    "mep": "ambito",
    "ccl": "ambito",
    "blue": "ambito",
    "cripto": "ambito",
    "oficial": "bcra",
}
FX_BACKFILL_START = datetime(2020, 1, 1)
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER)
SMTP_TO = os.getenv("SMTP_TO", SMTP_USER)
EMAIL_SUBJECT_PREFIX = os.getenv("EMAIL_SUBJECT_PREFIX", "Superprograma Portfolio")
CONFIG_PATH = os.path.join(DATA_DIR, "app_config.json")
COINGECKO_SIMPLE_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price"
COINGECKO_SEARCH_URL = "https://api.coingecko.com/api/v3/search"
CRYPTO_UPDATE_INTERVAL_MIN = 15
COINGECKO_SYMBOL_MAP = {
    "btc": "bitcoin",
    "eth": "ethereum",
    "usdt": "tether",
    "usdc": "usd-coin",
    "bnb": "binancecoin",
    "xrp": "ripple",
    "ada": "cardano",
    "sol": "solana",
    "dot": "polkadot",
    "doge": "dogecoin",
    "matic": "polygon",
    "link": "chainlink",
    "ltc": "litecoin",
    "bch": "bitcoin-cash",
    "avax": "avalanche-2",
    "trx": "tron",
    "uni": "uniswap",
    "atom": "cosmos",
    "near": "near",
    "shib": "shiba-inu",
    "xmr": "monero",
    "xlm": "stellar",
    "etc": "ethereum-classic",
    "ton": "the-open-network",
}


def load_app_config():
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_app_config(config):
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(config, f)
    except Exception as e:
        print(f"Error guardando config: {e}")


def send_email(subject, body):
    if not SMTP_USER or not SMTP_PASS or not SMTP_TO:
        print("SMTP no configurado para enviar notificaciones.")
        return False
    message = f"From: {SMTP_FROM}\r\nTo: {SMTP_TO}\r\nSubject: {subject}\r\n\r\n{body}"
    context = ssl.create_default_context()
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as server:
            server.ehlo()
            server.starttls(context=context)
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_FROM, [SMTP_TO], message)
        return True
    except Exception as e:
        print(f"Error enviando mail: {e}")
        return False

# Crear directorio si no existe
os.makedirs(DATA_DIR, exist_ok=True)

# Inicializar base de datos
def init_files():
    os.makedirs(DATA_DIR, exist_ok=True)
    init_db()
    journal_empty = analysis_empty = portfolio_empty = False
    try:
        with get_conn() as conn:
            journal_empty = conn.execute("SELECT COUNT(*) FROM journal").fetchone()[0] == 0
            analysis_empty = conn.execute("SELECT COUNT(*) FROM analysis").fetchone()[0] == 0
            portfolio_empty = conn.execute("SELECT COUNT(*) FROM portfolio").fetchone()[0] == 0
    except Exception as e:
        print(f"Error verificando datos iniciales: {e}")

    if journal_empty and os.path.exists(LEGACY_JOURNAL):
        try:
            import_journal_from_csv(LEGACY_JOURNAL)
        except Exception as e:
            print(f"Error migrando journal.csv: {e}")
    if analysis_empty and os.path.exists(LEGACY_ANALYSIS):
        try:
            import_analysis_from_csv(LEGACY_ANALYSIS)
        except Exception as e:
            print(f"Error migrando Analisis.csv: {e}")
    if portfolio_empty and os.path.exists(LEGACY_PORTFOLIO):
        try:
            import_portfolio_from_csv(LEGACY_PORTFOLIO)
        except Exception as e:
            print(f"Error migrando portfolio.csv: {e}")

# Clase principal de la aplicación
class PortfolioAppQt(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Gestor de Portafolio de Inversiones")
        self.setGeometry(100, 100, 1400, 900)

        init_files()
        self.fx_backfill_done = False
        self.fx_update_running = False
        self.crypto_update_running = False
        self.notify_state_path = os.path.join(DATA_DIR, "notify_state.json")

        # Crear widget central y layout principal
        central_widget = QWidget()
        self.main_layout = QVBoxLayout(central_widget)
        self.setCentralWidget(central_widget)  # <-- Añadir esta línea

        self.init_themes()
        header_frame = QFrame()
        header_layout = QHBoxLayout(header_frame)
        header_layout.setContentsMargins(0, 0, 0, 0)
        header_layout.addStretch()
        self.mode_toggle_btn = QPushButton("Usuario")
        self.mode_toggle_btn.setCheckable(True)
        self.mode_toggle_btn.toggled.connect(self.toggle_developer_mode)
        header_layout.addWidget(self.mode_toggle_btn)
        self.theme_toggle_btn = QPushButton("Modo oscuro")
        self.theme_toggle_btn.clicked.connect(self.toggle_theme)
        header_layout.addWidget(self.theme_toggle_btn)
        self.main_layout.addWidget(header_frame)

        self.df_mercado = None
        self.last_update = None
        self.cargar_datos_mercado()
        self.default_fx_rate = 1.0
        self.broker_colors = {
            "IOL": "#4e79a7",
            "BMB": "#f28e2b",
            "COCOS": "#59a14f",
            "BALANZ": "#e15759",
            "BINANCE": "#76b7b2",
            "KUCOIN": "#edc948",
            "BYBIT": "#b07aa1",
            "BINGX": "#ff9da7",
        }
        QTimer.singleShot(3000, lambda: self.start_fx_update_thread(run_backfill=True))

        # Inicializar atributos para actualización automática
        self.auto_update_interval = 5  # Valor por defecto en minutos
        self.auto_update_active = False
        self.remaining_time = 0  # Tiempo restante en segundos

        self.auto_update_timer = QTimer(self)
        self.auto_update_timer.timeout.connect(self.actualizar_datos_mercado)
        self.countdown_timer = QTimer(self)  # Nuevo timer para cuenta regresiva
        self.countdown_timer.timeout.connect(self.update_countdown)

        self.crypto_update_timer = QTimer(self)
        self.crypto_update_timer.timeout.connect(self.start_crypto_update_thread)
        self.crypto_update_timer.start(CRYPTO_UPDATE_INTERVAL_MIN * 60 * 1000)
        QTimer.singleShot(3000, self.start_crypto_update_thread)

        # Crear pestañas principales
        self.tabs = QTabWidget()
        self.main_layout.addWidget(self.tabs)

        # Pestaña principal de Operaciones (contendrá subpestañas)
        self.operations_tab = QWidget()
        self.operations_tab_layout = QVBoxLayout(self.operations_tab)
        self.operations_inner_tabs = QTabWidget()
        self.operations_tab_layout.addWidget(self.operations_inner_tabs)

        # Crear subpestañas para Operaciones
        self.operation_subtab = QWidget()
        self.finished_ops_subtab = QWidget()
        self.journal_subtab = QWidget()

        self.operations_inner_tabs.addTab(self.operation_subtab, "Nueva Operación")
        self.operations_inner_tabs.addTab(self.finished_ops_subtab, "Operaciones Finalizadas")
        self.operations_inner_tabs.addTab(self.journal_subtab, "Libro Diario")

        # Otras pestañas principales
        self.user_portfolio_tab = QWidget()
        self.dev_portfolio_tab = QWidget()
        self.analysis_tab = AnalysisTab(self)

        # Agregar pestañas principales
        self.tabs.addTab(self.operations_tab, "Operaciones")
        self.tabs.addTab(self.user_portfolio_tab, "Portafolio Actual (Usuario)")
        self.tabs.addTab(self.dev_portfolio_tab, "Portafolio Actual")
        self.tabs.addTab(self.analysis_tab, "Análisis")

        # Inicializar subpestañas con los widgets existentes
        self.create_operation_form(self.operation_subtab)
        self.create_finished_ops_view(self.finished_ops_subtab)
        self.create_journal_view(self.journal_subtab)
        self.user_portfolio_view = self.create_portfolio_view(self.user_portfolio_tab, is_user=True)
        self.dev_portfolio_view = self.create_portfolio_view(self.dev_portfolio_tab, is_user=False)
        self.portfolio_views = [self.user_portfolio_view, self.dev_portfolio_view]
        self.portfolio_views_by_tab = {
            self.user_portfolio_tab: self.user_portfolio_view,
            self.dev_portfolio_tab: self.dev_portfolio_view,
        }
        self.configure_user_portfolio_headers()
        self.tabs.setCurrentWidget(self.user_portfolio_tab)
        self.set_navigation_mode(False)
        self.apply_theme(self.detect_system_theme(), refresh_tables=False)

        self.compras_pendientes = {}
        self.load_compras_pendientes()
        self.recalcular_portfolio()
        self.refresh_portfolios()
        self.load_journal()
        self.plazo_fijo_counter = self.get_next_plazo_fijo_number()

        self.tabs.currentChanged.connect(self.on_tab_changed)
        # Conectar cambio de subpestañas en Operaciones
        self.operations_inner_tabs.currentChanged.connect(self.on_inner_tab_changed)


    def detect_system_theme(self):
        try:
            with winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                r"Software\\Microsoft\\Windows\\CurrentVersion\\Themes\\Personalize"
            ) as key:
                value, _ = winreg.QueryValueEx(key, "AppsUseLightTheme")
                return "light" if value == 1 else "dark"
        except OSError:
            return "light"

    def init_themes(self):
        self.themes = {
            "light": {
                "filter_info_color": "#1b5fd1",
                "table_category_bg": "#e6e6e6",
                "table_category_alt_bg": "#f0f0f0",
                "table_total_bg": "#dcdcdc",
                "chart_bg": "#ffffff",
                "chart_text": "#1a1a1a",
                "card_bg": "#ffffff",
                "card_border": "#d8d8d8",
                "card_text": "#1a1a1a",
                "gain_color": "#1e8e3e",
                "loss_color": "#b3261e",
                "qss": (
                    "QWidget { background: #f6f7f9; color: #1a1a1a; }"
                    "QMainWindow { background: #f6f7f9; }"
                    "QTabWidget::pane { border: 1px solid #c9c9c9; background: #ffffff; }"
                    "QTabBar::tab { background: #e9eaee; padding: 6px 12px; border: 1px solid #c9c9c9; border-bottom: none; }"
                    "QTabBar::tab:selected { background: #ffffff; font-weight: 600; }"
                    "QLineEdit, QComboBox, QDateEdit { background: #ffffff; border: 1px solid #c9c9c9; padding: 4px; border-radius: 4px; }"
                    "QTableWidget { background: #ffffff; gridline-color: #d8d8d8; }"
                    "QHeaderView::section { background: #e9eaee; padding: 4px; border: 1px solid #c9c9c9; }"
                    "QPushButton { background: #1f6feb; color: #ffffff; border: none; padding: 6px 12px; border-radius: 4px; }"
                    "QPushButton:hover { background: #1b5fd1; }"
                    "QPushButton:pressed { background: #174fb0; }"
                    "QPushButton:disabled { background: #a7a7a7; color: #f2f2f2; }"
                    "QGroupBox { border: 1px solid #c9c9c9; margin-top: 8px; }"
                    "QGroupBox::title { subcontrol-origin: margin; left: 8px; padding: 0 4px; }"
                    "QScrollArea { border: none; }"
                    "QCheckBox, QRadioButton { spacing: 6px; }"
                ),
            },
            "dark": {
                "filter_info_color": "#8ab4f8",
                "table_category_bg": "#2a313b",
                "table_category_alt_bg": "#242b33",
                "table_total_bg": "#303845",
                "chart_bg": "#1b1f24",
                "chart_text": "#e6e6e6",
                "card_bg": "#1b1f24",
                "card_border": "#2f3742",
                "card_text": "#e6e6e6",
                "gain_color": "#25c26e",
                "loss_color": "#ff5f5f",
                "qss": (
                    "QWidget { background: #14171a; color: #e6e6e6; }"
                    "QMainWindow { background: #14171a; }"
                    "QTabWidget::pane { border: 1px solid #2f3742; background: #1b1f24; }"
                    "QTabBar::tab { background: #232831; padding: 6px 12px; border: 1px solid #2f3742; border-bottom: none; }"
                    "QTabBar::tab:selected { background: #1b1f24; font-weight: 600; }"
                    "QLineEdit, QComboBox, QDateEdit { background: #1b1f24; border: 1px solid #3a424d; padding: 4px; border-radius: 4px; }"
                    "QTableWidget { background: #1b1f24; gridline-color: #2f3742; }"
                    "QHeaderView::section { background: #232831; padding: 4px; border: 1px solid #2f3742; }"
                    "QPushButton { background: #2d6cdf; color: #ffffff; border: none; padding: 6px 12px; border-radius: 4px; }"
                    "QPushButton:hover { background: #255fcb; }"
                    "QPushButton:pressed { background: #1f52b0; }"
                    "QPushButton:disabled { background: #4a5563; color: #c8c8c8; }"
                    "QGroupBox { border: 1px solid #2f3742; margin-top: 8px; }"
                    "QGroupBox::title { subcontrol-origin: margin; left: 8px; padding: 0 4px; }"
                    "QScrollArea { border: none; }"
                    "QCheckBox, QRadioButton { spacing: 6px; }"
                ),
            },
        }
        self.current_theme = "light"

    def toggle_developer_mode(self, checked):
        self.set_navigation_mode(checked)

    def set_navigation_mode(self, developer_mode):
        self.developer_mode = developer_mode
        if developer_mode:
            self.mode_toggle_btn.setText("Usuario")
            self.tabs.tabBar().show()
            self.operations_inner_tabs.tabBar().show()
        else:
            self.mode_toggle_btn.setText("Desarrollador")
            self.tabs.setCurrentWidget(self.user_portfolio_tab)
            self.tabs.tabBar().hide()
            self.operations_inner_tabs.tabBar().hide()

    def get_theme_value(self, key, fallback):
        theme = self.themes.get(self.current_theme, {})
        return theme.get(key, fallback)

    def toggle_theme(self):
        new_theme = "dark" if self.current_theme == "light" else "light"
        self.apply_theme(new_theme)

    def apply_theme(self, theme_key, refresh_tables=True):
        theme = self.themes.get(theme_key, self.themes["light"])
        self.current_theme = theme_key
        app = QApplication.instance()
        if app:
            app.setStyleSheet(theme["qss"])
        if hasattr(self, "filter_info_label"):
            self.filter_info_label.setStyleSheet(f"color: {theme['filter_info_color']};")
        if hasattr(self, "theme_toggle_btn"):
            self.theme_toggle_btn.setText("Modo claro" if theme_key == "dark" else "Modo oscuro")
        if refresh_tables:
            self.refresh_portfolios()

    def get_portfolio_views(self):
        return getattr(self, "portfolio_views", [])

    def refresh_portfolios(self):
        for view in self.get_portfolio_views():
            self.load_portfolio(view)

    def compute_bmb_tier(self, broker, fecha_dt):
        if (broker or "").upper() != "BMB":
            return None
        journal_rows = fetch_journal()
        first_of_month = fecha_dt.replace(day=1)
        prev_month_last = first_of_month - timedelta(days=1)
        volume = compute_bmb_monthly_volume(journal_rows, prev_month_last)
        return get_bmb_tier(volume)

    def is_intraday_bonus(
        self,
        broker,
        tipo,
        tipo_op,
        simbolo,
        cantidad,
        moneda,
        plazo,
        fecha_dt,
    ):
        if (broker or "").upper() != "BMB":
            return False
        if tipo not in ["Acciones AR", "CEDEARs", "Bonos AR"]:
            return False
        if tipo_op not in ["Compra", "Venta"]:
            return False
        if not simbolo:
            return False
        fecha_str = fecha_dt.strftime("%Y-%m-%d")
        opposite = "Venta" if tipo_op == "Compra" else "Compra"
        for row in fetch_journal():
            if row.get("broker") != broker:
                continue
            if row.get("tipo") != tipo:
                continue
            if row.get("tipo_operacion") != opposite:
                continue
            if row.get("simbolo") != simbolo:
                continue
            if row.get("moneda") != moneda:
                continue
            if (row.get("plazo") or "T+1") != plazo:
                continue
            if row.get("fecha") != fecha_str:
                continue
            try:
                row_cantidad = float(row.get("cantidad", 0) or 0)
            except Exception:
                row_cantidad = 0
            if abs(row_cantidad - cantidad) <= 1e-6:
                return True
        return False

    def get_fx_kind_for_tipo(self, tipo):
        if tipo in ["CEDEARs", "ETFs"]:
            return "ccl"
        if tipo in ["Criptomonedas"]:
            return "cripto"
        return "mep"

    def fetch_dolarhoy_rate(self, url):
        try:
            html = urllib.request.urlopen(url, timeout=10).read().decode("utf-8", errors="ignore")
        except Exception as e:
            print(f"Error leyendo {url}: {e}")
            return None, None

        compra_match = re.search(r"Compra</div>\\s*<div class=\"value\">\\$\\s*([0-9\\.,]+)", html)
        venta_match = re.search(r"Venta</div>\\s*<div class=\"value\">\\$\\s*([0-9\\.,]+)", html)
        if not compra_match and not venta_match:
            return None, None

        def _parse(val):
            return float(val.replace(".", "").replace(",", "."))

        compra = _parse(compra_match.group(1)) if compra_match else None
        venta = _parse(venta_match.group(1)) if venta_match else None
        return compra, venta

    def _parse_decimal(self, value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text:
            return None
        if "," in text:
            text = text.replace(".", "").replace(",", ".")
        return float(text)

    def _parse_ambito_date(self, value):
        if not value:
            return None
        text = str(value).strip()
        for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        return None

    def _load_notify_state(self):
        try:
            with open(self.notify_state_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_notify_state(self, state):
        try:
            with open(self.notify_state_path, "w", encoding="utf-8") as f:
                json.dump(state, f)
        except Exception as e:
            print(f"Error guardando notify_state: {e}")

    def _should_send_notification(self, key):
        state = self._load_notify_state()
        today = datetime.now().strftime("%Y-%m-%d")
        if state.get(key) == today:
            return False
        state[key] = today
        self._save_notify_state(state)
        return True

    def _send_notification(self, subject, body, key):
        if datetime.now().weekday() >= 5:
            return
        if not self._should_send_notification(key):
            return
        subject_line = f"{EMAIL_SUBJECT_PREFIX} - {subject}"
        send_email(subject_line, body)

    def _fetch_ambito_series(self, tipo, start_dt, end_dt):
        base = AMBITO_ENDPOINTS.get(tipo)
        if not base:
            return {}
        start_str = start_dt.strftime("%Y-%m-%d")
        end_str = end_dt.strftime("%Y-%m-%d")
        url = f"{base}{start_str}/{end_str}"
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            raw = urllib.request.urlopen(req, timeout=10).read().decode("utf-8", errors="ignore")
            data = json.loads(raw)
        except Exception as e:
            print(f"Error leyendo Ambito {tipo}: {e}")
            return {}

        if not isinstance(data, list) or len(data) < 2:
            return {}
        series = {}
        for row in data[1:]:
            if not row or len(row) < 2:
                continue
            dt = self._parse_ambito_date(row[0])
            val = self._parse_decimal(row[1])
            if dt and val is not None:
                series[dt.strftime("%Y-%m-%d")] = float(val)
        return series

    def _fill_missing_dates(self, start_dt, end_dt, series):
        filled = {}
        current = start_dt
        last_val = None
        while current <= end_dt:
            key = current.strftime("%Y-%m-%d")
            if key in series:
                last_val = series[key]
                filled[key] = last_val
            elif last_val is not None:
                filled[key] = last_val
            current += timedelta(days=1)
        return filled

    def update_fx_rates_from_ambito_range(self, tipo, start_dt, end_dt):
        series = self._fetch_ambito_series(tipo, start_dt, end_dt)
        if not series:
            return False, f"Sin datos Ambito {tipo} {start_dt:%Y-%m-%d} a {end_dt:%Y-%m-%d}"
        filled = self._fill_missing_dates(start_dt, end_dt, series)
        rows = [
            (fecha, tipo, "ambito", val, val)
            for fecha, val in filled.items()
        ]
        upsert_fx_rates_bulk(rows)
        return True, None

    def ensure_fx_backfill(self):
        if self.fx_backfill_done:
            return []
        today = datetime.now()
        errors = []
        for tipo in AMBITO_ENDPOINTS:
            min_date, _ = fetch_fx_date_bounds(tipo, "ambito")
            if min_date and min_date <= FX_BACKFILL_START.strftime("%Y-%m-%d"):
                continue
            current = FX_BACKFILL_START
            while current <= today:
                chunk_end = min(current + timedelta(days=180), today)
                ok, err = self.update_fx_rates_from_ambito_range(tipo, current, chunk_end)
                if not ok and err:
                    errors.append(err)
                current = chunk_end + timedelta(days=1)
        self.fx_backfill_done = True
        return errors

    def update_fx_rates_from_ambito_daily(self):
        today = datetime.now()
        errors = []
        for tipo in AMBITO_ENDPOINTS:
            _, max_date = fetch_fx_date_bounds(tipo, "ambito")
            if not max_date:
                continue
            try:
                last_dt = datetime.strptime(max_date, "%Y-%m-%d")
            except ValueError:
                last_dt = today - timedelta(days=7)
            start_dt = max(last_dt - timedelta(days=7), FX_BACKFILL_START)
            if start_dt > today:
                continue
            ok, err = self.update_fx_rates_from_ambito_range(tipo, start_dt, today)
            if not ok and err:
                errors.append(err)
        return errors

    def update_fx_rates_from_bcra(self):
        token = os.getenv("BCRA_API_TOKEN", "").strip()
        if not token:
            config = load_app_config()
            token = str(config.get("BCRA_API_TOKEN", "")).strip()
        if not token:
            print("BCRA_API_TOKEN no configurado. Salteando tipo de cambio oficial.")
            return "BCRA_API_TOKEN no configurado."
        url = "https://api.estadisticasbcra.com/usd_of"
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"BEARER {token}",
                "User-Agent": USER_AGENT,
            },
        )
        try:
            raw = urllib.request.urlopen(req, timeout=10).read().decode("utf-8", errors="ignore")
            data = json.loads(raw)
        except Exception as e:
            print(f"Error leyendo BCRA: {e}")
            return f"Error leyendo BCRA: {e}"
        if not isinstance(data, list):
            return "Respuesta BCRA invalida."
        _, max_date = fetch_fx_date_bounds("oficial", "bcra")
        last_dt = None
        if max_date:
            try:
                last_dt = datetime.strptime(max_date, "%Y-%m-%d")
            except ValueError:
                last_dt = None
        rows = []
        for row in data:
            fecha = row.get("d")
            val = row.get("v")
            dt = None
            try:
                dt = datetime.strptime(fecha, "%Y-%m-%d")
            except Exception:
                dt = None
            if not dt:
                continue
            if last_dt and dt <= last_dt:
                continue
            value = self._parse_decimal(val)
            if value is None:
                continue
            rows.append((dt.strftime("%Y-%m-%d"), "oficial", "bcra", value, value))
        upsert_fx_rates_bulk(rows)
        return None

    def update_fx_rates_from_sources(self, run_backfill=False):
        errors = []
        if run_backfill:
            try:
                errors.extend(self.ensure_fx_backfill())
            except Exception as e:
                errors.append(f"Backfill FX falló: {e}")
        try:
            errors.extend(self.update_fx_rates_from_ambito_daily())
        except Exception as e:
            errors.append(f"Actualización FX Ambito falló: {e}")
        try:
            err = self.update_fx_rates_from_bcra()
            if err:
                errors.append(err)
        except Exception as e:
            errors.append(f"Actualización FX BCRA falló: {e}")
        if errors:
            body = "Se detectaron errores en la actualización de tipos de cambio:\n\n"
            body += "\n".join(f"- {e}" for e in errors)
            self._send_notification("Fallo de actualización FX", body, "fx_update_error")

    def start_fx_update_thread(self, run_backfill=False):
        if self.fx_update_running:
            return
        self.fx_update_running = True

        def _worker():
            try:
                self.update_fx_rates_from_sources(run_backfill=run_backfill)
            finally:
                self.fx_update_running = False

        threading.Thread(target=_worker, daemon=True).start()

    def get_crypto_symbols(self):
        symbols = []
        try:
            with get_conn() as conn:
                rows = conn.execute(
                    "SELECT DISTINCT simbolo FROM portfolio WHERE tipo = 'Criptomonedas'"
                ).fetchall()
                for row in rows:
                    sym = (row["simbolo"] or "").strip().upper()
                    if sym:
                        symbols.append(sym)
        except Exception as e:
            print(f"Error leyendo simbolos cripto: {e}")
        return sorted(set(symbols))

    def resolve_coingecko_id(self, symbol):
        sym = symbol.strip().lower()
        if not sym:
            return None
        cached = fetch_crypto_map(sym.upper())
        if cached:
            return cached.get("coingecko_id")
        if sym in COINGECKO_SYMBOL_MAP:
            coingecko_id = COINGECKO_SYMBOL_MAP[sym]
            upsert_crypto_map(sym.upper(), coingecko_id)
            return coingecko_id
        try:
            query = urllib.parse.quote(sym)
            url = f"{COINGECKO_SEARCH_URL}?query={query}"
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            raw = urllib.request.urlopen(req, timeout=10).read().decode("utf-8", errors="ignore")
            data = json.loads(raw)
            coins = data.get("coins", [])
            for coin in coins:
                if coin.get("symbol", "").lower() == sym:
                    coingecko_id = coin.get("id")
                    if coingecko_id:
                        upsert_crypto_map(sym.upper(), coingecko_id)
                        return coingecko_id
            if coins:
                coingecko_id = coins[0].get("id")
                if coingecko_id:
                    upsert_crypto_map(sym.upper(), coingecko_id)
                    return coingecko_id
        except Exception as e:
            print(f"Error resolviendo CoinGecko id para {symbol}: {e}")
        return None

    def update_crypto_prices(self):
        symbols = self.get_crypto_symbols()
        if not symbols:
            return
        ids = []
        symbol_by_id = {}
        for sym in symbols:
            coingecko_id = self.resolve_coingecko_id(sym)
            if coingecko_id:
                ids.append(coingecko_id)
                symbol_by_id[coingecko_id] = sym
        if not ids:
            return
        ids_str = ",".join(sorted(set(ids)))
        url = (
            f"{COINGECKO_SIMPLE_PRICE_URL}?ids={urllib.parse.quote(ids_str)}"
            "&vs_currencies=usd&include_24hr_change=true"
        )
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            raw = urllib.request.urlopen(req, timeout=10).read().decode("utf-8", errors="ignore")
            data = json.loads(raw)
        except Exception as e:
            print(f"Error leyendo precios cripto: {e}")
            return
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for coingecko_id, payload in data.items():
            price = payload.get("usd")
            change_24h = payload.get("usd_24h_change")
            if price is None:
                continue
            symbol = symbol_by_id.get(coingecko_id)
            if symbol:
                change_val = self._safe_number(change_24h, None)
                upsert_crypto_price(symbol, float(price), now, change_val)

    def start_crypto_update_thread(self):
        if self.crypto_update_running:
            return
        self.crypto_update_running = True

        def _worker():
            try:
                self.update_crypto_prices()
            finally:
                self.crypto_update_running = False
                QTimer.singleShot(0, self.refresh_portfolios)

        threading.Thread(target=_worker, daemon=True).start()

    def get_fx_rate_for_date(self, fecha_dt, tipo):
        kind = self.get_fx_kind_for_tipo(tipo)
        fecha_str = fecha_dt.strftime("%Y-%m-%d")
        fuente = FX_SOURCE_BY_KIND.get(kind, "dolarhoy")
        row = fetch_fx_rate(fecha_str, kind, fuente)
        if row:
            return row.get("venta") or row.get("compra")
        row = fetch_fx_rate_on_or_before(fecha_str, kind, fuente)
        if row:
            return row.get("venta") or row.get("compra")
        if fuente != "dolarhoy":
            row = fetch_fx_rate(fecha_str, kind, "dolarhoy")
            if row:
                return row.get("venta") or row.get("compra")
            row = fetch_fx_rate_on_or_before(fecha_str, kind, "dolarhoy")
            if row:
                return row.get("venta") or row.get("compra")
        return None

    def _safe_number(self, value, default=0.0):
        try:
            num = float(value)
        except Exception:
            return default
        if math.isnan(num) or math.isinf(num):
            return default
        return num

    def get_ccl_rate_for_date(self, fecha_dt):
        fecha_str = fecha_dt.strftime("%Y-%m-%d")
        fuente = FX_SOURCE_BY_KIND.get("ccl", "dolarhoy")
        row = fetch_fx_rate(fecha_str, "ccl", fuente)
        if row:
            return row.get("venta") or row.get("compra")
        row = fetch_fx_rate_on_or_before(fecha_str, "ccl", fuente)
        if row:
            return row.get("venta") or row.get("compra")
        if fuente != "dolarhoy":
            row = fetch_fx_rate(fecha_str, "ccl", "dolarhoy")
            if row:
                return row.get("venta") or row.get("compra")
            row = fetch_fx_rate_on_or_before(fecha_str, "ccl", "dolarhoy")
            if row:
                return row.get("venta") or row.get("compra")
        return None


    def update_default_fx_rate(self):
        today = datetime.now()
        rate = self.get_fx_rate_for_date(today, "Acciones AR")
        if rate:
            self.default_fx_rate = rate

    def update_portfolio_summary(self, view, result_ars, result_usd, pct_ars, pct_usd, debug=None):
        if not getattr(view, "is_user", False):
            return
        card_bg = self.get_theme_value("card_bg", "#1b1f24")
        card_border = self.get_theme_value("card_border", "#2f3742")
        card_text = self.get_theme_value("card_text", "#e6e6e6")
        gain_color = self.get_theme_value("gain_color", "#25c26e")
        loss_color = self.get_theme_value("loss_color", "#ff5f5f")
        accent = self.get_theme_value("accent_color", "#3b82f6")
        view.summary_frame.setStyleSheet(
            f"QFrame#portfolioSummary {{ background: {card_bg}; border: 1px solid {card_border}; border-radius: 8px; }}"
        )
        view.summary_title_label.setText("GAN/PER ABIERTA (sin ventas)")
        view.summary_title_label.setStyleSheet(f"font-weight: 600; color: {card_text};")

        def fmt(num):
            sign = "-" if num < 0 else ""
            return f"{sign}$ {abs(num):,.2f}"

        ars_color = gain_color if result_ars >= 0 else loss_color
        usd_color = gain_color if result_usd >= 0 else loss_color
        view.summary_ars_label.setText(f"ARS: {fmt(result_ars)} ({pct_ars:.2f}%)")
        view.summary_usd_label.setText(f"USD: {fmt(result_usd)} ({pct_usd:.2f}%)")
        if debug:
            view.summary_ars_label.setToolTip(debug)
            view.summary_usd_label.setToolTip(debug)
        view.summary_ars_label.setStyleSheet(f"color: {ars_color}; font-weight: 600;")
        view.summary_usd_label.setStyleSheet(f"color: {usd_color}; font-weight: 600;")

        selected = getattr(view, "asset_currency_view", "ARS")
        ars_border = accent if selected == "ARS" else card_border
        usd_border = accent if selected == "USD" else card_border
        view.summary_ars_card.setStyleSheet(
            f"QFrame {{ background: {card_bg}; border: 1px solid {ars_border}; border-radius: 8px; }}"
        )
        view.summary_usd_card.setStyleSheet(
            f"QFrame {{ background: {card_bg}; border: 1px solid {usd_border}; border-radius: 8px; }}"
        )

    def configure_user_portfolio_headers(self):
        view = self.user_portfolio_view
        view.portfolio_table.setColumnCount(16)
        view.portfolio_table.setHorizontalHeaderLabels([
            "Categoría",
            "Ticker",
            "Moneda",
            "Cantidad\nNominal",
            "Variación\nDiaria",
            "PPC",
            "Capital Total Invertido",
            "Precio\nÚltimo Operado",
            "Valor\nActual (moneda)",
            "Valor ARS",
            "Valor USD",
            "% del\nPortafolio (ARS)",
            "Resultado ARS (%)",
            "Resultado USD",
            "Comisiones ARS",
            "Comisiones USD"
        ])
        header_ppc = view.portfolio_table.horizontalHeaderItem(5)
        if header_ppc:
            header_ppc.setToolTip("PRECIO PROMEDIO DE COMPRA")

    def update_asset_view_headers(self, view):
        currency = getattr(view, "asset_currency_view", "ARS")
        if currency not in ("ARS", "USD"):
            currency = "ARS"
        header_map = {
            5: f"PPC ({currency})",
            6: f"Capital Total Invertido ({currency})",
            7: f"Precio\nÚltimo Operado ({currency})",
            8: f"Valor\nActual ({currency})",
            11: f"% del\nPortafolio ({currency})",
            12: "Resultado ARS (%)",
            13: "Resultado USD (%)",
        }
        for col, text in header_map.items():
            item = view.portfolio_table.horizontalHeaderItem(col)
            if item:
                item.setText(text)

    def apply_currency_column_visibility(self, view):
        if not getattr(view, "is_user", False):
            for col in [9, 10, 11, 12, 13, 14, 15]:
                if col < view.portfolio_table.columnCount():
                    view.portfolio_table.setColumnHidden(col, False)
            return
        currency = getattr(view, "asset_currency_view", "ARS")
        show_ars = currency == "ARS"
        show_usd = currency == "USD"
        if currency not in ("ARS", "USD"):
            show_ars = True
            show_usd = True

        ars_columns = [9, 11, 12, 14]
        usd_columns = [10, 13, 15]
        for col in ars_columns:
            if col < view.portfolio_table.columnCount():
                view.portfolio_table.setColumnHidden(col, not show_ars)
        for col in usd_columns:
            if col < view.portfolio_table.columnCount():
                view.portfolio_table.setColumnHidden(col, not show_usd)
        self.update_asset_view_headers(view)

    def adjust_portfolio_table(self, view):
        header = view.portfolio_table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        header.setStretchLastSection(True)
        view.portfolio_table.resizeColumnsToContents()
        view.portfolio_table.resizeRowsToContents()

    def update_status_labels(self, message=None):
        if message is None:
            message = f"Última actualización: {self.last_update if self.last_update else '-'}"
        for view in self.get_portfolio_views():
            view.status_label.setText(message)

    def sync_auto_update_controls(self, source_view=None):
        for view in self.get_portfolio_views():
            if source_view is not None and view is source_view:
                continue
            view.auto_update_check.blockSignals(True)
            view.auto_update_check.setChecked(self.auto_update_active)
            view.auto_update_check.blockSignals(False)
            view.interval_edit.blockSignals(True)
            view.interval_edit.setText(str(self.auto_update_interval))
            view.interval_edit.blockSignals(False)

    def get_broker_color(self, broker):
        return self.broker_colors.get(broker, "#9aa0a6")

    def toggle_liquidity_chart(self, view):
        if not getattr(view, "is_user", False):
            return
        if getattr(view, "liquidity_overlay", None) is None:
            return
        if view.liquidity_overlay.isVisible():
            self.close_liquidity_overlay(view)
        else:
            self.open_liquidity_overlay(view)

    def set_user_grouping(self, grouping, view):
        if not getattr(view, "is_user", False):
            return
        view.group_by = grouping
        view.group_by_tipo_btn.setChecked(grouping == "tipo")
        view.group_by_broker_btn.setChecked(grouping == "broker")
        self.load_portfolio(view)

    def set_asset_currency_view(self, currency, view):
        if not getattr(view, "is_user", False):
            return
        if currency not in ("ARS", "USD"):
            return
        view.asset_currency_view = currency
        self.load_portfolio(view)

    def set_liquidity_currency(self, currency, view):
        if not getattr(view, "is_user", False):
            return
        view.liquidity_currency = currency
        if getattr(view, "liquidity_overlay", None) is not None:
            self.open_liquidity_overlay(view, currency)

    def draw_liquidity_chart(self, view):
        self.draw_liquidity_chart_for_currency(
            view,
            view.liquidity_currency,
            view.liquidity_chart_layout,
            view.liquidity_list,
            view.liquidity_total_label,
        )

    def draw_liquidity_chart_for_currency(self, view, currency, chart_layout, list_widget, total_label=None):
        for i in reversed(range(chart_layout.count())):
            widget = chart_layout.itemAt(i).widget()
            if widget:
                widget.setParent(None)
        list_widget.clear()

        liquidity_by_broker = getattr(view, "liquidity_by_broker", {})
        data = liquidity_by_broker.get(currency, {})
        labels = []
        values = []
        for broker, amount in data.items():
            if abs(amount) > 0.0001:
                labels.append(broker)
                values.append(abs(amount))

        if not values:
            no_data_label = QLabel("No hay liquidez para mostrar")
            chart_layout.addWidget(no_data_label)
            if total_label is not None:
                total_label.setText("")
            return

        fig = Figure(figsize=(5, 3.2), dpi=100)
        ax = fig.add_subplot(111)
        colors = [self.get_broker_color(broker) for broker in labels]
        chart_bg = self.get_theme_value("chart_bg", "#1b1f24")
        chart_text = self.get_theme_value("chart_text", "#e6e6e6")
        fig.patch.set_facecolor(chart_bg)
        ax.set_facecolor(chart_bg)

        def pct_label(pct):
            return f"{pct:.1f}%" if pct >= 6 else ""

        wedges, _texts, _autotexts = ax.pie(
            values,
            labels=None,
            autopct=pct_label,
            startangle=90,
            colors=colors[:len(labels)],
            textprops={'fontsize': 9, 'color': chart_text}
        )
        ax.set_title(
            f'Distribución de liquidez en {currency}',
            fontsize=12,
            pad=14,
            color=chart_text,
        )
        fig.subplots_adjust(left=0.05, right=0.95, top=0.85, bottom=0.05)
        ax.axis('equal')

        canvas = FigureCanvasQTAgg(fig)
        canvas.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        chart_layout.addWidget(canvas)

        total = sum(values) if values else 1
        currency_prefix = "USD" if currency == "USD" else "ARS"
        for broker, amount in sorted(data.items(), key=lambda x: x[0]):
            if abs(amount) <= 0.0001:
                continue
            pct = (abs(amount) / total) * 100 if total else 0
            value_str = f"{currency_prefix} ${abs(amount):,.2f}"
            item = QListWidgetItem(f"{broker}: {value_str} ({pct:.2f}%)")
            pixmap = QPixmap(10, 10)
            pixmap.fill(QColor(self.get_broker_color(broker)))
            item.setIcon(QIcon(pixmap))
            list_widget.addItem(item)

        if total_label is not None:
            total_assets_ars = getattr(view, "total_assets_ars", 0.0)
            total_liq_ars = sum(liquidity_by_broker.get("ARS", {}).values())
            total_liq_usd = sum(liquidity_by_broker.get("USD", {}).values())
            fx_rate = self.detect_fx_rate()
            total_portfolio_ars = total_assets_ars + total_liq_ars + (total_liq_usd * fx_rate if fx_rate else 0.0)
            if currency == "USD":
                total_equiv_ars = total * (fx_rate if fx_rate else 0.0)
            else:
                total_equiv_ars = total
            if total_portfolio_ars > 0:
                pct_total = (total_equiv_ars / total_portfolio_ars) * 100
            else:
                pct_total = 0.0
            total_label.setText(
                f"Total {currency_prefix}: ${total:,.2f} ({pct_total:.2f}%)"
            )

        tooltip = ax.annotate(
            "",
            xy=(0, 0),
            xytext=(12, 12),
            textcoords="offset points",
            bbox=dict(boxstyle="round", fc="white", ec="gray", alpha=0.9),
            arrowprops=dict(arrowstyle="->")
        )
        tooltip.set_visible(False)

        def on_move(event, wedges=wedges, brokers=labels, values=values, tooltip=tooltip):
            if event.inaxes != ax:
                if tooltip.get_visible():
                    tooltip.set_visible(False)
                    canvas.draw_idle()
                return
            for idx, wedge in enumerate(wedges):
                contains, _ = wedge.contains(event)
                if contains:
                    pct = (values[idx] / total) * 100 if total else 0
                    value_str = f"{currency_prefix} ${values[idx]:,.2f}"
                    tooltip.xy = (event.xdata, event.ydata)
                    tooltip.set_text(f"{brokers[idx]}: {value_str} ({pct:.1f}%)")
                    tooltip.set_visible(True)
                    canvas.draw_idle()
                    return
            if tooltip.get_visible():
                tooltip.set_visible(False)
                canvas.draw_idle()

        canvas.mpl_connect("motion_notify_event", on_move)

    def open_liquidity_overlay(self, view, currency=None):
        if getattr(view, "liquidity_overlay", None) is None:
            return
        view.liquidity_overlay.setGeometry(view.overlay_parent.rect())
        view.liquidity_overlay.raise_()
        view.liquidity_overlay.setVisible(True)
        self.draw_liquidity_chart_for_currency(
            view,
            "ARS",
            view.liquidity_overlay_charts["ARS"],
            view.liquidity_overlay_lists["ARS"],
            view.liquidity_overlay_totals["ARS"],
        )
        self.draw_liquidity_chart_for_currency(
            view,
            "USD",
            view.liquidity_overlay_charts["USD"],
            view.liquidity_overlay_lists["USD"],
            view.liquidity_overlay_totals["USD"],
        )
        if currency in ("ARS", "USD"):
            index = 0 if currency == "ARS" else 1
            view.liquidity_overlay_tabs.setCurrentIndex(index)

    def close_liquidity_overlay(self, view):
        if getattr(view, "liquidity_overlay", None) is None:
            return
        view.liquidity_overlay.setVisible(False)

    def update_liquidity_section(self, view, total_assets_ars, fx_rate):
        liquidity_by_broker = getattr(view, "liquidity_by_broker", {})
        total_liq_ars = sum(liquidity_by_broker.get("ARS", {}).values())
        total_liq_usd = sum(liquidity_by_broker.get("USD", {}).values())
        total_liq_ars_equiv = total_liq_ars + (total_liq_usd * fx_rate if fx_rate else 0)
        total_portfolio_ars = total_assets_ars + total_liq_ars_equiv
        if total_portfolio_ars > 0:
            liquidity_pct = (total_liq_ars_equiv / total_portfolio_ars) * 100
        else:
            liquidity_pct = 0

        view.liquidity_button.setText(
            "LIQUIDEZ TOTAL"
        )
        ars_link = "<a href='ARS'>ARS</a>"
        usd_link = "<a href='USD'>USD</a>"
        view.liquidity_currency_label.setText(
            f"{ars_link}: ${total_liq_ars:,.2f} &nbsp;|&nbsp; {usd_link}: ${total_liq_usd:,.2f}"
        )
        view.liquidity_total_label.setText(
            f"El total de su capital líquido es ${total_liq_ars_equiv:,.2f} ({liquidity_pct:.2f}%)"
        )
        view.total_assets_ars = total_assets_ars
        view.total_liq_ars_equiv = total_liq_ars_equiv
        view.total_portfolio_ars = total_portfolio_ars
        if getattr(view, "liquidity_overlay", None) is not None and view.liquidity_overlay.isVisible():
            self.open_liquidity_overlay(view)

    def update_countdown(self):
        """Actualizar la cuenta regresiva visual"""
        if self.remaining_time <= 0:
            return

        self.remaining_time -= 1
        minutes = self.remaining_time // 60
        seconds = self.remaining_time % 60
        text = f"Próxima actualización en: {minutes:02d}:{seconds:02d}"
        for view in self.get_portfolio_views():
            view.countdown_label.setText(text)

    def start_auto_update(self):
        """Iniciar la actualización automática con cuenta regresiva"""
        self.auto_update_timer.stop()
        self.countdown_timer.stop()

        # Convertir minutos a milisegundos
        interval_ms = self.auto_update_interval * 60 * 1000
        self.auto_update_timer.start(interval_ms)

        # Iniciar cuenta regresiva (actualizar cada segundo)
        self.remaining_time = self.auto_update_interval * 60  # Inicializar aquí
        self.update_countdown()  # Actualizar inmediatamente
        self.countdown_timer.start(1000)

    def on_tab_changed(self, index):
        current_widget = self.tabs.widget(index)
        if current_widget == self.operations_tab:
            inner_tab_name = self.operations_inner_tabs.tabText(self.operations_inner_tabs.currentIndex())
            if inner_tab_name == "Operaciones Finalizadas":
                self.load_finished_operations()
        elif current_widget in self.portfolio_views_by_tab:
            self.load_portfolio(self.portfolio_views_by_tab[current_widget])
        elif current_widget == self.analysis_tab:
            self.analysis_tab.load_portfolio()
            self.analysis_tab.load_saved_data()
            self.analysis_tab.sort_tables()  # Actualizar ordenamiento

    def on_inner_tab_changed(self, index):
        tab_name = self.operations_inner_tabs.tabText(index)
        if tab_name == "Operaciones Finalizadas":
            self.load_finished_operations()
        elif tab_name == "Libro Diario":
            self.load_journal()

    def create_finished_ops_view(self, parent_widget):
        layout = QVBoxLayout(parent_widget)

        # Frame de controles
        control_frame = QFrame()
        control_layout = QHBoxLayout(control_frame)

        # Botón Actualizar
        self.update_finished_btn = QPushButton("Actualizar")
        self.update_finished_btn.clicked.connect(self.load_finished_operations)
        control_layout.addWidget(self.update_finished_btn)

        # Grupo de filtros
        filter_group = QGroupBox("Filtro de Fechas")
        filter_layout = QHBoxLayout(filter_group)

        self.show_all_radio = QRadioButton("Mostrar Todo")
        self.limit_radio = QRadioButton("Limitar")
        self.show_all_radio.setChecked(True)

        filter_layout.addWidget(QLabel("Ver:"))
        filter_layout.addWidget(self.show_all_radio)
        filter_layout.addWidget(self.limit_radio)

        # Campos de fecha
        filter_layout.addWidget(QLabel("Desde:"))
        self.from_date_edit = QLineEdit()
        self.from_date_edit.setFixedWidth(100)
        self.from_date_edit.setEnabled(False)
        today = datetime.today()
        last_month = today - timedelta(days=30)
        self.from_date_edit.setText(last_month.strftime("%Y-%m-%d"))
        filter_layout.addWidget(self.from_date_edit)

        filter_layout.addWidget(QLabel("Hasta:"))
        self.to_date_edit = QLineEdit()
        self.to_date_edit.setFixedWidth(100)
        self.to_date_edit.setEnabled(False)
        self.to_date_edit.setText(today.strftime("%Y-%m-%d"))
        filter_layout.addWidget(self.to_date_edit)

        # Botón aplicar filtro
        self.apply_filter_btn = QPushButton("Aplicar Filtro")
        self.apply_filter_btn.clicked.connect(self.load_finished_operations)
        filter_layout.addWidget(self.apply_filter_btn)

        control_layout.addWidget(filter_group)
        layout.addWidget(control_frame)

        # Etiqueta de información de filtro
        self.filter_info_label = QLabel()
        layout.addWidget(self.filter_info_label)

        # Conectar cambios en los radio buttons
        self.show_all_radio.toggled.connect(self.toggle_date_filter)
        self.limit_radio.toggled.connect(self.toggle_date_filter)

        # Tabla de operaciones finalizadas
        self.finished_table = QTableWidget()
        self.finished_table.setColumnCount(10)
        self.finished_table.setHorizontalHeaderLabels([
            "Fecha", "Categoría", "Símbolo", "Cantidad Nominal",
            "Precio de Compra", "Precio de Venta",
            "Diferencia de Valor", "Descuentos", "Dividendos", "Resultado"
        ])
        self.finished_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.finished_table.verticalHeader().setVisible(False)
        self.finished_table.setSizeAdjustPolicy(QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents)

        layout.addWidget(self.finished_table)

    def toggle_date_filter(self):
        enabled = self.limit_radio.isChecked()
        self.from_date_edit.setEnabled(enabled)
        self.to_date_edit.setEnabled(enabled)

    def load_finished_operations(self):
        # Actualizar mensaje de filtro
        if self.show_all_radio.isChecked():
            self.filter_info_label.setText("")
        else:
            self.filter_info_label.setText(
                f"Se muestran los resultados desde {self.from_date_edit.text()} hasta {self.to_date_edit.text()}")

        # Limpiar tabla
        self.finished_table.setRowCount(0)

        # Obtener fechas si se usa filtro
        from_date = None
        to_date = None
        if not self.show_all_radio.isChecked():
            try:
                from_date = datetime.strptime(self.from_date_edit.text(), "%Y-%m-%d")
                to_date = datetime.strptime(self.to_date_edit.text(), "%Y-%m-%d")
            except:
                QMessageBox.critical(self, "Error", "Formato de fecha inválido. Use YYYY-MM-DD")
                return

        # Leer el libro diario completo desde la base
        try:
            journal_rows = fetch_journal()
        except Exception as e:
            print(f"Error leyendo journal: {e}")
            journal_rows = []

        finished_ops = compute_finished_operations(journal_rows, from_date, to_date)
        # Llenar tabla
        self.finished_table.setRowCount(len(finished_ops))
        total_diferencia_valor = 0
        total_descuentos = 0
        total_rendimiento = 0
        total_resultado = 0
        total_resultado_usd_tabla = 0

        for row_idx, op in enumerate(finished_ops):
            fecha = op['fecha']
            tipo = op['tipo']
            simbolo = op['simbolo']
            cantidad = op['cantidad']
            precio_compra = op['precio_compra']
            precio_venta = op['precio_venta']
            diferencia_valor = op['diferencia_valor']
            descuentos = op['descuentos']
            rendimiento = op['rendimiento']
            resultado = op['resultado']

            total_diferencia_valor += diferencia_valor
            total_descuentos += descuentos
            total_rendimiento += rendimiento
            total_resultado += resultado

            self.finished_table.setItem(row_idx, 0, QTableWidgetItem(fecha))
            self.finished_table.setItem(row_idx, 1, QTableWidgetItem(tipo))
            self.finished_table.setItem(row_idx, 2, QTableWidgetItem(simbolo))
            self.finished_table.setItem(row_idx, 3, QTableWidgetItem(f"{cantidad:,.2f}"))
            self.finished_table.setItem(row_idx, 4, QTableWidgetItem(f"${precio_compra:,.2f}"))
            self.finished_table.setItem(row_idx, 5, QTableWidgetItem(f"${precio_venta:,.2f}"))
            self.finished_table.setItem(row_idx, 6, QTableWidgetItem(f"${diferencia_valor:,.2f}"))
            self.finished_table.setItem(row_idx, 7, QTableWidgetItem(f"${descuentos:,.2f}"))
            self.finished_table.setItem(row_idx, 8, QTableWidgetItem(f"${rendimiento:,.2f}"))
            self.finished_table.setItem(row_idx, 9, QTableWidgetItem(f"${resultado:,.2f}"))

            # Color de texto
            if diferencia_valor > 0:
                self.finished_table.item(row_idx, 6).setForeground(QColor('green'))
            elif diferencia_valor < 0:
                self.finished_table.item(row_idx, 6).setForeground(QColor('red'))

            # Descuentos siempre rojos
            self.finished_table.item(row_idx, 7).setForeground(QColor('red'))

            if rendimiento > 0:
                self.finished_table.item(row_idx, 8).setForeground(QColor('green'))
            elif rendimiento < 0:
                self.finished_table.item(row_idx, 8).setForeground(QColor('red'))

            if resultado > 0:
                self.finished_table.item(row_idx, 9).setForeground(QColor('green'))
            elif resultado < 0:
                self.finished_table.item(row_idx, 9).setForeground(QColor('red'))

        # Agregar fila de totales
        if finished_ops:
            row_idx = self.finished_table.rowCount()
            self.finished_table.insertRow(row_idx)

            # Crear items para la fila de totales
            items = [
                QTableWidgetItem("TOTAL"),
                QTableWidgetItem(""),
                QTableWidgetItem(""),
                QTableWidgetItem(""),
                QTableWidgetItem(""),
                QTableWidgetItem(""),
                QTableWidgetItem(f"${total_diferencia_valor:,.2f}"),
                QTableWidgetItem(f"${total_descuentos:,.2f}"),
                QTableWidgetItem(f"${total_rendimiento:,.2f}"),
                QTableWidgetItem(f"${total_resultado:,.2f}")
            ]

            # Aplicar formato a los items
            for i, item in enumerate(items):
                if i >= 6:  # Columnas numéricas
                    font = QFont()
                    font.setBold(True)
                    item.setFont(font)

                    if i == 6:  # Diferencia de valor
                        if total_diferencia_valor > 0:
                            item.setForeground(QColor('green'))
                        elif total_diferencia_valor < 0:
                            item.setForeground(QColor('red'))
                    elif i == 7:  # Descuentos
                        item.setForeground(QColor('red'))
                    elif i == 8:  # Dividendos
                        if total_rendimiento > 0:
                            item.setForeground(QColor('green'))
                        elif total_rendimiento < 0:
                            item.setForeground(QColor('red'))
                    elif i == 9:  # Resultado
                        if total_resultado > 0:
                            item.setForeground(QColor('green'))
                        elif total_resultado < 0:
                            item.setForeground(QColor('red'))

                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                self.finished_table.setItem(row_idx, i, item)

    def recalcular_portfolio(self):
        """Recalcular completamente el portafolio desde el libro diario"""
        try:
            journal_rows = fetch_journal()
        except Exception as e:
            print(f"Error leyendo journal: {e}")
            return

        rows_to_save = recompute_portfolio_rows(journal_rows)
        replace_portfolio(rows_to_save)

    def cargar_datos_mercado(self):
        try:
            self.df_mercado, self.last_update = load_market_data()
            self.start_fx_update_thread(run_backfill=False)
            self.update_default_fx_rate()
            if self.df_mercado is not None and self.last_update:
                self.update_status_labels()
            if self.df_mercado is None:
                self.last_update = None
                self.update_status_labels()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error cargando datos de mercado: {str(e)}")
            self.df_mercado = None

    def actualizar_datos_mercado(self):
        if hasattr(self, 'update_thread') and self.update_thread.isRunning():
            return

        for view in self.get_portfolio_views():
            view.actualizar_btn.setEnabled(False)
            view.status_label.setText("Descargando datos de mercado...")

        # Usar QThread correctamente
        self.update_thread = DownloadThread(DATA_DIR)
        self.update_thread.finished.connect(self.on_download_finished)
        self.update_thread.start()

    def on_download_finished(self, success, message):
        for view in self.get_portfolio_views():
            view.actualizar_btn.setEnabled(True)
        self.update_status_labels(message)

        if success:
            self.cargar_datos_mercado()
            self.refresh_portfolios()
            self.load_finished_operations()

        # Reiniciar actualización automática
        if self.auto_update_active:
            self.start_auto_update()

    def get_next_plazo_fijo_number(self):
        try:
            journal_rows = fetch_journal()
            return next_plazo_fijo_number(journal_rows)
        except Exception as e:
            print(f"Error calculando próximo plazo fijo: {e}")
            return 1

    def create_operation_form(self, parent_widget):
        layout = QGridLayout(parent_widget)
        layout.setColumnStretch(1, 1)
        layout.setColumnStretch(3, 1)

        # Variables para almacenar valores
        self.fecha_edit = QDateEdit(QDate.currentDate())
        self.fecha_edit.setCalendarPopup(True)
        self.fecha_edit.setDisplayFormat("yyyy-MM-dd")
        self.tipo_combo = QComboBox()
        self.tipo_combo.addItems(["Depósito ARS", "Depósito USD", "Plazo Fijo", "Acciones AR", "CEDEARs", "Bonos AR", "ETFs", "Criptomonedas", "FCIs AR", "Cauciones"])
        self.tipo_op_combo = QComboBox()
        self.plazo_combo = QComboBox()
        self.plazo_combo.addItems(["T+0", "T+1", "T+2"])
        self.plazo_combo.setCurrentText("T+1")
        self.broker_combo = QComboBox()
        self.broker_combo.addItems(BROKERS)
        self.simbolo_combo = QComboBox()
        self.simbolo_combo.setEditable(True)  # Permitir edición para nuevos símbolos
        self.detalle_edit = QLineEdit()
        self.cantidad_edit = QLineEdit("0")
        self.precio_edit = QLineEdit("0")
        self.rendimiento_edit = QLineEdit("0")
        self.tc_edit = QLineEdit("1.0")
        self.ccl_edit = QLineEdit()
        self.ccl_edit.setReadOnly(True)

        # Campos calculados
        self.total_sin_desc_label = QLineEdit()
        self.total_sin_desc_label.setReadOnly(True)
        self.comision_edit = QLineEdit()
        self.iva_label = QLineEdit()
        self.iva_label.setReadOnly(True)
        self.derechos_edit = QLineEdit()
        self.iva_derechos_label = QLineEdit()
        self.iva_derechos_label.setReadOnly(True)
        self.total_descuentos_label = QLineEdit()
        self.total_descuentos_label.setReadOnly(True)
        self.costo_total_label = QLineEdit()
        self.costo_total_label.setReadOnly(True)
        self.ingreso_total_label = QLineEdit()
        self.ingreso_total_label.setReadOnly(True)
        self.balance_label = QLineEdit()
        self.balance_label.setReadOnly(True)

        # Botones
        self.calcular_btn = QPushButton("Calcular")
        self.guardar_btn = QPushButton("Guardar")
        self.limpiar_btn = QPushButton("Limpiar")

        # Configurar layout
        layout.addWidget(QLabel("Fecha:"), 0, 0)
        layout.addWidget(self.fecha_edit, 0, 1)
        layout.addWidget(QLabel("Tipo*:"), 1, 0)
        layout.addWidget(self.tipo_combo, 1, 1)
        layout.addWidget(QLabel("Tipo Operación*:"), 2, 0)
        layout.addWidget(self.tipo_op_combo, 2, 1)
        layout.addWidget(QLabel("Plazo:"), 3, 0)
        layout.addWidget(self.plazo_combo, 3, 1)
        layout.addWidget(QLabel("Broker*:"), 4, 0)
        layout.addWidget(self.broker_combo, 4, 1)
        layout.addWidget(QLabel("Símbolo:"), 5, 0)
        layout.addWidget(self.simbolo_combo, 5, 1)
        layout.addWidget(QLabel("Detalle:"), 6, 0)
        layout.addWidget(self.detalle_edit, 6, 1)
        layout.addWidget(QLabel("Cantidad*:"), 7, 0)
        layout.addWidget(self.cantidad_edit, 7, 1)
        layout.addWidget(QLabel("Precio*:"), 8, 0)
        layout.addWidget(self.precio_edit, 8, 1)
        layout.addWidget(QLabel("Dividendos:"), 9, 0)
        layout.addWidget(self.rendimiento_edit, 9, 1)
        layout.addWidget(QLabel("Precio CCL:"), 10, 0)
        layout.addWidget(self.ccl_edit, 10, 1)
        layout.addWidget(QLabel("TC USD/ARS:"), 11, 0)
        layout.addWidget(self.tc_edit, 11, 1)

        # Campos calculados
        layout.addWidget(QLabel("Total sin descuentos:"), 0, 2)
        layout.addWidget(self.total_sin_desc_label, 0, 3)
        layout.addWidget(QLabel("Comisión:"), 1, 2)
        layout.addWidget(self.comision_edit, 1, 3)
        layout.addWidget(QLabel("IVA 21%:"), 2, 2)
        layout.addWidget(self.iva_label, 2, 3)
        layout.addWidget(QLabel("Derechos:"), 3, 2)
        layout.addWidget(self.derechos_edit, 3, 3)
        layout.addWidget(QLabel("IVA Derechos:"), 4, 2)
        layout.addWidget(self.iva_derechos_label, 4, 3)
        layout.addWidget(QLabel("Total descuentos:"), 5, 2)
        layout.addWidget(self.total_descuentos_label, 5, 3)
        layout.addWidget(QLabel("Costo total:"), 6, 2)
        layout.addWidget(self.costo_total_label, 6, 3)
        layout.addWidget(QLabel("Ingreso total:"), 7, 2)
        layout.addWidget(self.ingreso_total_label, 7, 3)
        layout.addWidget(QLabel("Balance:"), 8, 2)
        layout.addWidget(self.balance_label, 8, 3)

        # Botones
        button_layout = QHBoxLayout()
        button_layout.addWidget(self.calcular_btn)
        button_layout.addWidget(self.guardar_btn)
        button_layout.addWidget(self.limpiar_btn)
        layout.addLayout(button_layout, 12, 0, 1, 4)

        layout.addWidget(QLabel("* Campos obligatorios"), 13, 0, 1, 4)

        # Conectar señales
        self.tipo_combo.currentTextChanged.connect(self.on_tipo_change)
        self.tipo_op_combo.currentTextChanged.connect(self.on_tipo_op_change)
        self.broker_combo.currentTextChanged.connect(self.on_broker_change)
        self.fecha_edit.dateChanged.connect(self.on_fecha_change)
        self.cantidad_edit.textChanged.connect(self.calcular_on_change)
        self.precio_edit.textChanged.connect(self.calcular_on_change)
        self.rendimiento_edit.textChanged.connect(self.calcular_on_change)
        self.comision_edit.textChanged.connect(self.calcular_descuentos)
        self.derechos_edit.textChanged.connect(self.calcular_descuentos)
        self.calcular_btn.clicked.connect(self.calcular)
        self.guardar_btn.clicked.connect(self.guardar_operacion)
        self.limpiar_btn.clicked.connect(self.limpiar_formulario)

        # Inicializar estado
        self.on_tipo_change(self.tipo_combo.currentText())
        self.update_ccl_display()

    def update_ccl_display(self):
        fecha_dt = datetime.combine(self.fecha_edit.date().toPyDate(), datetime.min.time())
        rate = self.get_ccl_rate_for_date(fecha_dt)
        if rate is None:
            self.ccl_edit.setText("-")
        else:
            self.ccl_edit.setText(str(rate))

    def on_tipo_change(self, tipo):
        # Limpiar y configurar combo de tipo de operación
        self.tipo_op_combo.clear()

        if tipo == "Depósito ARS":
            self.tipo_op_combo.addItems(["Entrada", "Salida"])
            self.simbolo_combo.setCurrentText("$")
            self.precio_edit.setText("1")
            self.precio_edit.setEnabled(False)
            self.rendimiento_edit.setText("0")
            self.rendimiento_edit.setEnabled(False)
            self.tc_edit.setText("1.0")
            self.tc_edit.setEnabled(False)
        elif tipo == "Depósito USD":
            self.tipo_op_combo.addItems(["Entrada", "Salida"])
            self.simbolo_combo.setCurrentText("USD")
            self.precio_edit.setText("1")
            self.precio_edit.setEnabled(False)
            self.rendimiento_edit.setText("0")
            self.rendimiento_edit.setEnabled(False)
            self.tc_edit.setEnabled(True)
            fecha_dt = datetime.combine(self.fecha_edit.date().toPyDate(), datetime.min.time())
            fx_rate = self.get_fx_rate_for_date(fecha_dt, "Acciones AR") or getattr(self, "default_fx_rate", 1.0)
            self.tc_edit.setText(str(fx_rate))
        elif tipo == "Plazo Fijo":
            self.tipo_op_combo.addItems(["Compra", "Venta"])
            self.precio_edit.setText("1")
            self.precio_edit.setEnabled(False)
            self.rendimiento_edit.setEnabled(True)
            self.tc_edit.setText("1.0")
            self.tc_edit.setEnabled(False)
        elif tipo in ["Acciones AR", "CEDEARs", "Bonos AR", "ETFs", "Criptomonedas", "FCIs AR", "Cauciones"]:
            self.tipo_op_combo.addItems(["Compra", "Venta", "Dividendos"])
            self.precio_edit.setEnabled(True)
            self.rendimiento_edit.setEnabled(True)
            requires_fx = True
            moneda = self.get_moneda_actual(tipo)
            if moneda == "USD" or requires_fx:
                self.tc_edit.setEnabled(True)
                fecha_dt = datetime.combine(self.fecha_edit.date().toPyDate(), datetime.min.time())
                fx_rate = self.get_fx_rate_for_date(fecha_dt, tipo) or getattr(self, "default_fx_rate", 1.0)
                self.tc_edit.setText(str(fx_rate))
            else:
                self.tc_edit.setEnabled(False)
                self.tc_edit.setText("1.0")
            if tipo == "Criptomonedas":
                self.plazo_combo.setCurrentText("T+0")
                self.plazo_combo.setEnabled(False)
            else:
                self.plazo_combo.setEnabled(True)
                if not self.plazo_combo.currentText():
                    self.plazo_combo.setCurrentText("T+1")
        else:
            self.tipo_op_combo.addItems(["Compra", "Venta"])
            self.precio_edit.setEnabled(True)
            self.rendimiento_edit.setEnabled(True)
            self.tc_edit.setEnabled(False)
            self.tc_edit.setText("1.0")
            self.plazo_combo.setEnabled(True)

        self.update_simbolo_combobox()
        self.calcular_on_change()

    def on_broker_change(self, _):
        """Actualizar símbolos disponibles y re-cálculos cuando cambia el broker."""
        self.update_simbolo_combobox()
        self.calcular_on_change()

    def on_fecha_change(self, _):
        tipo = self.tipo_combo.currentText()
        if self.tc_edit.isEnabled():
            fecha_dt = datetime.combine(self.fecha_edit.date().toPyDate(), datetime.min.time())
            fx_rate = self.get_fx_rate_for_date(fecha_dt, tipo) or getattr(self, "default_fx_rate", 1.0)
            self.tc_edit.setText(str(fx_rate))
        self.update_ccl_display()
        self.calcular_on_change()

    def update_simbolo_combobox(self):
        """Rellena el combo de símbolos según broker y tipo actual."""
        current = self.simbolo_combo.currentText()
        tipo = self.tipo_combo.currentText()
        broker = self.broker_combo.currentText()
        symbols = []
        try:
            query = "SELECT DISTINCT simbolo FROM portfolio WHERE broker = ?"
            params = [broker]
            if tipo == "Plazo Fijo":
                query += " AND tipo = ?"
                params.append("Plazo Fijo")
            with get_conn() as conn:
                cur = conn.execute(query, params)
                symbols = [row["simbolo"] for row in cur.fetchall() if row["simbolo"]]
        except Exception as e:
            print(f"Error actualizando símbolos: {e}")

        # Mantener el texto actual si no está en la lista
        if current and current not in symbols:
            symbols.insert(0, current)

        self.simbolo_combo.blockSignals(True)
        self.simbolo_combo.clear()
        self.simbolo_combo.addItems(sorted(set(symbols)))
        if current:
            self.simbolo_combo.setCurrentText(current)
        self.simbolo_combo.blockSignals(False)

    def get_moneda_actual(self, tipo):
        """Determina moneda base según tipo de operación."""
        if "USD" in tipo:
            return "USD"
        if tipo in ["Criptomonedas"]:
            return "USD"
        return "ARS"

    def calcular_on_change(self):
        """Recalcula importes cuando cambian campos numéricos."""
        try:
            self.calcular()
        except Exception:
            pass

    def on_tipo_op_change(self, tipo_op):
        tipo = self.tipo_combo.currentText()

        # Habilitar/deshabilitar campos según operación
        if tipo == "Plazo Fijo" and tipo_op == "Compra":
            simbolo = f"Plazo Fijo {self.plazo_fijo_counter}"
            self.simbolo_combo.setCurrentText(simbolo)
            self.plazo_fijo_counter += 1
            self.rendimiento_edit.setText("0")
            self.rendimiento_edit.setEnabled(False)
            self.tc_edit.setEnabled(False)
            self.tc_edit.setText("1.0")
        elif tipo == "Plazo Fijo" and tipo_op == "Venta":
            self.update_simbolo_combobox()
            self.rendimiento_edit.setEnabled(True)
            self.tc_edit.setEnabled(False)
            self.tc_edit.setText("1.0")
            # Cargar cantidad del plazo fijo
            if self.simbolo_combo.currentText():
                try:
                    with get_conn() as conn:
                        cur = conn.execute(
                            "SELECT cantidad FROM portfolio WHERE simbolo = ? LIMIT 1",
                            (self.simbolo_combo.currentText(),)
                        )
                        row = cur.fetchone()
                        if row:
                            self.cantidad_edit.setText(str(row["cantidad"]))
                except Exception as e:
                    print(f"Error cargando cantidad de plazo fijo: {e}")

    def calcular_descuentos(self):
        try:
            comision = float(self.comision_edit.text().replace(".", "").replace(",", ".")) if self.comision_edit.text() else 0
            derechos = float(self.derechos_edit.text().replace(".", "").replace(",", ".")) if self.derechos_edit.text() else 0
            descuentos = calcular_descuentos_y_totales(comision, derechos)

            def format_number(num):
                return f"{num:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

            self.iva_label.setText(format_number(descuentos["iva_basico"]))
            self.iva_derechos_label.setText(format_number(descuentos["iva_derechos"]))
            self.total_descuentos_label.setText(format_number(descuentos["total_descuentos"]))

            self.calcular_costos_ingresos()
        except Exception:
            pass

    def calcular_costos_ingresos(self):
        try:
            tipo = self.tipo_combo.currentText()
            tipo_op = self.tipo_op_combo.currentText()

            total_sin_desc = float(self.total_sin_desc_label.text().replace('.', '').replace(',', '.')) if self.total_sin_desc_label.text() else 0
            total_descuentos = float(self.total_descuentos_label.text().replace('.', '').replace(',', '.')) if self.total_descuentos_label.text() else 0

            if tipo in ["Deposito ARS", "Deposito USD", "Depósito ARS", "Depósito USD"]:
                if tipo_op == "Entrada":
                    costo_total = 0
                    ingreso_total = total_sin_desc
                    balance = total_sin_desc
                else:
                    costo_total = total_sin_desc
                    ingreso_total = 0
                    balance = -total_sin_desc
            else:
                if tipo_op in ["Compra", "Salida"]:
                    costo_total = total_sin_desc + total_descuentos
                    ingreso_total = 0
                    balance = -costo_total
                else:
                    costo_total = 0
                    ingreso_total = total_sin_desc - total_descuentos
                    balance = ingreso_total

            def format_number(num):
                return f"{num:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

            self.costo_total_label.setText(format_number(costo_total))
            self.ingreso_total_label.setText(format_number(ingreso_total))
            self.balance_label.setText(format_number(balance))
        except Exception:
            pass

    def calcular(self):
        try:
            tipo = self.tipo_combo.currentText()
            tipo_op = self.tipo_op_combo.currentText()
            broker = self.broker_combo.currentText() or "GENERAL"
            cantidad = float(self.cantidad_edit.text().replace(',', '.')) if self.cantidad_edit.text() else 0
            precio = float(self.precio_edit.text().replace(',', '.')) if self.precio_edit.text() else 0
            rendimiento = float(self.rendimiento_edit.text().replace(',', '.')) if self.rendimiento_edit.text() else 0
            moneda = self.get_moneda_actual(tipo)
            tc_usd_ars = float(self.tc_edit.text().replace(',', '.')) if self.tc_edit.text() else 1.0
            fecha_dt = datetime.combine(self.fecha_edit.date().toPyDate(), datetime.min.time())
            simbolo = self.simbolo_combo.currentText()
            plazo = self.plazo_combo.currentText() or "T+1"
            bmb_tier = self.compute_bmb_tier(broker, fecha_dt)
            intraday_bonus = self.is_intraday_bonus(
                broker, tipo, tipo_op, simbolo, cantidad, moneda, plazo, fecha_dt
            )

            op = calcular_operacion(
                tipo,
                tipo_op,
                cantidad,
                precio,
                rendimiento,
                broker,
                bmb_tier=bmb_tier,
                intraday_bonus=intraday_bonus,
            )

            def format_number(num):
                return f"{num:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

            self.total_sin_desc_label.setText(format_number(op["total_sin_desc"]))
            self.comision_edit.setText(format_number(op["comision"]))
            self.iva_label.setText(format_number(op["iva_basico"]))
            self.derechos_edit.setText(format_number(op["derechos"]))
            self.iva_derechos_label.setText(format_number(op["iva_derechos"]))
            self.total_descuentos_label.setText(format_number(op["total_descuentos"]))
            self.costo_total_label.setText(format_number(op["costo_total"]))
            self.ingreso_total_label.setText(format_number(op["ingreso_total"]))
            self.balance_label.setText(format_number(op["balance"]))
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Datos inválidos: {str(e)}")

    def guardar_operacion(self):
        try:
            if not (self.tipo_combo.currentText() and self.tipo_op_combo.currentText() and 
                    self.cantidad_edit.text() and self.precio_edit.text()):
                QMessageBox.critical(self, "Error", "Complete todos los campos obligatorios (*)")
                return

            tipo = self.tipo_combo.currentText()
            if tipo not in ["Depósito ARS", "Depósito USD"] and not self.simbolo_combo.currentText():
                QMessageBox.critical(self, "Error", "El campo Símbolo es obligatorio para este tipo de instrumento")
                return

            cantidad = float(self.cantidad_edit.text().replace(',', '.')) if self.cantidad_edit.text() else 0
            precio = float(self.precio_edit.text().replace(',', '.')) if self.precio_edit.text() else 0
            rendimiento = float(self.rendimiento_edit.text().replace(',', '.')) if self.rendimiento_edit.text() else 0
            tc_usd_ars = float(self.tc_edit.text().replace(',', '.')) if self.tc_edit.text() else 1.0
            tipo_op = self.tipo_op_combo.currentText()
            moneda = self.get_moneda_actual(tipo)

            # Validaciones específicas
            if tipo == "Plazo Fijo" and tipo_op == "Venta":
                if rendimiento == 0:
                    QMessageBox.critical(self, "Error", "Para venta de Plazo Fijo debe ingresar el Dividendos")
                    return

            if cantidad < 0 or precio < 0:
                QMessageBox.critical(self, "Error", "Cantidad y Precio deben ser valores positivos")
                return

            fecha = self.fecha_edit.date().toString("yyyy-MM-dd")
            simbolo = self.simbolo_combo.currentText()
            detalle = self.detalle_edit.text()
            broker = self.broker_combo.currentText() or "GENERAL"
            fecha_dt = datetime.combine(self.fecha_edit.date().toPyDate(), datetime.min.time())

            plazo = self.plazo_combo.currentText() or "T+1"
            bmb_tier = self.compute_bmb_tier(broker, fecha_dt)
            intraday_bonus = self.is_intraday_bonus(
                broker, tipo, tipo_op, simbolo, cantidad, moneda, plazo, fecha_dt
            )
            op = calcular_operacion(
                tipo,
                tipo_op,
                cantidad,
                precio,
                rendimiento,
                broker,
                bmb_tier=bmb_tier,
                intraday_bonus=intraday_bonus,
            )

            # Validar cantidad para ventas (por broker)
            if tipo_op == "Venta":
                simbolo = self.simbolo_combo.currentText()
                holdings = self.get_holdings_by_broker()
                disponible = holdings.get(broker, {}).get(simbolo, 0.0)

                # Validar cantidad
                if cantidad > disponible + 1e-6:
                    QMessageBox.critical(
                        self, 
                        "Error", 
                        f"Cantidad insuficiente en {broker}. Disponible: {disponible}"
                    )
                    return

            # Obtener valores calculados
            total_sin_desc = op["total_sin_desc"]
            comision = op["comision"]
            iva_basico = op["iva_basico"]
            derechos = op["derechos"]
            iva_derechos = op["iva_derechos"]
            total_descuentos = op["total_descuentos"]
            costo_total = op["costo_total"]
            ingreso_total = op["ingreso_total"]
            balance = op["balance"]

            # Validar liquidez por broker (solo egresos netos)
            delta_cash = ingreso_total - costo_total
            if not (tipo in ["Depósito ARS", "Depósito USD"] and tipo_op == "Entrada"):  # Entradas siempre permitidas
                cash_by_broker = self.get_cash_by_broker(moneda)
                disponible_broker = cash_by_broker.get(broker, 0.0)
                if delta_cash < 0 and (disponible_broker + delta_cash) < -0.0001:
                    def fmt(num):
                        return f"{num:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                    QMessageBox.critical(
                        self,
                        "Fondos insuficientes",
                        f"Fondos insuficientes en {broker} ({moneda}). Disponible: {fmt(disponible_broker)}. "
                        f"Necesario: {fmt(-delta_cash)}"
                    )
                    return

            # Guardar en base de datos
            insert_journal_row({
                'fecha': fecha,
                'tipo': tipo,
                'tipo_operacion': tipo_op,
                'simbolo': simbolo,
                'detalle': detalle,
                'plazo': plazo,
                'cantidad': cantidad,
                'precio': precio,
                'rendimiento': rendimiento,
                'total_sin_desc': total_sin_desc,
                'comision': comision,
                'iva_21': iva_basico,
                'derechos': derechos,
                'iva_derechos': iva_derechos,
                'total_descuentos': total_descuentos,
                'costo_total': costo_total,
                'ingreso_total': ingreso_total,
                'balance': balance,
                'broker': broker,
                'moneda': moneda,
                'tc_usd_ars': tc_usd_ars
            })

            # Actualizar compras pendientes
            if tipo_op == "Compra":
                if simbolo not in self.compras_pendientes:
                    self.compras_pendientes[simbolo] = deque()
                self.compras_pendientes[simbolo].append((cantidad, precio))
            elif tipo_op == "Venta":
                cantidad_a_vender = cantidad
                while cantidad_a_vender > 0 and self.compras_pendientes.get(simbolo):
                    primera_compra = self.compras_pendientes[simbolo][0]
                    if primera_compra[0] <= cantidad_a_vender:
                        cantidad_a_vender -= primera_compra[0]
                        self.compras_pendientes[simbolo].popleft()
                    else:
                        self.compras_pendientes[simbolo][0] = (primera_compra[0] - cantidad_a_vender, primera_compra[1])
                        cantidad_a_vender = 0

            # Recalcular el portafolio
            self.recalcular_portfolio()

            QMessageBox.information(self, "Éxito", "Operación registrada correctamente")
            self.limpiar_formulario()
            self.load_journal()
            self.refresh_portfolios()
            self.load_finished_operations()

            # Actualizar pestaña de Análisis
            self.analysis_tab.load_portfolio()
            self.analysis_tab.load_saved_data()
            self.analysis_tab.sort_tables()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al guardar: {str(e)}")

    def limpiar_formulario(self):
        self.tipo_combo.setCurrentIndex(0)
        self.tipo_op_combo.clear()
        self.plazo_combo.setCurrentText("T+1")
        self.simbolo_combo.clear()
        self.broker_combo.setCurrentIndex(0)
        self.detalle_edit.clear()
        self.cantidad_edit.setText("0")
        self.precio_edit.setText("0")
        self.rendimiento_edit.setText("0")

        self.precio_edit.setEnabled(True)
        self.rendimiento_edit.setEnabled(True)
        self.cantidad_edit.setEnabled(True)

        self.total_sin_desc_label.clear()
        self.comision_edit.clear()
        self.iva_label.clear()
        self.derechos_edit.clear()
        self.iva_derechos_label.clear()
        self.total_descuentos_label.clear()
        self.costo_total_label.clear()
        self.ingreso_total_label.clear()
        self.balance_label.clear()

    def create_portfolio_view(self, parent_widget, is_user=False):
        view = SimpleNamespace()
        view.is_user = is_user
        layout = QVBoxLayout(parent_widget)
        view.layout = layout

        if is_user:
            view.asset_currency_view = "ARS"
            view.summary_frame = QFrame()
            view.summary_frame.setObjectName("portfolioSummary")
            summary_layout = QHBoxLayout(view.summary_frame)
            summary_layout.setContentsMargins(12, 10, 12, 10)
            summary_layout.setSpacing(16)

            view.summary_title_label = QLabel("GAN/PER PORTAFOLIO")
            view.summary_title_label.setStyleSheet("font-weight: 600;")
            summary_layout.addWidget(view.summary_title_label)

            summary_layout.addStretch()

            view.summary_ars_card = QFrame()
            view.summary_ars_card.setCursor(Qt.CursorShape.PointingHandCursor)
            view.summary_ars_card.mousePressEvent = (
                lambda _event, v=view: self.set_asset_currency_view("ARS", v)
            )
            ars_layout = QVBoxLayout(view.summary_ars_card)
            ars_layout.setContentsMargins(12, 6, 12, 6)
            view.summary_ars_label = QLabel("ARS: $0.00")
            ars_layout.addWidget(view.summary_ars_label)

            view.summary_usd_card = QFrame()
            view.summary_usd_card.setCursor(Qt.CursorShape.PointingHandCursor)
            view.summary_usd_card.mousePressEvent = (
                lambda _event, v=view: self.set_asset_currency_view("USD", v)
            )
            usd_layout = QVBoxLayout(view.summary_usd_card)
            usd_layout.setContentsMargins(12, 6, 12, 6)
            view.summary_usd_label = QLabel("USD: $0.00")
            usd_layout.addWidget(view.summary_usd_label)

            summary_layout.addWidget(view.summary_ars_card)
            summary_layout.addWidget(view.summary_usd_card)

            layout.addWidget(view.summary_frame)

            view.liquidity_frame = QFrame()
            liquidity_layout = QVBoxLayout(view.liquidity_frame)
            liquidity_layout.setContentsMargins(0, 0, 0, 0)

            liquidity_header = QHBoxLayout()
            liquidity_header.setSpacing(6)
            view.liquidity_button = QPushButton("LIQUIDEZ TOTAL")
            view.liquidity_button.setCursor(Qt.CursorShape.PointingHandCursor)
            view.liquidity_button.clicked.connect(
                lambda _checked=False, v=view: self.toggle_liquidity_chart(v)
            )
            view.liquidity_currency = "ARS"
            view.liquidity_currency_label = QLabel()
            view.liquidity_currency_label.setTextFormat(Qt.TextFormat.RichText)
            view.liquidity_currency_label.setOpenExternalLinks(False)
            view.liquidity_currency_label.linkActivated.connect(
                lambda link, v=view: self.set_liquidity_currency(link, v)
            )
            view.liquidity_currency_label.setStyleSheet(
                "QLabel { padding: 6px 10px; border-radius: 4px; background: #1b1f24; }"
                "QLabel a { color: #f28e2b; text-decoration: underline; }"
            )
            liquidity_header.addWidget(view.liquidity_button)
            liquidity_header.addWidget(view.liquidity_currency_label)
            liquidity_header.addStretch()
            liquidity_layout.addLayout(liquidity_header)

            view.liquidity_chart_frame = QFrame()
            chart_frame_layout = QVBoxLayout(view.liquidity_chart_frame)
            chart_list_layout = QHBoxLayout()
            chart_list_layout.setSpacing(16)
            chart_list_layout.setContentsMargins(0, 6, 0, 0)

            view.liquidity_chart_container = QFrame()
            view.liquidity_chart_layout = QVBoxLayout(view.liquidity_chart_container)
            view.liquidity_chart_container.setSizePolicy(
                QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding
            )
            chart_list_layout.addWidget(view.liquidity_chart_container, 3)

            view.liquidity_list_container = QFrame()
            view.liquidity_list_container.setStyleSheet(
                "QFrame { background: transparent; border: none; }"
            )
            list_layout = QVBoxLayout(view.liquidity_list_container)
            list_layout.setContentsMargins(0, 0, 0, 0)
            view.liquidity_list = QListWidget()
            view.liquidity_list.setFrameShape(QFrame.Shape.NoFrame)
            view.liquidity_list.setSpacing(6)
            view.liquidity_list.setStyleSheet(
                "QListWidget { background: transparent; }"
                "QListWidget::item { padding: 4px 2px; }"
            )
            list_layout.addWidget(view.liquidity_list)
            view.liquidity_list_container.setMinimumWidth(260)
            chart_list_layout.addWidget(view.liquidity_list_container, 1)

            chart_frame_layout.addLayout(chart_list_layout)
            view.liquidity_chart_frame.setVisible(False)
            view.liquidity_total_label = QLabel(
                "El total de su capital líquido es $0.00 (0.00%)"
            )
            chart_frame_layout.addWidget(view.liquidity_total_label)
            liquidity_layout.addWidget(view.liquidity_chart_frame)

            layout.addWidget(view.liquidity_frame)

            view.overlay_parent = parent_widget
            parent_widget.installEventFilter(self)

            view.liquidity_overlay = QFrame(parent_widget)
            view.liquidity_overlay.setObjectName("liquidityOverlay")
            view.liquidity_overlay.setStyleSheet(
                "QFrame#liquidityOverlay { background: rgba(0, 0, 0, 160); }"
            )
            view.liquidity_overlay.setVisible(False)

            overlay_layout = QVBoxLayout(view.liquidity_overlay)
            overlay_layout.setContentsMargins(40, 30, 40, 30)
            overlay_layout.addStretch()

            view.liquidity_overlay_card = QFrame()
            view.liquidity_overlay_card.setObjectName("liquidityOverlayCard")
            card_bg = self.get_theme_value("card_bg", "#1b1f24")
            card_border = self.get_theme_value("card_border", "#2f3742")
            view.liquidity_overlay_card.setStyleSheet(
                f"QFrame#liquidityOverlayCard {{ background: {card_bg}; border: 1px solid {card_border}; border-radius: 10px; }}"
            )
            view.liquidity_overlay_card.setMinimumHeight(420)
            view.liquidity_overlay_card.setMaximumWidth(1100)
            overlay_layout.addWidget(view.liquidity_overlay_card, alignment=Qt.AlignmentFlag.AlignCenter)
            overlay_layout.addStretch()

            card_layout = QVBoxLayout(view.liquidity_overlay_card)
            card_layout.setContentsMargins(16, 12, 16, 16)
            card_layout.setSpacing(10)

            overlay_header = QHBoxLayout()
            overlay_title = QLabel("Liquidez")
            overlay_title.setStyleSheet("font-weight: 600;")
            overlay_header.addWidget(overlay_title)
            overlay_header.addStretch()
            view.liquidity_overlay_close = QPushButton("X")
            view.liquidity_overlay_close.setFixedSize(26, 26)
            view.liquidity_overlay_close.clicked.connect(
                lambda _checked=False, v=view: self.close_liquidity_overlay(v)
            )
            overlay_header.addWidget(view.liquidity_overlay_close)
            card_layout.addLayout(overlay_header)

            view.liquidity_overlay_tabs = QTabWidget()
            view.liquidity_overlay_charts = {}
            view.liquidity_overlay_lists = {}
            view.liquidity_overlay_totals = {}
            for currency in ("ARS", "USD"):
                tab = QWidget()
                tab_layout = QVBoxLayout(tab)
                tab_layout.setContentsMargins(0, 0, 0, 0)
                tab_layout.setSpacing(8)

                chart_list_layout = QHBoxLayout()
                chart_list_layout.setSpacing(20)

                chart_container = QFrame()
                chart_layout = QVBoxLayout(chart_container)
                chart_container.setSizePolicy(
                    QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding
                )
                chart_list_layout.addWidget(chart_container, 3)

                list_container = QFrame()
                list_container.setStyleSheet("QFrame { background: transparent; border: none; }")
                list_layout = QVBoxLayout(list_container)
                list_layout.setContentsMargins(0, 0, 0, 0)
                list_widget = QListWidget()
                list_widget.setFrameShape(QFrame.Shape.NoFrame)
                list_widget.setSpacing(6)
                list_widget.setStyleSheet(
                    "QListWidget { background: transparent; }"
                    "QListWidget::item { padding: 4px 2px; }"
                )
                list_layout.addWidget(list_widget)
                list_container.setMinimumWidth(260)
                chart_list_layout.addWidget(list_container, 1)

                tab_layout.addLayout(chart_list_layout)
                total_label = QLabel("")
                tab_layout.addWidget(total_label)

                view.liquidity_overlay_charts[currency] = chart_layout
                view.liquidity_overlay_lists[currency] = list_widget
                view.liquidity_overlay_totals[currency] = total_label
                view.liquidity_overlay_tabs.addTab(tab, currency)

            card_layout.addWidget(view.liquidity_overlay_tabs)

        # Crear pestañas internas
        view.portfolio_tabs = QTabWidget()
        layout.addWidget(view.portfolio_tabs)

        # Tabla
        view.table_tab = QWidget()
        view.table_layout = QVBoxLayout(view.table_tab)

        view.portfolio_table = QTableWidget()
        view.portfolio_table.setColumnCount(16)
        view.portfolio_table.setHorizontalHeaderLabels([
            "Categoría",
            "Ticker",
            "Moneda",
            "Cantidad\nNominal",
            "Variación\nDiaria",
            "Precio Operación\nde Compra",
            "Valor Operación\nde Compra",
            "Precio\nÚltimo Operado",
            "Valor\nActual (moneda)",
            "Valor ARS",
            "Valor USD",
            "% del\nPortafolio (ARS)",
            "Resultado ARS (%)",
            "Resultado USD",
            "Comisiones ARS",
            "Comisiones USD"
        ])
        header = view.portfolio_table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        header.setStretchLastSection(True)
        view.portfolio_table.verticalHeader().setVisible(False)
        view.portfolio_table.setSizeAdjustPolicy(QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents)

        view.table_layout.addWidget(view.portfolio_table)

        # Gráfico
        view.chart_tab = QWidget()
        view.chart_layout = QHBoxLayout(view.chart_tab)

        view.category_chart_widget = QWidget()
        view.category_chart_layout = QVBoxLayout(view.category_chart_widget)

        view.symbol_chart_widget = QWidget()
        view.symbol_chart_layout = QVBoxLayout(view.symbol_chart_widget)
        view.symbol_chart_label = QLabel("Seleccione una categoría para ver detalles")
        view.symbol_chart_layout.addWidget(view.symbol_chart_label)

        view.chart_layout.addWidget(view.category_chart_widget)
        view.chart_layout.addWidget(view.symbol_chart_widget)

        # Añadir pestañas
        view.portfolio_tabs.addTab(view.table_tab, "Tabla")
        view.portfolio_tabs.addTab(view.chart_tab, "Gráfico")
        if is_user:
            view.portfolio_tabs.setTabText(0, "ACTIVOS")

        # 1) Marco y layout principal de controles
        control_frame  = QFrame()
        control_layout = QHBoxLayout(control_frame)
        control_layout.setSpacing(10)
        control_layout.setContentsMargins(0, 0, 0, 0)

        # 2) Botón "Actualizar Datos"
        view.actualizar_btn = QPushButton("Actualizar Datos")
        view.actualizar_btn.clicked.connect(self.actualizar_datos_mercado)
        control_layout.addWidget(view.actualizar_btn)

        # 3) Bloque de auto-actualización con QGridLayout
        auto_frame  = QFrame()
        auto_layout = QGridLayout(auto_frame)
        auto_layout.setContentsMargins(0, 0, 0, 0)
        auto_layout.setHorizontalSpacing(0)
        auto_layout.setVerticalSpacing(0)

        #   Fila 0: checkbox ocupando ambas columnas
        view.auto_update_check = QCheckBox("Actualización Automática")
        view.auto_update_check.stateChanged.connect(
            lambda state, v=view: self.toggle_auto_update(state, v)
        )
        auto_layout.addWidget(view.auto_update_check, 0, 0, 1, 2, alignment=Qt.AlignmentFlag.AlignLeft)

        #   Fila 1: "Intervalo (min):" + campo de texto
        view.interval_label = QLabel("Intervalo (min):")
        view.interval_edit  = QLineEdit(str(self.auto_update_interval))
        view.interval_edit.setFixedWidth(40)
        view.interval_edit.textChanged.connect(
            lambda text, v=view: self.update_interval(text, v)
        )
        auto_layout.addWidget(view.interval_label, 1, 0, alignment=Qt.AlignmentFlag.AlignLeft)
        auto_layout.addWidget(view.interval_edit,  1, 1, alignment=Qt.AlignmentFlag.AlignLeft)

        #   Fila 2: cuenta atrás "Próxima actualización"
        view.countdown_label = QLabel("Próxima actualización: --:--")
        auto_layout.addWidget(view.countdown_label, 2, 0, 1, 2, alignment=Qt.AlignmentFlag.AlignLeft)

        control_layout.addWidget(auto_frame)

        # 4) Grupo vertical para "Última actualización"
        status_group = QVBoxLayout()
        status_group.setSpacing(0)
        status_group.setContentsMargins(0, 0, 0, 0)

        view.status_label = QLabel(
            f"Última actualización: {self.last_update if self.last_update else '-'}"
        )
        status_group.addWidget(view.status_label, alignment=Qt.AlignmentFlag.AlignLeft)

        # Filtro de moneda
        filter_layout = QHBoxLayout()
        filter_layout.setSpacing(5)
        filter_layout.addWidget(QLabel("Moneda:"))
        view.currency_filter_combo = QComboBox()
        view.currency_filter_combo.addItems(["Todos", "ARS", "USD"])
        view.currency_filter_combo.currentTextChanged.connect(
            lambda _text, v=view: self.load_portfolio(v)
        )
        filter_layout.addWidget(view.currency_filter_combo)
        if is_user:
            group_layout = QHBoxLayout()
            group_layout.setSpacing(6)
            group_layout.addWidget(QLabel("Agrupar:"))
            view.group_by = "tipo"
            view.group_by_tipo_btn = QPushButton("Instrumento")
            view.group_by_tipo_btn.setCheckable(True)
            view.group_by_tipo_btn.setChecked(True)
            view.group_by_tipo_btn.clicked.connect(
                lambda _checked=False, v=view: self.set_user_grouping("tipo", v)
            )
            view.group_by_broker_btn = QPushButton("Broker")
            view.group_by_broker_btn.setCheckable(True)
            view.group_by_broker_btn.clicked.connect(
                lambda _checked=False, v=view: self.set_user_grouping("broker", v)
            )
            group_layout.addWidget(view.group_by_tipo_btn)
            group_layout.addWidget(view.group_by_broker_btn)
            filter_layout.addLayout(group_layout)
        status_group.addLayout(filter_layout)

        control_layout.addLayout(status_group)

        # 5) Stretch para empujar el botón final a la derecha
        control_layout.addStretch()

        # 6) Botón "Actualizar Portafolio"
        view.update_portfolio_btn = QPushButton("Actualizar Portafolio")
        view.update_portfolio_btn.clicked.connect(
            lambda _checked=False, v=view: self.load_portfolio(v)
        )
        control_layout.addWidget(view.update_portfolio_btn)

        # 7) Insertar todo en el layout global de la ventana/dialog
        layout.addWidget(control_frame)

        # Conectar cambio de pestaña
        view.portfolio_tabs.currentChanged.connect(
            lambda index, v=view: self.on_portfolio_tab_changed(index, v)
        )

        return view

    def toggle_auto_update(self, state, view):
        self.auto_update_active = (state == Qt.CheckState.Checked.value)
        self.sync_auto_update_controls(source_view=view)
        if self.auto_update_active:
            self.start_auto_update()
        else:
            self.stop_auto_update()

    def eventFilter(self, obj, event):
        if event.type() == QEvent.Type.Resize:
            for view in self.get_portfolio_views():
                if getattr(view, "overlay_parent", None) is obj and hasattr(view, "liquidity_overlay"):
                    view.liquidity_overlay.setGeometry(obj.rect())
        return super().eventFilter(obj, event)

    def detect_fx_rate(self):
        """Intenta obtener un tipo de cambio MEP desde los datos de mercado usando AL30/AL30D"""
        fx_rate = self.get_fx_rate_for_date(datetime.now(), "Acciones AR") or 1.0
        if self.df_mercado is not None:
            symbol_col = next(
                (c for c in ["Símbolo.1", "Símbolo", "Simbolo", "Symbol", "Ticker"] if c in self.df_mercado.columns),
                None,
            )
            price_col = next(
                (c for c in ["Último Operado", "Ultimo Operado", "Precio", "Close"] if c in self.df_mercado.columns),
                None,
            )
            if symbol_col and price_col:
                try:
                    al30 = self.df_mercado[self.df_mercado[symbol_col] == "AL30"].iloc[0][price_col]
                    al30d = self.df_mercado[self.df_mercado[symbol_col] == "AL30D"].iloc[0][price_col]
                    if isinstance(al30, str):
                        al30 = al30.replace('.', '').replace(',', '.')
                    if isinstance(al30d, str):
                        al30d = al30d.replace('.', '').replace(',', '.')
                    al30 = float(al30)
                    al30d = float(al30d)
                    if al30d != 0:
                        fx_rate = al30 / al30d
                except Exception:
                    pass
        self.default_fx_rate = fx_rate if fx_rate > 0 else 1.0
        return self.default_fx_rate

    def update_interval(self, text, view):
        try:
            interval = int(text)
            if interval > 0:
                self.auto_update_interval = interval
                self.sync_auto_update_controls(source_view=view)
                if self.auto_update_active:
                    self.start_auto_update()
        except ValueError:
            pass

    def start_auto_update(self):
        """Iniciar la actualización automática con cuenta regresiva"""
        self.auto_update_timer.stop()
        self.countdown_timer.stop()

        # Convertir minutos a milisegundos
        interval_ms = self.auto_update_interval * 60 * 1000
        self.auto_update_timer.start(interval_ms)

        # Iniciar cuenta regresiva
        self.remaining_time = self.auto_update_interval * 60
        self.update_countdown()
        self.countdown_timer.start(1000)

    def stop_auto_update(self):
        """Detener la actualización automática"""
        self.auto_update_timer.stop()
        self.countdown_timer.stop()
        for view in self.get_portfolio_views():
            view.countdown_label.setText("Próxima actualización: --:--")

    def on_portfolio_tab_changed(self, index, view):
        if view.portfolio_tabs.tabText(index) == "Gráfico":
            self.draw_category_pie_chart(view)

    def get_cash_by_broker(self, moneda="ARS"):
        balances = {broker: 0.0 for broker in BROKERS}
        try:
            journal_rows = fetch_journal()
            computed = compute_cash_by_broker(journal_rows, moneda)
            for broker, val in computed.items():
                balances[broker] = balances.get(broker, 0.0) + val
        except Exception as e:
            print(f"Error calculando efectivo por broker: {e}")
        return balances

    def get_holdings_by_broker(self):
        holdings = {broker: {} for broker in BROKERS}
        try:
            journal_rows = fetch_journal()
            computed = compute_holdings_by_broker(journal_rows)
            for broker, symbols in computed.items():
                holdings[broker] = symbols
        except Exception as e:
            print(f"Error calculando posiciones por broker: {e}")
        return holdings

    def load_portfolio(self, view=None):
        if view is None:
            view = self.user_portfolio_view

        # Limpiar tabla
        view.portfolio_table.setRowCount(0)

        plazo_fijo_detalles = {}
        descuentos_por_simbolo = {}
        rendimientos_por_simbolo = {}
        tc_weighted_by_symbol = {}
        tc_weighted_amount = {}
        try:
            for row in fetch_journal():
                simbolo = row.get("simbolo", "")
                tipo_op = row.get("tipo_operacion", "")
                tipo = row.get("tipo", "")
                total_desc = row.get("total_descuentos", 0)
                rendimiento_val = row.get("rendimiento", 0)

                # Recolectar descuentos
                if tipo_op in ["Compra", "Dividendos"]:
                    try:
                        total_desc = float(str(total_desc).replace(',', '.'))
                    except Exception:
                        total_desc = 0
                    if simbolo not in descuentos_por_simbolo:
                        descuentos_por_simbolo[simbolo] = 0
                    descuentos_por_simbolo[simbolo] += total_desc

                # Recolectar rendimientos
                if tipo_op == "Dividendos":
                    try:
                        rendimiento = float(str(rendimiento_val).replace(',', '.'))
                    except Exception:
                        rendimiento = 0
                    if simbolo not in rendimientos_por_simbolo:
                        rendimientos_por_simbolo[simbolo] = 0
                    rendimientos_por_simbolo[simbolo] += rendimiento

                if tipo_op == "Compra" and simbolo:
                    try:
                        cantidad = float(row.get("cantidad", 0) or 0)
                        precio = float(str(row.get("precio", 0)).replace(",", ".") or 0)
                    except Exception:
                        cantidad = 0
                        precio = 0
                    monto = cantidad * precio
                    if monto > 0:
                        tc_val = 0
                        try:
                            fecha_dt = datetime.strptime(row.get("fecha", ""), "%Y-%m-%d")
                            tc_val = self.get_fx_rate_for_date(fecha_dt, tipo) or 0
                        except Exception:
                            tc_val = 0
                        if tc_val <= 0:
                            tc_row = row.get("tc_usd_ars", 0) or 0
                            try:
                                tc_val = float(tc_row)
                            except Exception:
                                tc_val = 0
                        if tc_val > 0:
                            tc_weighted_by_symbol[simbolo] = tc_weighted_by_symbol.get(simbolo, 0.0) + (tc_val * monto)
                            tc_weighted_amount[simbolo] = tc_weighted_amount.get(simbolo, 0.0) + monto

                if tipo == "Plazo Fijo" and simbolo:
                    plazo_fijo_detalles[simbolo] = row.get("detalle", "")
        except Exception as e:
            print(f"Error leyendo journal para portfolio: {e}")

        # Calcular efectivo por moneda/broker
        cash_by_broker_ars = self.get_cash_by_broker("ARS")
        cash_by_broker_usd = self.get_cash_by_broker("USD")
        fx_rate = self.detect_fx_rate()
        if getattr(view, "is_user", False):
            view.liquidity_by_broker = {
                "ARS": cash_by_broker_ars,
                "USD": cash_by_broker_usd
            }

        portfolio_data = []
        # Inicializar total_valor a 0 (se calcularó despuós)
        total_valor = 0.0
        tipo_valores = {}

        # Agregar efectivo por broker combinando ARS y USD en una fila
        cash_combined = {}
        for broker, amount in cash_by_broker_ars.items():
            if abs(amount) >= 0.0001:
                cash_combined.setdefault(broker, {'ars': 0.0, 'usd': 0.0})
                cash_combined[broker]['ars'] += amount
        for broker, amount in cash_by_broker_usd.items():
            if abs(amount) >= 0.0001:
                cash_combined.setdefault(broker, {'ars': 0.0, 'usd': 0.0})
                cash_combined[broker]['usd'] += amount

        for broker, amounts in cash_combined.items():
            moneda_label = "ARS/USD" if amounts['ars'] and amounts['usd'] else ("ARS" if amounts['ars'] else "USD")
            portfolio_data.append({
                'tipo': "Efectivo",
                'broker': broker,
                'moneda': moneda_label,
                'simbolo': broker,
                'simbolo_display': f"Liquidez {broker}",
                'detalle': f"Liquidez en {broker}",
                'precio_prom': 1.0,
                'cantidad': 0,  # no aplica cantidad única
                'precio_actual': 1.0,
                'variacion_diaria': 0.0,
                'monto_ars': amounts.get('ars', 0.0),
                'monto_usd': amounts.get('usd', 0.0)
            })

        # Cargar otros activos
        # Cargar otros activos desde base de datos
        with get_conn() as conn:
            cur = conn.execute("SELECT simbolo, broker, tipo, moneda, cantidad, precio_prom FROM portfolio")
            for row in cur.fetchall():
                simbolo = row['simbolo']
                broker = row['broker']
                tipo = row['tipo']
                moneda = row['moneda']
                cantidad = float(row['cantidad'])
                precio_prom = float(row['precio_prom'])
                simbolo_display = simbolo

                if tipo == "Plazo Fijo":
                    detalle = plazo_fijo_detalles.get(simbolo, "")
                    if detalle:
                        simbolo_display = f"{simbolo} ({detalle})"

                portfolio_data.append({
                    'tipo': tipo,
                    'broker': broker,
                    'moneda': moneda,
                    'simbolo': simbolo,
                    'simbolo_display': simbolo_display,
                    'detalle': "",
                    'precio_prom': precio_prom,
                    'cantidad': cantidad
                })

        # Filtrar entradas obsoletas de "Efectivo Líquido"
        portfolio_data = [p for p in portfolio_data if p.get('tipo') != "Efectivo Líquido"]

        crypto_prices = {}
        try:
            crypto_symbols = [
                (p.get("simbolo") or "").strip().upper()
                for p in portfolio_data
                if p.get("tipo") == "Criptomonedas"
            ]
            crypto_prices = fetch_crypto_prices(crypto_symbols)
        except Exception as e:
            print(f"Error leyendo precios cripto: {e}")

        display_currency = "ARS"
        if getattr(view, "is_user", False):
            display_currency = getattr(view, "asset_currency_view", "ARS")

        # Calcular los valores para cada activo
        total_valor_ars = 0.0
        total_valor_usd = 0.0
        total_valor_actual_usd = 0.0  # para sumatoria de columna Valor USD

        for item in portfolio_data:
            moneda_item = item.get('moneda', 'ARS')
            # Para Efectivo
            if item['tipo'] == "Efectivo":
                item['precio_operacion_compra'] = 1.0
                item['valor_compra'] = item['cantidad'] * item['precio_operacion_compra']
                item['precio_actual'] = 1.0
                item['valor_actual'] = item['cantidad'] * item['precio_actual']
                item['diferencia_valor'] = item['valor_actual'] - item['valor_compra']
                item['variacion_diaria'] = 0.0

            # Para Plazo Fijo
            elif item['tipo'] == "Plazo Fijo":
                item['precio_operacion_compra'] = 1.0
                item['valor_compra'] = item['cantidad'] * item['precio_operacion_compra']
                item['precio_actual'] = 1.0
                item['valor_actual'] = item['cantidad'] * item['precio_actual']
                item['diferencia_valor'] = item['valor_actual'] - item['valor_compra']
                item['variacion_diaria'] = 0.0

            else:  # Para otros activos
                if item['simbolo'] in self.compras_pendientes and self.compras_pendientes[item['simbolo']]:
                    total_cantidad = sum([compra[0] for compra in self.compras_pendientes[item['simbolo']]])
                    total_inversion = sum([compra[0] * compra[1] for compra in self.compras_pendientes[item['simbolo']]])
                    if total_cantidad > 0:
                        item['precio_operacion_compra'] = total_inversion / total_cantidad
                    else:
                        item['precio_operacion_compra'] = 0
                else:
                    item['precio_operacion_compra'] = 0

                item['valor_compra'] = item['cantidad'] * item['precio_operacion_compra']

                # Obtener precio actual y variación diaria del mercado
                item['precio_actual'] = None
                item['variacion_diaria'] = None
                if item['tipo'] == "Criptomonedas":
                    symbol_key = (item.get("simbolo") or "").strip().upper()
                    price_row = crypto_prices.get(symbol_key)
                    if price_row:
                        item['precio_actual'] = self._safe_number(price_row.get("price_usd"), None)
                        item['variacion_diaria'] = self._safe_number(price_row.get("change_24h"), None)
                if item['precio_actual'] is None and self.df_mercado is not None:
                    simbolo = item['simbolo']
                    if item['tipo'] == "Plazo Fijo" and "(" in simbolo:
                        simbolo = simbolo.split("(")[0].strip()

                    # Buscar columnas disponibles en el CSV de mercado
                    symbol_col = next(
                        (c for c in ["Símbolo.1", "Símbolo", "Simbolo", "Symbol", "Ticker"] if c in self.df_mercado.columns),
                        None,
                    )
                    price_col = next(
                        (c for c in ["Último Operado", "Ultimo Operado", "Precio", "Close"] if c in self.df_mercado.columns),
                        None,
                    )
                    var_col = next(
                        (c for c in ["Variación Diaria", "Variacion Diaria", "Var.%", "Variación", "Change %"] if c in self.df_mercado.columns),
                        None,
                    )

                    if symbol_col and price_col:
                        match = self.df_mercado[self.df_mercado[symbol_col] == simbolo]
                    else:
                        match = pd.DataFrame()

                    if not match.empty:
                            try:
                                ultimo_operado = match.iloc[0].get(price_col)
                                if isinstance(ultimo_operado, str):
                                    ultimo_operado = ultimo_operado.replace('.', '').replace(',', '.')
                                ultimo_operado = self._safe_number(ultimo_operado, None)
                                if ultimo_operado is None:
                                    item['precio_actual'] = None
                                elif item['tipo'] == "Bonos AR":
                                    item['precio_actual'] = ultimo_operado * 0.01
                                else:
                                    item['precio_actual'] = ultimo_operado
                            except Exception:
                                item['precio_actual'] = None

                            if var_col:
                                try:
                                    variacion = match.iloc[0].get(var_col)
                                    if isinstance(variacion, str):
                                        variacion = variacion.replace('%', '').replace(',', '.').strip()
                                        if variacion.endswith('%'):
                                            variacion = variacion[:-1]
                                    item['variacion_diaria'] = self._safe_number(variacion, None)
                                except Exception:
                                    item['variacion_diaria'] = None

                if item['precio_actual'] is not None:
                    item['valor_actual'] = item['cantidad'] * item['precio_actual']
                else:
                    item['valor_actual'] = item['valor_compra']
                item['valor_actual'] = self._safe_number(item['valor_actual'], item['valor_compra'])

                item['diferencia_valor'] = item['valor_actual'] - item['valor_compra']

            # Calcular descuentos totales (base = comisiones del libro)
            descuento_total = descuentos_por_simbolo.get(item['simbolo'], 0)
            item['comisiones_base'] = descuento_total

            # Calcular descuento adicional según tipo
            if item['tipo'] in ["Acciones AR", "CEDEARs", "ETFs" , "Criptomonedas"]:
                if item['valor_actual'] is not None:
                    descuento_adicional = 0.008228 * item['valor_actual']
                else:
                    descuento_adicional = 0
                item['descuentos'] = descuento_total + descuento_adicional
            elif item['tipo'] == "Bonos AR":
                if item['valor_actual'] is not None:
                    descuento_adicional = 0.006171 * item['valor_actual']
                else:
                    descuento_adicional = 0
                item['descuentos'] = descuento_total + descuento_adicional
            else:  # Efectivo, Plazo Fijo
                item['descuentos'] = 0

            # Calcular rendimientos acumulados
            item['rendimientos'] = rendimientos_por_simbolo.get(item['simbolo'], 0.0)

            # Calcular resultado
            item['resultado'] = item['diferencia_valor'] - item['descuentos'] + item['rendimientos']

            # Acumular valor actual para calcular el total
            # Valores convertidos por tipo de activo
            tc_actual = self.get_fx_rate_for_date(datetime.now(), item.get("tipo", "")) or fx_rate
            if tc_actual <= 0:
                tc_actual = fx_rate
            simbolo = item.get("simbolo", "")
            tc_compra = fx_rate
            if simbolo in tc_weighted_by_symbol and tc_weighted_amount.get(simbolo, 0) > 0:
                tc_compra = tc_weighted_by_symbol[simbolo] / tc_weighted_amount[simbolo]
            if tc_compra <= 0:
                tc_compra = fx_rate

            item["tc_compra"] = tc_compra
            item["tc_actual"] = tc_actual

            if item['tipo'] == "Efectivo":
                valor_ars = self._safe_number(item.get('monto_ars', 0.0), 0.0)
                valor_usd = self._safe_number(item.get('monto_usd', 0.0), 0.0)
            else:
                if moneda_item == "USD":
                    valor_ars = self._safe_number(item['valor_actual'], 0.0) * tc_actual
                    valor_usd = self._safe_number(item['valor_actual'], 0.0)
                else:
                    valor_ars = self._safe_number(item['valor_actual'], 0.0)
                    valor_usd = (
                        self._safe_number(item['valor_actual'], 0.0) / tc_actual
                        if tc_actual
                        else self._safe_number(item['valor_actual'], 0.0)
                    )
            item['valor_ars'] = valor_ars
            item['valor_usd'] = valor_usd

            total_valor_ars += valor_ars
            total_valor_usd += valor_usd
            if item['tipo'] in tipo_valores:
                tipo_valores[item['tipo']] += valor_ars
            else:
                tipo_valores[item['tipo']] = valor_ars
            total_valor_actual_usd += valor_usd

        table_data = portfolio_data
        total_assets_ars = total_valor_ars
        if getattr(view, "is_user", False):
            table_data = [p for p in portfolio_data if p.get('tipo') not in ("Efectivo", "Efectivo Líquido")]
            total_assets_ars = sum(p.get('valor_ars', 0) for p in table_data)
            total_valor_ars = total_assets_ars
            total_valor_usd = sum(p.get('valor_usd', 0) for p in table_data)
            tipo_valores = {}
            for p in table_data:
                tipo_valores[p['tipo']] = tipo_valores.get(p['tipo'], 0) + p.get('valor_ars', 0)

        # Aplicar filtro de moneda si corresponde
        filtro_moneda = getattr(view, "currency_filter_combo", None)
        moneda_seleccionada = filtro_moneda.currentText() if filtro_moneda else "Todos"
        if moneda_seleccionada != "Todos":
            table_data = [p for p in table_data if p.get('moneda', 'ARS') == moneda_seleccionada]
            # Recalcular categorías con el filtro aplicado
            tipo_valores = {}
            for p in table_data:
                tipo_valores[p['tipo']] = tipo_valores.get(p['tipo'], 0) + p.get('valor_ars', 0)
            total_valor_ars = sum(p.get('valor_ars', 0) for p in table_data)
            total_valor_usd = sum(p.get('valor_usd', 0) for p in table_data)
            total_valor = total_valor_usd if display_currency == "USD" else total_valor_ars
        else:
            total_valor = total_valor_usd if display_currency == "USD" else total_valor_ars

        if getattr(view, "is_user", False) and getattr(view, "group_by", "tipo") == "tipo":
            grouped = {}
            passthrough = []
            for item in table_data:
                simbolo = item.get("simbolo")
                if not simbolo:
                    passthrough.append(item)
                    continue
                key = (item.get("tipo"), simbolo, item.get("moneda", "ARS"))
                if key not in grouped:
                    grouped[key] = {
                        **item,
                        "cantidad": 0.0,
                        "valor_compra": 0.0,
                        "valor_actual": 0.0,
                        "valor_ars": 0.0,
                        "valor_usd": 0.0,
                        "descuentos": 0.0,
                        "rendimientos": 0.0,
                        "comisiones_base": 0.0,
                        "_brokers": set(),
                    }
                g = grouped[key]
                g["_brokers"].add(item.get("broker"))
                g["cantidad"] += self._safe_number(item.get("cantidad", 0.0), 0.0)
                g["valor_compra"] += self._safe_number(item.get("valor_compra", 0.0), 0.0)
                g["valor_actual"] += self._safe_number(item.get("valor_actual", 0.0), 0.0)
                g["valor_ars"] += self._safe_number(item.get("valor_ars", 0.0), 0.0)
                g["valor_usd"] += self._safe_number(item.get("valor_usd", 0.0), 0.0)
                g["descuentos"] += self._safe_number(item.get("descuentos", 0.0), 0.0)
                g["rendimientos"] += self._safe_number(item.get("rendimientos", 0.0), 0.0)
                g["comisiones_base"] += self._safe_number(item.get("comisiones_base", 0.0), 0.0)
                if g.get("precio_actual") is None and item.get("precio_actual") is not None:
                    g["precio_actual"] = item.get("precio_actual")
                if g.get("variacion_diaria") is None and item.get("variacion_diaria") is not None:
                    g["variacion_diaria"] = item.get("variacion_diaria")

            aggregated = []
            for g in grouped.values():
                if g["cantidad"] > 0:
                    g["precio_operacion_compra"] = g["valor_compra"] / g["cantidad"]
                    g["precio_actual"] = g["valor_actual"] / g["cantidad"]
                else:
                    g["precio_operacion_compra"] = 0.0
                g["diferencia_valor"] = g["valor_actual"] - g["valor_compra"]
                g["resultado"] = g["diferencia_valor"] - g["descuentos"] + g["rendimientos"]
                brokers = g.pop("_brokers", set())
                g["broker"] = "Varios" if len(brokers) > 1 else next(iter(brokers), "")
                aggregated.append(g)

            table_data = aggregated + passthrough
            tipo_valores = {}
            for p in table_data:
                tipo_valores[p['tipo']] = tipo_valores.get(p['tipo'], 0) + p.get('valor_ars', 0)
            total_valor_ars = sum(p.get('valor_ars', 0) for p in table_data)
            total_valor_usd = sum(p.get('valor_usd', 0) for p in table_data)
            total_valor = total_valor_usd if display_currency == "USD" else total_valor_ars

        if getattr(view, "is_user", False):
            self.update_liquidity_section(view, total_assets_ars, fx_rate)

            summary_result_ars = 0.0
            summary_result_usd = 0.0
            summary_cost_ars = 0.0
            summary_cost_usd = 0.0
            total_compra_ars = 0.0
            total_compra_usd = 0.0
            total_actual_ars = 0.0
            total_actual_usd = 0.0
            total_comisiones_ars = 0.0
            total_comisiones_usd = 0.0
            for p in table_data:
                valor_compra = self._safe_number(p.get("valor_compra", 0.0), 0.0)
                valor_actual = self._safe_number(p.get("valor_actual", 0.0), 0.0)
                moneda_item = p.get("moneda", "ARS")
                tc_compra = fx_rate
                simbolo = p.get("simbolo", "")
                if simbolo in tc_weighted_by_symbol and tc_weighted_amount.get(simbolo, 0) > 0:
                    tc_compra = tc_weighted_by_symbol[simbolo] / tc_weighted_amount[simbolo]
                if tc_compra <= 0:
                    tc_compra = fx_rate
                tc_actual = self.get_fx_rate_for_date(datetime.now(), p.get("tipo", "")) or fx_rate

                if moneda_item == "USD":
                    valor_compra_ars = valor_compra * tc_compra
                    valor_actual_ars = valor_actual * tc_actual
                    valor_compra_usd = valor_compra
                    valor_actual_usd = valor_actual
                else:
                    valor_compra_ars = valor_compra
                    valor_actual_ars = valor_actual
                    valor_compra_usd = (valor_compra / tc_compra) if tc_compra else 0.0
                    valor_actual_usd = (valor_actual / tc_actual) if tc_actual else 0.0

                comisiones_base = self._safe_number(p.get("comisiones_base", 0.0), 0.0)
                if moneda_item == "USD":
                    comisiones_ars = comisiones_base * tc_compra
                    comisiones_usd = comisiones_base
                else:
                    comisiones_ars = comisiones_base
                    comisiones_usd = (comisiones_base / tc_compra) if tc_compra else 0.0

                total_compra_ars += valor_compra_ars
                total_compra_usd += valor_compra_usd
                total_actual_ars += valor_actual_ars
                total_actual_usd += valor_actual_usd
                total_comisiones_ars += comisiones_ars
                total_comisiones_usd += comisiones_usd

            summary_result_ars = (total_actual_ars - total_compra_ars) - total_comisiones_ars
            summary_result_usd = (total_actual_usd - total_compra_usd) - total_comisiones_usd
            summary_cost_ars = total_compra_ars + total_comisiones_ars
            summary_cost_usd = total_compra_usd + total_comisiones_usd
            pct_ars = (summary_result_ars / summary_cost_ars * 100) if summary_cost_ars > 0 else 0.0
            pct_usd = (summary_result_usd / summary_cost_usd * 100) if summary_cost_usd > 0 else 0.0
            debug = (
                f"ARS compra: {total_compra_ars:,.2f}\\n"
                f"ARS actual: {total_actual_ars:,.2f}\\n"
                f"ARS comisiones: {total_comisiones_ars:,.2f}\\n"
                f"USD compra: {total_compra_usd:,.2f}\\n"
                f"USD actual: {total_actual_usd:,.2f}\\n"
                f"USD comisiones: {total_comisiones_usd:,.2f}"
            )
            self.update_portfolio_summary(view, summary_result_ars, summary_result_usd, pct_ars, pct_usd, debug)

        # Orden de categorías
        if getattr(view, "is_user", False):
            if getattr(view, "group_by", "tipo") == "broker":
                orden_categorias = {}
            else:
                orden_categorias = {
                    "Plazo Fijo": 1,
                    "FCI": 2,
                    "Bonos AR": 3,
                    "Acciones AR": 4,
                    "CEDEARs": 5,
                    "ETFs": 6,
                    "Criptomonedas": 7,
                    "Cauciones": 8
                }
        else:
            orden_categorias = {
                "Liquidez Actual": 0,
                "Plazo Fijo": 1,
                "FCI": 2,
                "Bonos AR": 3,
                "Acciones AR": 4,
                "CEDEARs": 5,
                "ETFs": 6,
                "Criptomonedas": 7,
                "Cauciones": 8
            }

        categorias = {}
        group_by = getattr(view, "group_by", "tipo")
        for item in table_data:
            if getattr(view, "is_user", False) and group_by == "broker":
                cat = item.get("broker", "Sin broker")
            else:
                cat = item['tipo']
                # Renombrar efectivo a Liquidez Actual
                if not getattr(view, "is_user", False) and cat in ["Efectivo", "Efectivo Líquido"]:
                    cat = "Liquidez Actual"
            if cat not in categorias:
                categorias[cat] = []
            categorias[cat].append(item)

        # Guardar datos para gráficos
        view.portfolio_data = table_data
        view.categorias = categorias
        self.total_valor = total_valor
        self.total_valor_usd = total_valor_usd

        # Construir la tabla
        row_idx = 0
        total_valor_actual = 0
        total_resultado = 0
        total_valor_usd_tabla = 0
        total_resultado_usd_tabla = 0
        total_comisiones_ars = 0
        total_comisiones_usd = 0
        suma_porcentajes = 0.0

        # Recorrer categorías en el orden definido
        for cat in sorted(categorias.keys(), key=lambda x: orden_categorias.get(x, 10)):
            items = categorias[cat]
            cat_total_ars = sum(item['valor_ars'] for item in items)
            cat_total_usd = sum(item['valor_usd'] for item in items)
            cat_total_compra_ars = 0.0
            cat_total_compra_usd = 0.0
            cat_total_resultado = 0.0
            cat_total_resultado_usd = 0.0
            for item in items:
                if item.get("precio_actual") is None:
                    continue
                moneda_item = item.get("moneda", "ARS")
                simbolo = item.get("simbolo", "")
                tc_compra = item.get("tc_compra", fx_rate) or fx_rate
                if tc_compra <= 0:
                    tc_compra = fx_rate
                tc_actual = item.get("tc_actual", fx_rate) or fx_rate
                valor_compra = self._safe_number(item.get("valor_compra", 0.0), 0.0)
                valor_actual = self._safe_number(item.get("valor_actual", 0.0), 0.0)
                if moneda_item == "USD":
                    valor_compra_ars = valor_compra * tc_compra
                    valor_actual_ars = valor_actual * tc_actual
                    valor_compra_usd = valor_compra
                    valor_actual_usd = valor_actual
                else:
                    valor_compra_ars = valor_compra
                    valor_actual_ars = valor_actual
                    valor_compra_usd = (valor_compra / tc_compra) if tc_compra else 0.0
                    valor_actual_usd = (valor_actual / tc_actual) if tc_actual else 0.0
                cat_total_compra_ars += valor_compra_ars
                cat_total_compra_usd += valor_compra_usd
                cat_total_resultado += (valor_actual_ars - valor_compra_ars)
                cat_total_resultado_usd += (valor_actual_usd - valor_compra_usd)

            # Fila de categoría
            view.portfolio_table.insertRow(row_idx)
            view.portfolio_table.setItem(row_idx, 0, QTableWidgetItem(cat))
            view.portfolio_table.setItem(row_idx, 9, QTableWidgetItem(f"${cat_total_ars:,.2f}"))
            view.portfolio_table.setItem(row_idx, 10, QTableWidgetItem(f"${cat_total_usd:,.2f}"))

            if total_valor > 0:
                cat_total_view = cat_total_usd if display_currency == "USD" else cat_total_ars
                cat_porcentaje = (cat_total_view / total_valor) * 100
                view.portfolio_table.setItem(row_idx, 11, QTableWidgetItem(f"{cat_porcentaje:.2f}%"))
            if getattr(view, "is_user", False):
                if cat_total_compra_ars > 0:
                    cat_rend_pct_ars = (cat_total_resultado / cat_total_compra_ars) * 100
                else:
                    cat_rend_pct_ars = 0
                if cat_total_compra_usd > 0:
                    cat_rend_pct_usd = (cat_total_resultado_usd / cat_total_compra_usd) * 100
                else:
                    cat_rend_pct_usd = 0
                cat_resultado_str = f"${cat_total_resultado:,.2f} ({cat_rend_pct_ars:.2f}%)"
                view.portfolio_table.setItem(row_idx, 12, QTableWidgetItem(cat_resultado_str))
                cat_resultado_usd_str = f"${cat_total_resultado_usd:,.2f} ({cat_rend_pct_usd:.2f}%)"
                view.portfolio_table.setItem(row_idx, 13, QTableWidgetItem(cat_resultado_usd_str))
                cat_comisiones_ars = 0.0
                cat_comisiones_usd = 0.0
                for item in items:
                    descuentos = self._safe_number(item.get("comisiones_base", 0.0), 0.0)
                    moneda_item = item.get("moneda", "ARS")
                    tc_compra = item.get("tc_compra", fx_rate) or fx_rate
                    if tc_compra <= 0:
                        tc_compra = fx_rate
                    if moneda_item == "USD":
                        cat_comisiones_usd += descuentos
                        cat_comisiones_ars += descuentos * tc_compra
                    else:
                        cat_comisiones_ars += descuentos
                        cat_comisiones_usd += (descuentos / tc_compra) if tc_compra else 0.0
                view.portfolio_table.setItem(row_idx, 14, QTableWidgetItem(f"${cat_comisiones_ars:,.2f}"))
                view.portfolio_table.setItem(row_idx, 15, QTableWidgetItem(f"${cat_comisiones_usd:,.2f}"))

            # Estilo fila categoría
            for col in range(16):
                item = view.portfolio_table.item(row_idx, col)
                if item is None:
                    item = QTableWidgetItem("")
                    view.portfolio_table.setItem(row_idx, col, item)
                if col == 0:
                    item.setBackground(QColor(self.get_theme_value("table_category_bg", "#e0e0e0")))
                else:
                    item.setBackground(QColor(self.get_theme_value("table_category_alt_bg", "#f0f0f0")))
                font = item.font()
                font.setBold(True)
                item.setFont(font)

            row_idx += 1

            # Filas de activos
            for item in items:
                view.portfolio_table.insertRow(row_idx)

                moneda_item = item.get("moneda", "ARS")
                tc_compra = item.get("tc_compra", fx_rate) or fx_rate
                if tc_compra <= 0:
                    tc_compra = fx_rate
                tc_actual = item.get("tc_actual", fx_rate) or fx_rate
                if tc_actual <= 0:
                    tc_actual = fx_rate

                valor_compra_base = self._safe_number(item.get("valor_compra", 0.0), 0.0)
                valor_actual_base = self._safe_number(item.get("valor_actual", 0.0), 0.0)
                precio_compra_base = self._safe_number(item.get("precio_operacion_compra", 0.0), 0.0)
                precio_actual_base = item.get("precio_actual", None)

                if moneda_item == "USD":
                    valor_compra_ars = valor_compra_base * tc_compra
                    valor_compra_usd = valor_compra_base
                    valor_actual_ars = valor_actual_base * tc_actual
                    valor_actual_usd = valor_actual_base
                    precio_compra_ars = precio_compra_base * tc_compra
                    precio_compra_usd = precio_compra_base
                    precio_actual_ars = precio_actual_base * tc_actual if precio_actual_base is not None else None
                    precio_actual_usd = precio_actual_base
                else:
                    valor_compra_ars = valor_compra_base
                    valor_compra_usd = (valor_compra_base / tc_compra) if tc_compra else 0.0
                    valor_actual_ars = valor_actual_base
                    valor_actual_usd = (valor_actual_base / tc_actual) if tc_actual else 0.0
                    precio_compra_ars = precio_compra_base
                    precio_compra_usd = (precio_compra_base / tc_compra) if tc_compra else 0.0
                    precio_actual_ars = precio_actual_base
                    precio_actual_usd = (
                        (precio_actual_base / tc_actual) if (precio_actual_base is not None and tc_actual) else None
                    )

                valor_ars = valor_actual_ars
                valor_usd = valor_actual_usd
                total_valor_actual += valor_ars
                total_valor_usd_tabla += valor_usd

                if display_currency == "USD":
                    precio_compra_view = precio_compra_usd
                    valor_compra_view = valor_compra_usd
                    precio_actual_view = precio_actual_usd
                    valor_actual_view = valor_actual_usd
                else:
                    precio_compra_view = precio_compra_ars
                    valor_compra_view = valor_compra_ars
                    precio_actual_view = precio_actual_ars
                    valor_actual_view = valor_actual_ars

                # Formatear valores para mostrar
                precio_operacion_compra = f"${precio_compra_view:,.2f}" if precio_compra_base != 0 else "N/A"
                valor_operacion_compra = f"${valor_compra_view:,.2f}" if precio_compra_base != 0 else "N/A"
                precio_actual = f"${precio_actual_view:,.2f}" if precio_actual_view is not None else "N/D"
                valor_actual_str = f"${valor_actual_view:,.2f}"
                variacion_str = f"{item['variacion_diaria']:.2f}%" if item['variacion_diaria'] is not None else "N/D"
                resultado_str = "$0.00 (0.00%)"
                rendimiento_str = "0.00%"
                resultado_usd_str = "$0.00 (0.00%)"
                rendimiento_usd_str = "0.00%"
                if item['precio_actual'] is not None:
                    resultado_ars_val = valor_actual_ars - valor_compra_ars
                    resultado_usd_val = valor_actual_usd - valor_compra_usd

                    if valor_compra_ars > 0:
                        rendimiento_pct_ars = (resultado_ars_val / valor_compra_ars) * 100
                        rendimiento_str = f"{rendimiento_pct_ars:.2f}%".replace(".", ",")
                    if valor_compra_usd > 0:
                        rendimiento_pct_usd = (resultado_usd_val / valor_compra_usd) * 100
                        rendimiento_usd_str = f"{rendimiento_pct_usd:.2f}%".replace(".", ",")
                    else:
                        rendimiento_usd_str = "0.00%"

                    resultado_str = f"${resultado_ars_val:,.2f} ({rendimiento_str})"
                    resultado_usd_str = f"${resultado_usd_val:,.2f} ({rendimiento_usd_str})"
                    total_resultado += resultado_ars_val

                # Calcular % del portafolio usando el total_valor (suma de todos los valores actuales)
                if total_valor > 0:
                    valor_view = valor_usd if display_currency == "USD" else valor_ars
                    porcentaje_activo = (valor_view / total_valor) * 100
                    porcentaje_str = f"{porcentaje_activo:.2f}%"
                    suma_porcentajes += porcentaje_activo
                else:
                    porcentaje_str = "0.00%"

                # Llenar la fila
                view.portfolio_table.setItem(row_idx, 0, QTableWidgetItem(""))
                view.portfolio_table.setItem(row_idx, 1, QTableWidgetItem(item['simbolo_display']))
                view.portfolio_table.setItem(row_idx, 2, QTableWidgetItem(item.get('moneda', 'ARS')))
                if item['tipo'] in ["Efectivo", "Plazo Fijo"]:
                    cantidad_str = ""
                elif item['tipo'] == "Criptomonedas":
                    cantidad_str = f"{item['cantidad']:,.4f}".rstrip("0").rstrip(".")
                else:
                    cantidad_str = f"{item['cantidad']:,.2f}".rstrip("0").rstrip(".")
                view.portfolio_table.setItem(row_idx, 3, QTableWidgetItem(cantidad_str))
                view.portfolio_table.setItem(row_idx, 4, QTableWidgetItem(variacion_str))
                view.portfolio_table.setItem(row_idx, 5, QTableWidgetItem(precio_operacion_compra))
                view.portfolio_table.setItem(row_idx, 6, QTableWidgetItem(valor_operacion_compra))
                view.portfolio_table.setItem(row_idx, 7, QTableWidgetItem(precio_actual))
                view.portfolio_table.setItem(row_idx, 8, QTableWidgetItem(valor_actual_str))
                view.portfolio_table.setItem(row_idx, 9, QTableWidgetItem(f"${valor_ars:,.2f}"))
                view.portfolio_table.setItem(row_idx, 10, QTableWidgetItem(f"${valor_usd:,.2f}"))
                view.portfolio_table.setItem(row_idx, 11, QTableWidgetItem(porcentaje_str))
                view.portfolio_table.setItem(row_idx, 12, QTableWidgetItem(resultado_str))
                if item.get("precio_actual") is not None:
                    total_resultado_usd_tabla += resultado_usd_val
                view.portfolio_table.setItem(row_idx, 13, QTableWidgetItem(resultado_usd_str))
                comisiones_ars = 0.0
                comisiones_usd = 0.0
                if item.get("precio_actual") is not None:
                    descuentos = self._safe_number(item.get("comisiones_base", 0.0), 0.0)
                    if item.get("moneda", "ARS") == "USD":
                        comisiones_usd = descuentos
                        comisiones_ars = descuentos * tc_compra
                    else:
                        comisiones_ars = descuentos
                        comisiones_usd = (descuentos / tc_compra) if tc_compra else 0.0
                    total_comisiones_ars += comisiones_ars
                    total_comisiones_usd += comisiones_usd
                view.portfolio_table.setItem(row_idx, 14, QTableWidgetItem(f"${comisiones_ars:,.2f}"))
                view.portfolio_table.setItem(row_idx, 15, QTableWidgetItem(f"${comisiones_usd:,.2f}"))

                # Colorear celdas
                if item['variacion_diaria'] is not None:
                    if item['variacion_diaria'] > 0:
                        view.portfolio_table.item(row_idx, 4).setForeground(QColor('green'))
                    elif item['variacion_diaria'] < 0:
                        view.portfolio_table.item(row_idx, 4).setForeground(QColor('red'))

                if item['diferencia_valor'] > 0:
                    view.portfolio_table.item(row_idx, 8).setForeground(QColor('green'))
                elif item['diferencia_valor'] < 0:
                    view.portfolio_table.item(row_idx, 8).setForeground(QColor('red'))

                if item.get("precio_actual") is not None:
                    if resultado_ars_val > 0:
                        view.portfolio_table.item(row_idx, 12).setForeground(QColor('green'))
                    elif resultado_ars_val < 0:
                        view.portfolio_table.item(row_idx, 12).setForeground(QColor('red'))
                    if resultado_usd_val > 0:
                        view.portfolio_table.item(row_idx, 13).setForeground(QColor('green'))
                    elif resultado_usd_val < 0:
                        view.portfolio_table.item(row_idx, 13).setForeground(QColor('red'))

                row_idx += 1

        # Fila total
        view.portfolio_table.insertRow(row_idx)
        view.portfolio_table.setItem(row_idx, 0, QTableWidgetItem("TOTAL"))
        view.portfolio_table.setItem(row_idx, 9, QTableWidgetItem(f"${total_valor_actual:,.2f}"))
        view.portfolio_table.setItem(row_idx, 10, QTableWidgetItem(f"${total_valor_usd_tabla:,.2f}"))
        view.portfolio_table.setItem(row_idx, 13, QTableWidgetItem(f"${total_resultado_usd_tabla:,.2f}"))
        view.portfolio_table.setItem(row_idx, 14, QTableWidgetItem(f"${total_comisiones_ars:,.2f}"))
        view.portfolio_table.setItem(row_idx, 15, QTableWidgetItem(f"${total_comisiones_usd:,.2f}"))
        total_pct_item = QTableWidgetItem(f"{suma_porcentajes:.2f}%")
        if getattr(view, "is_user", False):
            total_assets_ars = getattr(view, "total_assets_ars", total_valor_actual)
            total_portfolio_ars = getattr(view, "total_portfolio_ars", total_assets_ars)
            if total_portfolio_ars > 0:
                assets_pct = (total_assets_ars / total_portfolio_ars) * 100
            else:
                assets_pct = 0
            total_pct_item.setText(f"{suma_porcentajes:.2f}% ({assets_pct:.2f}%)")
            total_pct_item.setToolTip(
                "Porcentaje de activos sobre el total del portafolio (incluye liquidez)."
            )
        view.portfolio_table.setItem(row_idx, 11, total_pct_item)
        view.portfolio_table.setItem(row_idx, 12, QTableWidgetItem(f"${total_resultado:,.2f}"))

        # Estilo fila total
        for col in range(16):
            item = view.portfolio_table.item(row_idx, col)
            if item is None:
                item = QTableWidgetItem("")
                view.portfolio_table.setItem(row_idx, col, item)
            item.setBackground(QColor(self.get_theme_value("table_total_bg", "lightgray")))
            font = item.font()
            font.setBold(True)
            item.setFont(font)

        self.apply_currency_column_visibility(view)
        self.adjust_portfolio_table(view)

        # Actualizar gráficos si es necesario
        if view.portfolio_tabs.currentIndex() == 1:  # Si está en la pestaña de gráficos
            self.draw_category_pie_chart(view)

    def draw_category_pie_chart(self, view):
        # Limpiar widget anterior
        for i in reversed(range(view.category_chart_layout.count())):
            widget = view.category_chart_layout.itemAt(i).widget()
            if widget:
                widget.setParent(None)

        if not hasattr(view, 'portfolio_data') or not hasattr(view, 'categorias'):
            return

        # Agrupar por categoría y sumar el valor actual según vista
        display_currency = "ARS"
        if getattr(view, "is_user", False):
            display_currency = getattr(view, "asset_currency_view", "ARS")
        value_key = "valor_usd" if display_currency == "USD" else "valor_ars"
        categorias_dict = {}
        for item in view.portfolio_data:
            cat = item['tipo']
            if cat not in categorias_dict:
                categorias_dict[cat] = 0.0
            categorias_dict[cat] += item.get(value_key, item.get('valor_actual', 0.0))

        # Solo consideramos categorías con valor positivo
        cats = []
        valores = []
        for cat, val in categorias_dict.items():
            if val > 0:
                cats.append(cat)
                valores.append(val)

        # Si no hay valores positivos, no dibujamos
        if not valores:
            no_data_label = QLabel("No hay datos suficientes para mostrar el gráfico")
            view.category_chart_layout.addWidget(no_data_label)
            return

        # Crear figura de matplotlib
        fig = Figure(figsize=(6, 6), dpi=100)
        ax = fig.add_subplot(111)

        # Colores para las categorías
        colors = plt.get_cmap('Accent')(np.linspace(0, 1, len(cats)))

        # Dibujar el gráfico de torta
        wedges, texts, autotexts = ax.pie(
            valores, 
            labels=cats, 
            autopct='%1.1f%%', 
            startangle=90,
            colors=colors[:len(cats)],
            textprops={'fontsize': 9}
        )

        ax.set_title(
            f'Distribución del Portafolio por Categoría ({display_currency})',
            fontsize=12,
            pad=25
        )
        ax.axis('equal')  # Para que sea circular

        # Canvas para mostrar la figura
        canvas = FigureCanvasQTAgg(fig)
        view.category_chart_layout.addWidget(canvas)

        # Guardar referencia para eventos
        view.pie_chart_elements = {
            'wedges': wedges,
            'texts': texts,
            'autotexts': autotexts,
            'categories': cats,
            'canvas': canvas
        }

        # Habilitar eventos de clic
        for wedge in wedges:
            wedge.set_picker(True)  # Permitir selección

        # Conectar evento de clic
        canvas.mpl_connect('pick_event', lambda event, v=view: self.on_pie_click(event, v))

        # Guardar referencia para eventos
        view.pie_chart = {
            'canvas': canvas,
            'wedges': wedges,
            'categories': cats
        }

    def on_pie_click(self, event, view):
        wedge = event.artist
        cat_idx = view.pie_chart['wedges'].index(wedge)
        category = view.pie_chart['categories'][cat_idx]
        self.draw_symbols_pie_chart(category, view)

    def on_category_click(self, event, view):
        if isinstance(event.artist, plt.Text):
            label_text = event.artist.get_text()
            if label_text in view.pie_chart_elements['categories']:
                idx = view.pie_chart_elements['categories'].index(label_text)
            else:
                return
        else:
            idx = view.pie_chart_elements['wedges'].index(event.artist)

        category = view.pie_chart_elements['categories'][idx]
        self.draw_symbols_pie_chart(category, view)

    def draw_symbols_pie_chart(self, category, view):
        # Limpiar widget anterior
        for i in reversed(range(view.symbol_chart_layout.count())):
            widget = view.symbol_chart_layout.itemAt(i).widget()
            if widget:
                widget.deleteLater()

        # Filtrar los símbolos de esta categoría con valor actual positivo
        symbols = []
        values = []
        for item in view.portfolio_data:
            if item['tipo'] == category and item['valor_actual'] > 0:
                symbols.append(item['simbolo_display'])
                values.append(item['valor_actual'])

        # Si no hay símbolos, mostrar mensaje
        if not values:
            label = QLabel(f"No hay activos en {category}")
            view.symbol_chart_layout.addWidget(label)
            return

        # Crear figura de matplotlib
        fig = Figure(figsize=(6, 6), dpi=100)
        ax = fig.add_subplot(111)

        # Colores para los símbolos
        colors = plt.get_cmap('Accent')(np.linspace(0, 1, len(symbols)))

        # Dibujar el gráfico de torta
        wedges, texts, autotexts = ax.pie(
            values, 
            labels=symbols, 
            autopct='%1.1f%%', 
            startangle=90,
            colors=colors[:len(symbols)],
            textprops={'fontsize': 9}
        )

        ax.set_title(f'Distribución de {category}', fontsize=12, pad=25)
        ax.axis('equal')  # Para que sea circular

        # Canvas para mostrar la figura
        canvas = FigureCanvasQTAgg(fig)
        view.symbol_chart_layout.addWidget(canvas)

    def create_journal_view(self, parent_widget):
        layout = QVBoxLayout(parent_widget)

        # Tabla
        self.journal_table = QTableWidget()
        self.journal_table.setColumnCount(21)
        self.journal_table.setHorizontalHeaderLabels([
            "Fecha", "Tipo", "Operación", "Símbolo", "Detalle", "Plazo",
            "Broker", "Cantidad", "Precio", "Dividendos", "Total", "Comisión",
            "IVA", "Derechos", "IVA Der", "Desc Total",
            "Costo", "Ingreso", "Balance", "Moneda", "TC USD/ARS"
        ])
        self.journal_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.journal_table.verticalHeader().setVisible(False)
        self.journal_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.journal_table.setSizeAdjustPolicy(QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents)

        layout.addWidget(self.journal_table)

        # Botones
        button_frame = QFrame()
        button_layout = QHBoxLayout(button_frame)

        self.delete_journal_btn = QPushButton("Eliminar")
        self.delete_journal_btn.clicked.connect(self.eliminar_operacion)
        button_layout.addWidget(self.delete_journal_btn)

        self.refresh_journal_btn = QPushButton("Actualizar")
        self.refresh_journal_btn.clicked.connect(self.load_journal)
        button_layout.addWidget(self.refresh_journal_btn)

        button_layout.addStretch()
        layout.addWidget(button_frame)

    def load_journal(self):
        self.journal_table.setRowCount(0)

        rows = fetch_journal()
        headers = [
            "id", "fecha", "tipo", "tipo_operacion", "simbolo", "detalle", "plazo",
            "cantidad", "precio", "Dividendos", "total_sin_desc", "comision",
            "iva_21", "derechos", "iva_derechos", "total_descuentos",
            "costo_total", "ingreso_total", "balance", "broker", "moneda", "tc_usd_ars"
        ]
        self.journal_table.setColumnCount(len(headers)-1)  # omit id in view
        self.journal_table.setHorizontalHeaderLabels(headers[1:])
        numeric_columns = {
            "cantidad", "precio", "rendimiento", "total_sin_desc", "comision",
            "iva_21", "derechos", "iva_derechos", "total_descuentos",
            "costo_total", "ingreso_total", "balance", "tc_usd_ars"
        }

        for row_idx, row in enumerate(rows):
            self.journal_table.insertRow(row_idx)
            for col_idx, key in enumerate(headers[1:]):
                value = row.get(key, "")
                if key in numeric_columns:
                    try:
                        num = float(value)
                        item = QTableWidgetItem(f"${num:,.2f}")
                    except:
                        item = QTableWidgetItem(str(value))
                else:
                    item = QTableWidgetItem(str(value))
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                self.journal_table.setItem(row_idx, col_idx, item)

    def eliminar_operacion(self):
        selected_items = self.journal_table.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Advertencia", "Seleccione una operación para eliminar")
            return

        if QMessageBox.question(self, "Confirmar", "¿Está seguro de eliminar la operación seleccionada?") == QMessageBox.StandardButton.Yes:
            selected_row = selected_items[0].row()
            # Obtener id desde la tabla (no visible). Suponemos que las filas están en orden y fetch_journal devuelve ids crecientes
            rows = fetch_journal()
            if selected_row < len(rows):
                row_id = rows[selected_row].get('id')
                if row_id is not None:
                    delete_journal_row_by_id(row_id)

            # Recalcular compras pendientes y portafolio
            self.load_compras_pendientes()
            self.recalcular_portfolio()
            self.load_journal()
            self.refresh_portfolios()
            self.load_finished_operations()

            # Actualizar pestaña de Análisis
            self.analysis_tab.load_portfolio()
            self.analysis_tab.load_saved_data()
            self.analysis_tab.sort_tables()

    def load_compras_pendientes(self):
        self.compras_pendientes = {}
        rows = fetch_journal()
        for row in rows:
            tipo_op = row.get("tipo_operacion", "")
            simbolo = row.get("simbolo", "")
            try:
                cantidad = float(row.get("cantidad", 0) or 0)
                precio = float(row.get("precio", 0) or 0)
            except:
                continue
            if tipo_op == "Compra":
                if simbolo not in self.compras_pendientes:
                    self.compras_pendientes[simbolo] = deque()
                self.compras_pendientes[simbolo].append((cantidad, precio))
            elif tipo_op == "Venta":
                cantidad_a_vender = cantidad
                while cantidad_a_vender > 0 and self.compras_pendientes.get(simbolo):
                    primera_compra = self.compras_pendientes[simbolo][0]
                    if primera_compra[0] <= cantidad_a_vender:
                        cantidad_a_vender -= primera_compra[0]
                        self.compras_pendientes[simbolo].popleft()
                    else:
                        self.compras_pendientes[simbolo][0] = (primera_compra[0] - cantidad_a_vender, primera_compra[1])
                        cantidad_a_vender = 0

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = PortfolioAppQt()
    window.showMaximized()
    sys.exit(app.exec())


















