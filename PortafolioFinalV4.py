# -*- coding: utf-8 -*-
import os
import threading
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import sys
import sqlite3
import winreg
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
from PyQt6.QtCore import Qt, QSize, QUrl, QTimer, QDate
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

        # Inicializar atributos para actualización automática
        self.auto_update_interval = 5  # Valor por defecto en minutos
        self.auto_update_active = False
        self.remaining_time = 0  # Tiempo restante en segundos

        self.auto_update_timer = QTimer(self)
        self.auto_update_timer.timeout.connect(self.actualizar_datos_mercado)
        self.countdown_timer = QTimer(self)  # Nuevo timer para cuenta regresiva
        self.countdown_timer.timeout.connect(self.update_countdown)

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

    def configure_user_portfolio_headers(self):
        view = self.user_portfolio_view
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
            "Resultado ARS (%)"
        ])
        header_ppc = view.portfolio_table.horizontalHeaderItem(5)
        if header_ppc:
            header_ppc.setToolTip("PRECIO PROMEDIO DE COMPRA")

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
        visible = not view.liquidity_chart_frame.isVisible()
        view.liquidity_chart_frame.setVisible(visible)
        if visible:
            self.draw_liquidity_chart(view)

    def set_user_grouping(self, grouping, view):
        if not getattr(view, "is_user", False):
            return
        view.group_by = grouping
        view.group_by_tipo_btn.setChecked(grouping == "tipo")
        view.group_by_broker_btn.setChecked(grouping == "broker")
        self.load_portfolio(view)

    def set_liquidity_currency(self, currency, view):
        if not getattr(view, "is_user", False):
            return
        view.liquidity_currency = currency
        view.liquidity_ars_btn.setChecked(currency == "ARS")
        view.liquidity_usd_btn.setChecked(currency == "USD")
        if view.liquidity_chart_frame.isVisible():
            self.draw_liquidity_chart(view)

    def draw_liquidity_chart(self, view):
        for i in reversed(range(view.liquidity_chart_layout.count())):
            widget = view.liquidity_chart_layout.itemAt(i).widget()
            if widget:
                widget.setParent(None)
        view.liquidity_list.clear()

        liquidity_by_broker = getattr(view, "liquidity_by_broker", {})
        data = liquidity_by_broker.get(view.liquidity_currency, {})
        labels = []
        values = []
        for broker, amount in data.items():
            if abs(amount) > 0.0001:
                labels.append(broker)
                values.append(abs(amount))

        if not values:
            no_data_label = QLabel("No hay liquidez para mostrar")
            view.liquidity_chart_layout.addWidget(no_data_label)
            return

        fig = Figure(figsize=(5, 4), dpi=100)
        ax = fig.add_subplot(111)
        colors = [self.get_broker_color(broker) for broker in labels]

        def pct_label(pct):
            return f"{pct:.1f}%" if pct >= 6 else ""

        wedges, _texts, _autotexts = ax.pie(
            values,
            labels=None,
            autopct=pct_label,
            startangle=90,
            colors=colors[:len(labels)],
            textprops={'fontsize': 9}
        )
        ax.set_title(f'Distribución de liquidez en {view.liquidity_currency}', fontsize=12, pad=20)
        fig.subplots_adjust(top=0.82)
        ax.axis('equal')

        canvas = FigureCanvasQTAgg(fig)
        view.liquidity_chart_layout.addWidget(canvas)

        total = sum(values) if values else 1
        currency_prefix = "USD" if view.liquidity_currency == "USD" else "ARS"
        for broker, amount in sorted(data.items(), key=lambda x: x[0]):
            if abs(amount) <= 0.0001:
                continue
            pct = (abs(amount) / total) * 100 if total else 0
            value_str = f"{currency_prefix} ${abs(amount):,.2f}"
            item = QListWidgetItem(f"{broker}: {value_str} ({pct:.2f}%)")
            pixmap = QPixmap(10, 10)
            pixmap.fill(QColor(self.get_broker_color(broker)))
            item.setIcon(QIcon(pixmap))
            view.liquidity_list.addItem(item)

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
            f"LIQUIDEZ TOTAL: ARS ${total_liq_ars:,.2f} | USD ${total_liq_usd:,.2f}"
        )
        view.liquidity_total_label.setText(
            f"El total de su capital líquido es ${total_liq_ars_equiv:,.2f} ({liquidity_pct:.2f}%)"
        )
        view.total_assets_ars = total_assets_ars
        view.total_liq_ars_equiv = total_liq_ars_equiv
        view.total_portfolio_ars = total_portfolio_ars

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
            "Fecha", "Categoria", "Simbolo", "Cantidad Nominal", 
            "Precio de Compra", "Precio de Venta", 
            "Diferencia de Valor", "Descuentos", "Rendimiento", "Resultado"
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
                    elif i == 8:  # Rendimiento
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
        self.broker_combo = QComboBox()
        self.broker_combo.addItems(BROKERS)
        self.simbolo_combo = QComboBox()
        self.simbolo_combo.setEditable(True)  # Permitir edición para nuevos símbolos
        self.detalle_edit = QLineEdit()
        self.cantidad_edit = QLineEdit("0")
        self.precio_edit = QLineEdit("0")
        self.rendimiento_edit = QLineEdit("0")
        self.tc_edit = QLineEdit("1.0")

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
        layout.addWidget(QLabel("Broker*:"), 3, 0)
        layout.addWidget(self.broker_combo, 3, 1)
        layout.addWidget(QLabel("Símbolo:"), 4, 0)
        layout.addWidget(self.simbolo_combo, 4, 1)
        layout.addWidget(QLabel("Detalle:"), 5, 0)
        layout.addWidget(self.detalle_edit, 5, 1)
        layout.addWidget(QLabel("Cantidad*:"), 6, 0)
        layout.addWidget(self.cantidad_edit, 6, 1)
        layout.addWidget(QLabel("Precio*:"), 7, 0)
        layout.addWidget(self.precio_edit, 7, 1)
        layout.addWidget(QLabel("Rendimiento:"), 8, 0)
        layout.addWidget(self.rendimiento_edit, 8, 1)
        layout.addWidget(QLabel("TC USD/ARS:"), 9, 0)
        layout.addWidget(self.tc_edit, 9, 1)

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
        layout.addLayout(button_layout, 10, 0, 1, 4)

        layout.addWidget(QLabel("* Campos obligatorios"), 11, 0, 1, 4)

        # Conectar señales
        self.tipo_combo.currentTextChanged.connect(self.on_tipo_change)
        self.tipo_op_combo.currentTextChanged.connect(self.on_tipo_op_change)
        self.broker_combo.currentTextChanged.connect(self.on_broker_change)
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
            self.tc_edit.setText(str(getattr(self, "default_fx_rate", 1.0)))
        elif tipo == "Plazo Fijo":
            self.tipo_op_combo.addItems(["Compra", "Venta"])
            self.precio_edit.setText("1")
            self.precio_edit.setEnabled(False)
            self.rendimiento_edit.setEnabled(True)
            self.tc_edit.setText("1.0")
            self.tc_edit.setEnabled(False)
        elif tipo in ["Acciones AR", "CEDEARs", "Bonos AR","ETFs" , "Criptomonedas", "FCIs AR", "Cauciones"]:
            self.tipo_op_combo.addItems(["Compra", "Venta", "Rendimiento"])
            self.precio_edit.setEnabled(True)
            self.rendimiento_edit.setEnabled(True)
            moneda = self.get_moneda_actual(tipo)
            if moneda == "USD":
                self.tc_edit.setEnabled(True)
                self.tc_edit.setText(str(getattr(self, "default_fx_rate", 1.0)))
            else:
                self.tc_edit.setEnabled(False)
                self.tc_edit.setText("1.0")
        else:
            self.tipo_op_combo.addItems(["Compra", "Venta"])
            self.precio_edit.setEnabled(True)
            self.rendimiento_edit.setEnabled(True)
            self.tc_edit.setEnabled(False)
            self.tc_edit.setText("1.0")

        self.update_simbolo_combobox()
        self.calcular_on_change()

    def on_broker_change(self, _):
        """Actualizar símbolos disponibles y re-cálculos cuando cambia el broker."""
        self.update_simbolo_combobox()
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

            if tipo in ["Deposito ARS", "Deposito USD", "Dep?sito ARS", "Dep?sito USD"]:
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

            op = calcular_operacion(tipo, tipo_op, cantidad, precio, rendimiento, broker)

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
                    QMessageBox.critical(self, "Error", "Para venta de Plazo Fijo debe ingresar el Rendimiento")
                    return

            if cantidad < 0 or precio < 0:
                QMessageBox.critical(self, "Error", "Cantidad y Precio deben ser valores positivos")
                return

            fecha = self.fecha_edit.date().toString("yyyy-MM-dd")
            simbolo = self.simbolo_combo.currentText()
            detalle = self.detalle_edit.text()
            broker = self.broker_combo.currentText() or "GENERAL"

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
            total_sin_desc = float(self.total_sin_desc_label.text().replace('.', '').replace(',', '.')) if self.total_sin_desc_label.text() else 0
            comision = float(self.comision_edit.text().replace('.', '').replace(',', '.')) if self.comision_edit.text() else 0
            iva_basico = float(self.iva_label.text().replace('.', '').replace(',', '.')) if self.iva_label.text() else 0
            derechos = float(self.derechos_edit.text().replace('.', '').replace(',', '.')) if self.derechos_edit.text() else 0
            iva_derechos = float(self.iva_derechos_label.text().replace('.', '').replace(',', '.')) if self.iva_derechos_label.text() else 0
            total_descuentos = float(self.total_descuentos_label.text().replace('.', '').replace(',', '.')) if self.total_descuentos_label.text() else 0
            costo_total = float(self.costo_total_label.text().replace('.', '').replace(',', '.')) if self.costo_total_label.text() else 0
            ingreso_total = float(self.ingreso_total_label.text().replace('.', '').replace(',', '.')) if self.ingreso_total_label.text() else 0
            balance = float(self.balance_label.text().replace('.', '').replace(',', '.')) if self.balance_label.text() else 0

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
            view.liquidity_frame = QFrame()
            liquidity_layout = QVBoxLayout(view.liquidity_frame)
            liquidity_layout.setContentsMargins(0, 0, 0, 0)

            liquidity_header = QHBoxLayout()
            view.liquidity_button = QPushButton("LIQUIDEZ TOTAL: ARS $0.00 | USD $0.00")
            view.liquidity_button.setCursor(Qt.CursorShape.PointingHandCursor)
            view.liquidity_button.clicked.connect(
                lambda _checked=False, v=view: self.toggle_liquidity_chart(v)
            )
            liquidity_header.addWidget(view.liquidity_button)

            view.liquidity_currency = "ARS"
            view.liquidity_ars_btn = QPushButton("ARS")
            view.liquidity_ars_btn.setCheckable(True)
            view.liquidity_ars_btn.setChecked(True)
            view.liquidity_ars_btn.clicked.connect(
                lambda _checked=False, v=view: self.set_liquidity_currency("ARS", v)
            )
            view.liquidity_usd_btn = QPushButton("USD")
            view.liquidity_usd_btn.setCheckable(True)
            view.liquidity_usd_btn.clicked.connect(
                lambda _checked=False, v=view: self.set_liquidity_currency("USD", v)
            )
            liquidity_header.addWidget(view.liquidity_ars_btn)
            liquidity_header.addWidget(view.liquidity_usd_btn)
            liquidity_header.addStretch()
            liquidity_layout.addLayout(liquidity_header)

            view.liquidity_chart_frame = QFrame()
            chart_frame_layout = QVBoxLayout(view.liquidity_chart_frame)
            chart_list_layout = QHBoxLayout()

            view.liquidity_chart_container = QFrame()
            view.liquidity_chart_layout = QVBoxLayout(view.liquidity_chart_container)
            chart_list_layout.addWidget(view.liquidity_chart_container, 2)

            view.liquidity_list = QListWidget()
            view.liquidity_list.setMinimumWidth(220)
            chart_list_layout.addWidget(view.liquidity_list, 1)

            chart_frame_layout.addLayout(chart_list_layout)
            view.liquidity_chart_frame.setVisible(False)
            view.liquidity_total_label = QLabel(
                "El total de su capital líquido es $0.00 (0.00%)"
            )
            chart_frame_layout.addWidget(view.liquidity_total_label)
            liquidity_layout.addWidget(view.liquidity_chart_frame)

            layout.addWidget(view.liquidity_frame)

        # Crear pestañas internas
        view.portfolio_tabs = QTabWidget()
        layout.addWidget(view.portfolio_tabs)

        # Tabla
        view.table_tab = QWidget()
        view.table_layout = QVBoxLayout(view.table_tab)

        view.portfolio_table = QTableWidget()
        view.portfolio_table.setColumnCount(13)
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
            "Resultado ARS"
        ])
        view.portfolio_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
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

    def detect_fx_rate(self):
        """Intenta obtener un tipo de cambio MEP desde los datos de mercado usando AL30/AL30D"""
        fx_rate = 1.0
        if self.df_mercado is not None:
            symbol_col = next((c for c in ["Símbolo.1", "Símbolo", "Simbolo", "Symbol", "Ticker"] if c in self.df_mercado.columns), None)
            price_col = next((c for c in ["Último Operado", "Ultimo Operado", "Precio", "Close"] if c in self.df_mercado.columns), None)
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
        try:
            for row in fetch_journal():
                simbolo = row.get("simbolo", "")
                tipo_op = row.get("tipo_operacion", "")
                tipo = row.get("tipo", "")
                total_desc = row.get("total_descuentos", 0)
                rendimiento_val = row.get("rendimiento", 0)

                # Recolectar descuentos
                if tipo_op in ["Compra", "Rendimiento"]:
                    try:
                        total_desc = float(str(total_desc).replace(',', '.'))
                    except Exception:
                        total_desc = 0
                    if simbolo not in descuentos_por_simbolo:
                        descuentos_por_simbolo[simbolo] = 0
                    descuentos_por_simbolo[simbolo] += total_desc

                # Recolectar rendimientos
                if tipo_op == "Rendimiento":
                    try:
                        rendimiento = float(str(rendimiento_val).replace(',', '.'))
                    except Exception:
                        rendimiento = 0
                    if simbolo not in rendimientos_por_simbolo:
                        rendimientos_por_simbolo[simbolo] = 0
                    rendimientos_por_simbolo[simbolo] += rendimiento

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
                if self.df_mercado is not None:
                    simbolo = item['simbolo']
                    if item['tipo'] == "Plazo Fijo" and "(" in simbolo:
                        simbolo = simbolo.split("(")[0].strip()

                    # Buscar columnas disponibles en el CSV de mercado
                    symbol_col = next((c for c in ["Símbolo.1", "Símbolo", "Simbolo", "Symbol", "Ticker"] if c in self.df_mercado.columns), None)
                    price_col = next((c for c in ["Último Operado", "Ultimo Operado", "Precio", "Close"] if c in self.df_mercado.columns), None)
                    var_col = next((c for c in ["Variación Diaria", "Variacion Diaria", "Var.%", "Variacion", "Change %"] if c in self.df_mercado.columns), None)

                    if symbol_col and price_col:
                        match = self.df_mercado[self.df_mercado[symbol_col] == simbolo]
                    else:
                        match = pd.DataFrame()

                    if not match.empty:
                        try:
                            ultimo_operado = match.iloc[0].get(price_col)
                            if isinstance(ultimo_operado, str):
                                ultimo_operado = ultimo_operado.replace('.', '').replace(',', '.')
                            if item['tipo'] == "Bonos AR":
                                item['precio_actual'] = float(ultimo_operado) * 0.01
                            else:
                                item['precio_actual'] = float(ultimo_operado)
                        except Exception:
                            item['precio_actual'] = None

                        if var_col:
                            try:
                                variacion = match.iloc[0].get(var_col)
                                if isinstance(variacion, str):
                                    variacion = variacion.replace('%', '').replace(',', '.').strip()
                                    if variacion.endswith('%'):
                                        variacion = variacion[:-1]
                                item['variacion_diaria'] = float(variacion) if variacion not in (None, "") else None
                            except Exception:
                                item['variacion_diaria'] = None

                if item['precio_actual'] is not None:
                    item['valor_actual'] = item['cantidad'] * item['precio_actual']
                else:
                    item['valor_actual'] = item['valor_compra']

                item['diferencia_valor'] = item['valor_actual'] - item['valor_compra']

            # Calcular descuentos totales
            descuento_total = descuentos_por_simbolo.get(item['simbolo'], 0)

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
            # Valores convertidos
            if item['tipo'] == "Efectivo":
                valor_ars = item.get('monto_ars', 0.0)
                valor_usd = item.get('monto_usd', 0.0)
            else:
                if moneda_item == "USD":
                    valor_ars = item['valor_actual'] * fx_rate
                    valor_usd = item['valor_actual']
                else:
                    valor_ars = item['valor_actual']
                    valor_usd = item['valor_actual'] / fx_rate if fx_rate != 0 else item['valor_actual']
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
            total_valor = total_valor_ars
        else:
            total_valor = total_valor_ars

        if getattr(view, "is_user", False):
            self.update_liquidity_section(view, total_assets_ars, fx_rate)

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
        suma_porcentajes = 0.0

        # Recorrer categorías en el orden definido
        for cat in sorted(categorias.keys(), key=lambda x: orden_categorias.get(x, 10)):
            items = categorias[cat]
            cat_total_ars = sum(item['valor_ars'] for item in items)
            cat_total_compra = sum(item.get('valor_compra', 0) for item in items)
            cat_total_resultado = sum(item.get('resultado', 0) for item in items)

            # Fila de categoría
            view.portfolio_table.insertRow(row_idx)
            view.portfolio_table.setItem(row_idx, 0, QTableWidgetItem(cat))
            view.portfolio_table.setItem(row_idx, 9, QTableWidgetItem(f"${cat_total_ars:,.2f}"))

            if total_valor > 0:
                cat_porcentaje = (cat_total_ars / total_valor) * 100
                view.portfolio_table.setItem(row_idx, 11, QTableWidgetItem(f"{cat_porcentaje:.2f}%"))
            if getattr(view, "is_user", False):
                if cat_total_compra > 0:
                    cat_rend_pct = (cat_total_resultado / cat_total_compra) * 100
                else:
                    cat_rend_pct = 0
                cat_resultado_str = f"${cat_total_resultado:,.2f} ({cat_rend_pct:.2f}%)"
                view.portfolio_table.setItem(row_idx, 12, QTableWidgetItem(cat_resultado_str))

            # Estilo fila categoría
            for col in range(13):
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

                valor_actual = item['valor_actual']
                valor_ars = item.get('valor_ars', valor_actual)
                valor_usd = item.get('valor_usd', valor_actual)
                total_valor_actual += valor_ars
                total_valor_usd_tabla += valor_usd
                total_resultado += item['resultado']

                # Formatear valores para mostrar
                precio_operacion_compra = f"${item['precio_operacion_compra']:,.2f}" if item['precio_operacion_compra'] != 0 else "N/A"
                valor_operacion_compra = f"${item['valor_compra']:,.2f}" if item['precio_operacion_compra'] != 0 else "N/A"
                precio_actual = f"${item['precio_actual']:,.2f}" if item['precio_actual'] is not None else "N/D"
                valor_actual_str = f"${valor_actual:,.2f}"
                variacion_str = f"{item['variacion_diaria']:.2f}%" if item['variacion_diaria'] is not None else "N/D"
                diferencia_valor_str = f"${item['diferencia_valor']:,.2f}"
                resultado_str = f"${item['resultado']:,.2f}"
                if getattr(view, "is_user", False) and item['precio_operacion_compra'] != 0 and item['precio_actual'] is not None:
                    rendimiento_pct = ((item['precio_actual'] - item['precio_operacion_compra']) / item['precio_operacion_compra']) * 100
                    rendimiento_str = f"{rendimiento_pct:.2f}%".replace(".", ",")
                    resultado_str = f"{resultado_str} ({rendimiento_str})"

                # Calcular % del portafolio usando el total_valor (suma de todos los valores actuales)
                if total_valor > 0:
                    porcentaje_activo = (valor_ars / total_valor) * 100
                    porcentaje_str = f"{porcentaje_activo:.2f}%"
                    suma_porcentajes += porcentaje_activo
                else:
                    porcentaje_str = "0.00%"

                # Llenar la fila
                view.portfolio_table.setItem(row_idx, 0, QTableWidgetItem(""))
                view.portfolio_table.setItem(row_idx, 1, QTableWidgetItem(item['simbolo_display']))
                view.portfolio_table.setItem(row_idx, 2, QTableWidgetItem(item.get('moneda', 'ARS')))
                view.portfolio_table.setItem(row_idx, 3, QTableWidgetItem(
                    f"{item['cantidad']:,.2f}" if item['tipo'] not in ["Efectivo", "Plazo Fijo"] else ""))
                view.portfolio_table.setItem(row_idx, 4, QTableWidgetItem(variacion_str))
                view.portfolio_table.setItem(row_idx, 5, QTableWidgetItem(precio_operacion_compra))
                view.portfolio_table.setItem(row_idx, 6, QTableWidgetItem(valor_operacion_compra))
                view.portfolio_table.setItem(row_idx, 7, QTableWidgetItem(precio_actual))
                view.portfolio_table.setItem(row_idx, 8, QTableWidgetItem(valor_actual_str))
                view.portfolio_table.setItem(row_idx, 9, QTableWidgetItem(f"${valor_ars:,.2f}"))
                view.portfolio_table.setItem(row_idx, 10, QTableWidgetItem(f"${valor_usd:,.2f}"))
                view.portfolio_table.setItem(row_idx, 11, QTableWidgetItem(porcentaje_str))
                view.portfolio_table.setItem(row_idx, 12, QTableWidgetItem(resultado_str))

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

                if item['resultado'] > 0:
                    view.portfolio_table.item(row_idx, 12).setForeground(QColor('green'))
                elif item['resultado'] < 0:
                    view.portfolio_table.item(row_idx, 12).setForeground(QColor('red'))

                row_idx += 1

        # Fila total
        view.portfolio_table.insertRow(row_idx)
        view.portfolio_table.setItem(row_idx, 0, QTableWidgetItem("TOTAL"))
        view.portfolio_table.setItem(row_idx, 9, QTableWidgetItem(f"${total_valor_actual:,.2f}"))
        view.portfolio_table.setItem(row_idx, 10, QTableWidgetItem(f"${total_valor_usd_tabla:,.2f}"))
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
        for col in range(13):
            item = view.portfolio_table.item(row_idx, col)
            if item is None:
                item = QTableWidgetItem("")
                view.portfolio_table.setItem(row_idx, col, item)
            item.setBackground(QColor(self.get_theme_value("table_total_bg", "lightgray")))
            font = item.font()
            font.setBold(True)
            item.setFont(font)

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

        # Agrupar por categoría y sumar el valor actual
        categorias_dict = {}
        for item in view.portfolio_data:
            cat = item['tipo']
            if cat not in categorias_dict:
                categorias_dict[cat] = 0.0
            categorias_dict[cat] += item['valor_actual']

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

        ax.set_title('Distribución del Portafolio por Categoría', fontsize=12, pad=25)
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
        self.journal_table.setColumnCount(19)
        self.journal_table.setHorizontalHeaderLabels([
            "Fecha", "Tipo", "Operación", "Símbolo", "Detalle",
            "Broker", "Cantidad", "Precio", "Rendimiento", "Total", "Comisión",
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
            "id", "fecha", "tipo", "tipo_operacion", "simbolo", "detalle",
            "cantidad", "precio", "rendimiento", "total_sin_desc", "comision",
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

        if QMessageBox.question(self, "Confirmar", "óEstó seguro de eliminar la operación seleccionada?") == QMessageBox.StandardButton.Yes:
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






















