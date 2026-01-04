from PyQt6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QTabWidget,
    QLabel,
    QLineEdit,
    QPushButton,
    QComboBox,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QAbstractItemView,
    QMessageBox,
    QSplitter,
    QStyledItemDelegate,
)
from datetime import datetime
from PyQt6.QtCore import Qt, QUrl
from PyQt6.QtGui import QColor, QBrush
from PyQt6.QtWebEngineWidgets import QWebEngineView
from PyQt6.QtWebEngineCore import QWebEngineSettings

from db_utils import fetch_analysis, save_analysis, fetch_journal


class ColorDelegate(QStyledItemDelegate):
    def initStyleOption(self, option, index):
        super().initStyleOption(option, index)
        text = index.data(Qt.ItemDataRole.DisplayRole)
        if text == "Muy Alta":
            option.backgroundBrush = QBrush(QColor(255, 0, 0))  # Rojo
        elif text == "Alta":
            option.backgroundBrush = QBrush(QColor(255, 165, 0))  # Naranja
        elif text == "Indeciso":
            option.backgroundBrush = QBrush(QColor(255, 255, 0))  # Amarillo
        elif text == "Baja":
            option.backgroundBrush = QBrush(QColor(173, 255, 47))  # Verde claro
        elif text == "Muy Baja":
            option.backgroundBrush = QBrush(QColor(0, 128, 0))  # Verde oscuro


class AnalysisTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.parent_app = parent
        self.setObjectName("AnalysisTab")
        self.current_sort_criteria = "Simbolo"  # Criterio de ordenamiento por defecto

        layout = QVBoxLayout(self)

        sort_control_layout = QHBoxLayout()
        sort_label = QLabel("Ordenar por:")
        sort_control_layout.addWidget(sort_label)

        self.sort_combo = QComboBox()
        self.sort_combo.addItems(["Simbolo", "Revision"])
        self.sort_combo.currentIndexChanged.connect(self.sort_tables)
        sort_control_layout.addWidget(self.sort_combo)

        sort_control_layout.addStretch()
        layout.addLayout(sort_control_layout)

        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        self.tab_bonos = QWidget()
        self.tab_acciones = QWidget()
        self.tab_cedears = QWidget()
        self.tab_etfs = QWidget()
        self.tab_cripto = QWidget()
        self.tab_fci = QWidget()

        self.tabs.addTab(self.tab_bonos, "Bonos AR")
        self.tabs.addTab(self.tab_acciones, "Acciones AR")
        self.tabs.addTab(self.tab_cedears, "CEDEARs")
        self.tabs.addTab(self.tab_etfs, "ETFs AR")
        self.tabs.addTab(self.tab_cripto, "Criptomonedas")
        self.tabs.addTab(self.tab_fci, "FCIs AR")

        self.init_tab(self.tab_bonos, "BONOS")
        self.init_tab(self.tab_acciones, "ACCIONES")
        self.init_tab(self.tab_cedears, "CEDEARS")
        self.init_tab(self.tab_etfs, "ETFS")
        self.init_tab(self.tab_cripto, "CRIPTO")
        self.init_tab(self.tab_fci, "FCI")

        self.load_portfolio()
        self.load_saved_data()

    def detect_fx_rate(self):
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

    def load_portfolio(self):
        self.portfolio_symbols = set()
        try:
            journal_rows = fetch_journal()
            for row in journal_rows:
                sym = str(row.get("simbolo", "")).upper()
                if sym:
                    self.portfolio_symbols.add(sym)
        except Exception as e:
            print(f"Error cargando portfolio: {e}")

    def load_saved_data(self):
        try:
            rows = fetch_analysis()
            for tipo in ["bonos", "acciones", "cedears", "etfs", "cripto", "fci"]:
                table = getattr(self, f"table_{tipo}")
                tipo_rows = [r for r in rows if r.get('tipo') == tipo.upper()]

                table.setRowCount(0)

                for _, row in enumerate(tipo_rows):
                    symbol = row.get('simbolo', "") or ""
                    desc = row.get('descripcion', "") or ""
                    revision = row.get('revision', "") or ""
                    ultima_revision = row.get('ultima_revision', "") or ""
                    comentario = row.get('comentario', "") or ""

                    row_position = table.rowCount()
                    table.insertRow(row_position)

                    symbol_item = QTableWidgetItem(symbol)
                    desc_item = QTableWidgetItem(desc)

                    revision_combo = QComboBox()
                    revision_combo.addItems(["Muy Alta", "Alta", "Indeciso", "Baja", "Muy Baja"])
                    delegate = ColorDelegate()
                    revision_combo.setItemDelegate(delegate)

                    index = revision_combo.findText(revision, Qt.MatchFlag.MatchFixedString)
                    if index >= 0:
                        revision_combo.setCurrentIndex(index)

                    self.update_combo_color(revision_combo)

                    revision_combo.currentIndexChanged.connect(
                        lambda _, r=row_position, t=tipo: self.on_revision_changed(r, t)
                    )

                    ultima_item = QTableWidgetItem(ultima_revision)
                    ultima_item.setFlags(Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)

                    comentario_item = QTableWidgetItem(comentario)
                    comentario_item.setFlags(Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsEditable | Qt.ItemFlag.ItemIsSelectable)

                    chart_item = QTableWidgetItem("Ver Gráfico")
                    chart_item.setForeground(QColor("blue"))
                    font = chart_item.font()
                    font.setUnderline(True)
                    chart_item.setFont(font)
                    chart_item.setFlags(Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)

                    if symbol in self.portfolio_symbols:
                        symbol_item.setBackground(QColor("#FFA500"))
                        desc_item.setBackground(QColor("#FFA500"))
                        ultima_item.setBackground(QColor("#FFA500"))
                        comentario_item.setBackground(QColor("#FFA500"))
                        chart_item.setBackground(QColor("#FFA500"))

                    table.setItem(row_position, 0, symbol_item)
                    table.setItem(row_position, 1, desc_item)
                    table.setCellWidget(row_position, 2, revision_combo)
                    table.setItem(row_position, 3, ultima_item)
                    table.setItem(row_position, 4, comentario_item)
                    table.setItem(row_position, 5, chart_item)

                self.sort_table_data(table)
        except Exception as e:
            print(f"Error cargando datos: {e}")

    def update_combo_color(self, combo):
        text = combo.currentText()
        if text == "Muy Alta":
            color = QColor(255, 0, 0)
        elif text == "Alta":
            color = QColor(255, 165, 0)
        elif text == "Indeciso":
            color = QColor(255, 255, 0)
        elif text == "Baja":
            color = QColor(173, 255, 47)
        elif text == "Muy Baja":
            color = QColor(0, 128, 0)
        else:
            color = QColor(255, 255, 255)

        combo.setStyleSheet(f"background-color: {color.name()};")

    def save_data(self):
        try:
            data = []
            tipos = ["bonos", "acciones", "cedears", "etfs", "cripto", "fci"]

            for tipo in tipos:
                table = getattr(self, f"table_{tipo}")

                for row in range(table.rowCount()):
                    symbol_item = table.item(row, 0)
                    desc_item = table.item(row, 1)
                    ultima_revision_item = table.item(row, 3)
                    comentario_item = table.item(row, 4)

                    symbol = symbol_item.text() if symbol_item is not None else ""
                    desc = desc_item.text() if desc_item is not None else ""
                    ultima_revision = ultima_revision_item.text() if ultima_revision_item is not None else ""
                    comentario = comentario_item.text() if comentario_item is not None else ""

                    revision_combo = table.cellWidget(row, 2)
                    revision = revision_combo.currentText() if revision_combo is not None else ""

                    data.append({
                        'tipo': tipo.upper(),
                        'simbolo': symbol,
                        'descripcion': desc,
                        'revision': revision,
                        'ultima_revision': ultima_revision,
                        'comentario': comentario
                    })

            save_analysis(data)

        except Exception as e:
            print(f"Error guardando datos: {e}")

    def init_tab(self, tab, tipo):
        tipo_lower = tipo.lower()
        main_layout = QVBoxLayout(tab)
        splitter = QSplitter(Qt.Orientation.Horizontal)
        main_layout.addWidget(splitter)

        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)

        form_layout = QHBoxLayout()

        symbol_label = QLabel("Símbolo:")
        symbol_input = QLineEdit()
        symbol_input.setPlaceholderText("Símbolo")

        desc_label = QLabel("Descripción:")
        desc_input = QLineEdit()
        desc_input.setPlaceholderText("Descripción")

        add_button = QPushButton("Agregar")
        add_button.clicked.connect(lambda: self.add_item(tipo_lower, symbol_input.text().strip().upper(), desc_input.text().strip()))

        update_button = QPushButton("Actualizar")
        update_button.clicked.connect(self.update_all)

        delete_button = QPushButton("Eliminar seleccionado")
        delete_button.clicked.connect(lambda: self.delete_selected(tipo_lower))

        form_layout.addWidget(symbol_label)
        form_layout.addWidget(symbol_input)
        form_layout.addWidget(desc_label)
        form_layout.addWidget(desc_input)
        form_layout.addWidget(add_button)
        form_layout.addWidget(update_button)
        form_layout.addWidget(delete_button)

        left_layout.addLayout(form_layout)

        table = QTableWidget(0, 6)
        table.setHorizontalHeaderLabels(["Símbolo", "Descripción", "Revisión", "Última Revisión", "Comentario", "Gráfico"])
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        table.setEditTriggers(QAbstractItemView.EditTrigger.DoubleClicked | QAbstractItemView.EditTrigger.EditKeyPressed)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        table.cellClicked.connect(lambda row, col, tab=tab, table=table: self.on_cell_clicked(row, col, tab, table))
        left_layout.addWidget(table)

        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)

        web_view = QWebEngineView()
        settings = web_view.settings()
        settings.setAttribute(QWebEngineSettings.WebAttribute.JavascriptEnabled, True)
        settings.setAttribute(QWebEngineSettings.WebAttribute.WebGLEnabled, True)
        settings.setAttribute(QWebEngineSettings.WebAttribute.PluginsEnabled, True)
        settings.setAttribute(QWebEngineSettings.WebAttribute.JavascriptCanOpenWindows, True)
        settings.setAttribute(QWebEngineSettings.WebAttribute.LocalContentCanAccessRemoteUrls, True)
        settings.setAttribute(QWebEngineSettings.WebAttribute.AllowRunningInsecureContent, True)
        web_view.page().profile().setHttpUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36")

        exit_button = QPushButton("Salir de Gráfico")
        exit_button.clicked.connect(lambda: self.hide_chart(tipo_lower, right_widget, exit_button))
        exit_button.setVisible(False)

        right_layout.addWidget(web_view)
        right_layout.addWidget(exit_button)

        splitter.addWidget(left_widget)
        splitter.addWidget(right_widget)
        right_widget.setVisible(False)

        setattr(self, f"table_{tipo_lower}", table)
        setattr(self, f"web_view_{tipo_lower}", web_view)
        setattr(self, f"right_widget_{tipo_lower}", right_widget)
        setattr(self, f"exit_button_{tipo_lower}", exit_button)
        setattr(self, f"symbol_input_{tipo_lower}", symbol_input)
        setattr(self, f"desc_input_{tipo_lower}", desc_input)
        setattr(self, f"splitter_{tipo_lower}", splitter)

    def add_item(self, tipo, symbol, desc):
        if not symbol:
            return

        table = getattr(self, f"table_{tipo}")

        for row in range(table.rowCount()):
            symbol_item = table.item(row, 0)
            if symbol_item and symbol_item.text() == symbol:
                QMessageBox.warning(self, "Símbolo duplicado", f"El símbolo {symbol} ya está en la tabla.")
                return

        row_position = table.rowCount()
        table.insertRow(row_position)

        symbol_item = QTableWidgetItem(symbol)
        desc_item = QTableWidgetItem(desc)

        revision_combo = QComboBox()
        revision_combo.addItems(["Muy Alta", "Alta", "Indeciso", "Baja", "Muy Baja"])
        delegate = ColorDelegate()
        revision_combo.setItemDelegate(delegate)
        revision_combo.currentIndexChanged.connect(lambda _, row=row_position, tipo=tipo: self.on_revision_changed(row, tipo))
        self.update_combo_color(revision_combo)

        ultima_item = QTableWidgetItem("")
        ultima_item.setFlags(Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)

        comentario_item = QTableWidgetItem("")
        comentario_item.setFlags(Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsEditable | Qt.ItemFlag.ItemIsSelectable)

        chart_item = QTableWidgetItem("Ver Gráfico")
        chart_item.setForeground(QColor("blue"))
        font = chart_item.font()
        font.setUnderline(True)
        chart_item.setFont(font)
        chart_item.setFlags(Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)

        table.setItem(row_position, 0, symbol_item)
        table.setItem(row_position, 1, desc_item)
        table.setCellWidget(row_position, 2, revision_combo)
        table.setItem(row_position, 3, ultima_item)
        table.setItem(row_position, 4, comentario_item)
        table.setItem(row_position, 5, chart_item)

        self.sort_tables()

    def delete_selected(self, tipo):
        table = getattr(self, f"table_{tipo}")
        selected_rows = sorted({index.row() for index in table.selectedIndexes()}, reverse=True)
        for row in selected_rows:
            table.removeRow(row)
        self.save_data()

    def on_cell_clicked(self, row, column, tab, table):
        if column == 5:
            symbol_item = table.item(row, 0)
            if symbol_item:
                symbol = symbol_item.text()
                self.show_chart(tab.objectName().lower() if tab.objectName() else tab.windowTitle().lower(), symbol)

    def show_chart(self, tipo, symbol):
        web_view = getattr(self, f"web_view_{tipo}")
        right_widget = getattr(self, f"right_widget_{tipo}")
        exit_button = getattr(self, f"exit_button_{tipo}")
        splitter = getattr(self, f"splitter_{tipo}")

        if tipo == "bonos":
            url = f"https://www.tradingview.com/chart/?symbol=BCBA%3A{symbol}"
        elif tipo == "acciones":
            url = f"https://www.tradingview.com/chart/?symbol=BCBA%3A{symbol}"
        elif tipo == "cedears":
            url = f"https://www.tradingview.com/chart/?symbol=BCBA%3A{symbol}"
        elif tipo == "etfs":
            url = f"https://www.tradingview.com/chart/?symbol=BCBA%3A{symbol}"
        elif tipo == "cripto":
            url = f"https://es.tradingview.com/chart/?symbol=BINANCE%3A{symbol}ARS"
        elif tipo == "fci":
            url = f"https://www.tradingview.com/chart/?symbol=BCBA%3A{symbol}"
        else:
            url = "https://www.tradingview.com/chart/"

        web_view.setUrl(QUrl(url))
        right_widget.setVisible(True)
        exit_button.setVisible(True)

        total_width = splitter.width()
        left_width = int(total_width * 0.3)
        right_width = total_width - left_width
        splitter.setSizes([left_width, right_width])

    def hide_chart(self, tipo, right_widget, exit_button):
        right_widget.setVisible(False)
        exit_button.setVisible(False)
        splitter = getattr(self, f"splitter_{tipo}")
        splitter.setSizes([1, 0])

    def sort_tables(self):
        self.current_sort_criteria = self.sort_combo.currentText()
        for tipo in ["bonos", "acciones", "cedears", "etfs", "cripto", "fci"]:
            table = getattr(self, f"table_{tipo}")
            self.sort_table_data(table)

    def sort_tables_by_symbol(self):
        self.current_sort_criteria = "Simbolo"
        self.sort_tables()

    def sort_tables_by_revision(self):
        self.current_sort_criteria = "Revision"
        self.sort_tables()

    def sort_table_data(self, table):
        if self.current_sort_criteria == "Simbolo":
            table.sortItems(0)
        elif self.current_sort_criteria == "Revision":
            table.sortItems(2)

    def update_all(self):
        for tipo in ["bonos", "acciones", "cedears", "etfs", "cripto", "fci"]:
            table = getattr(self, f"table_{tipo}")
            table.viewport().update()
        self.save_data()

    def on_revision_changed(self, row, tipo):
        table = getattr(self, f"table_{tipo}")
        combo = table.cellWidget(row, 2)
        if combo:
            self.update_combo_color(combo)
            if table.item(row, 3) is None:
                now = datetime.now().strftime("%Y-%m-%d")
                ultima_item = QTableWidgetItem(now)
                ultima_item.setFlags(Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)
                table.setItem(row, 3, ultima_item)
            else:
                table.item(row, 3).setText(datetime.now().strftime("%Y-%m-%d"))
            self.save_data()
            symbol_item = table.item(row, 0)
            if symbol_item:
                self.show_chart(tipo, symbol_item.text())
