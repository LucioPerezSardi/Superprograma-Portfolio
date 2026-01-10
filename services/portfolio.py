from collections import deque
from datetime import datetime
from typing import Dict, Iterable, List, Optional


def _to_float(val) -> float:
    try:
        return float(str(val).replace(",", "."))
    except Exception:
        return 0.0


def compute_cash_by_broker(journal_rows: Iterable[dict], moneda: str = "ARS") -> Dict[str, float]:
    balances: Dict[str, float] = {}
    for row in journal_rows:
        row_moneda = row.get("moneda", "ARS") or "ARS"
        if row_moneda != moneda:
            continue
        ingreso_total = _to_float(row.get("ingreso_total", 0))
        costo_total = _to_float(row.get("costo_total", 0))
        broker = row.get("broker") or "GENERAL"
        balances[broker] = balances.get(broker, 0.0) + ingreso_total - costo_total
    return balances


def compute_holdings_by_broker(journal_rows: Iterable[dict]) -> Dict[str, Dict[str, float]]:
    holdings: Dict[str, Dict[str, float]] = {}
    for row in journal_rows:
        tipo = row.get("tipo", "")
        if tipo in ["Depósito ARS", "Depósito USD", "DepІsito ARS", "DepІsito USD"]:
            continue
        tipo_op = row.get("tipo_operacion", "")
        simbolo = row.get("simbolo", "")
        broker = row.get("broker") or "GENERAL"
        cantidad = _to_float(row.get("cantidad", 0))
        if not simbolo:
            continue
        holdings.setdefault(broker, {})
        if tipo_op == "Compra":
            holdings[broker][simbolo] = holdings[broker].get(simbolo, 0.0) + cantidad
        elif tipo_op == "Venta":
            holdings[broker][simbolo] = max(0.0, holdings[broker].get(simbolo, 0.0) - cantidad)
    return holdings


def compute_finished_operations(
    journal_rows: Iterable[dict],
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
) -> List[dict]:
    journal_data = []
    for row in journal_rows:
        fecha = row.get("fecha") or row.get("Fecha")
        if not fecha:
            continue
        try:
            parsed_date = datetime.strptime(fecha, "%Y-%m-%d")
        except Exception:
            continue
        if from_date and parsed_date < from_date:
            continue
        if to_date and parsed_date > to_date:
            continue
        journal_data.append(
            {
                "Fecha": fecha,
                "Tipo": row.get("tipo") or row.get("Tipo"),
                "Tipo_Operacion": row.get("tipo_operacion") or row.get("Tipo_Operacion"),
                "Simbolo": row.get("simbolo") or row.get("Simbolo"),
                "Detalle": row.get("detalle") or row.get("Detalle"),
                "Cantidad": str(row.get("cantidad", row.get("Cantidad", ""))),
                "Precio": str(row.get("precio", row.get("Precio", ""))),
                "Rendimiento": str(row.get("rendimiento", row.get("Rendimiento", ""))),
                "Total_Descuentos": str(row.get("total_descuentos", row.get("Total_Descuentos", ""))),
            }
        )

    journal_data.sort(key=lambda x: datetime.strptime(x["Fecha"], "%Y-%m-%d"))

    compras_pendientes_hist = {}
    ventas = []
    rendimientos = {}

    for row in journal_data:
        fecha = row["Fecha"]
        tipo = row["Tipo"]
        tipo_op = row["Tipo_Operacion"]
        simbolo = row["Simbolo"]
        detalle = row["Detalle"]
        cantidad = _to_float(row["Cantidad"])
        precio = _to_float(row["Precio"])
        rendimiento = _to_float(row["Rendimiento"])
        total_descuentos = _to_float(row["Total_Descuentos"])

        if tipo in ["Depósito ARS", "Depósito USD", "DepІsito ARS", "DepІsito USD"]:
            continue

        if tipo_op == "Compra":
            compras_pendientes_hist.setdefault(simbolo, deque()).append(
                {
                    "cantidad": cantidad,
                    "precio": precio,
                    "fecha": fecha,
                    "descuentos": total_descuentos,
                    "rendimiento": 0.0,
                }
            )
        elif tipo_op == "Rendimiento" and tipo in [
            "Acciones AR",
            "CEDEARs",
            "Bonos AR",
            "Criptomonedas",
            "ETFs",
            "FCIs AR",
        ]:
            rendimientos.setdefault(simbolo, deque()).append(
                {"fecha": fecha, "valor": rendimiento, "descuentos": total_descuentos}
            )
        elif tipo_op == "Venta":
            ventas.append(
                {
                    "fecha": fecha,
                    "tipo": tipo,
                    "simbolo": simbolo,
                    "detalle": detalle,
                    "cantidad": cantidad,
                    "precio_venta": precio,
                    "rendimiento_venta": rendimiento,
                    "descuentos_venta": total_descuentos,
                }
            )

    for simbolo, rends in rendimientos.items():
        if simbolo not in compras_pendientes_hist:
            continue
        for rend in rends:
            fecha_rend = rend["fecha"]
            tenencia_total = sum(compra["cantidad"] for compra in compras_pendientes_hist[simbolo] if compra["fecha"] <= fecha_rend)
            if tenencia_total == 0:
                continue
            for compra in compras_pendientes_hist[simbolo]:
                if compra["fecha"] <= fecha_rend:
                    proporcion = compra["cantidad"] / tenencia_total
                    compra["rendimiento"] += rend["valor"] * proporcion
                    compra["descuentos"] += rend["descuentos"] * proporcion

    finished_ops: List[dict] = []
    for venta in ventas:
        simbolo = venta["simbolo"]
        cantidad_vendida = venta["cantidad"]
        tipo = venta["tipo"]

        if tipo == "Plazo Fijo":
            if simbolo not in compras_pendientes_hist or not compras_pendientes_hist[simbolo]:
                continue
            compra = compras_pendientes_hist[simbolo].popleft()
            finished_ops.append(
                {
                    "fecha": venta["fecha"],
                    "tipo": tipo,
                    "simbolo": simbolo,
                    "cantidad": cantidad_vendida,
                    "precio_compra": compra["precio"],
                    "precio_venta": venta["precio_venta"],
                    "diferencia_valor": (venta["precio_venta"] - compra["precio"]) * cantidad_vendida,
                    "descuentos": -(compra["descuentos"] + venta["descuentos_venta"]),
                    "rendimiento": venta["rendimiento_venta"],
                    "resultado": venta["rendimiento_venta"] - compra["descuentos"] - venta["descuentos_venta"],
                }
            )
            continue

        if simbolo not in compras_pendientes_hist or not compras_pendientes_hist[simbolo]:
            continue

        costo_total = 0.0
        total_compra_discounts = 0.0
        total_rendimiento = 0.0
        cantidad_restante = cantidad_vendida

        while cantidad_restante > 0 and compras_pendientes_hist[simbolo]:
            compra = compras_pendientes_hist[simbolo][0]
            compra_cantidad = compra["cantidad"]
            cantidad_usada = min(compra_cantidad, cantidad_restante)
            proporcion = cantidad_usada / compra_cantidad if compra_cantidad else 0
            costo_total += compra["precio"] * cantidad_usada
            total_compra_discounts += compra["descuentos"] * proporcion
            total_rendimiento += compra["rendimiento"] * proporcion

            compra["cantidad"] -= cantidad_usada
            cantidad_restante -= cantidad_usada
            if compra["cantidad"] <= 0:
                compras_pendientes_hist[simbolo].popleft()
            else:
                compras_pendientes_hist[simbolo][0] = compra

        if cantidad_restante > 0:
            continue

        valor_venta = venta["precio_venta"] * cantidad_vendida
        diferencia_valor = valor_venta - costo_total
        descuentos_totales = -(total_compra_discounts + venta["descuentos_venta"])
        rendimiento_total = venta["rendimiento_venta"] + total_rendimiento
        resultado = rendimiento_total + descuentos_totales + diferencia_valor

        finished_ops.append(
            {
                "fecha": venta["fecha"],
                "tipo": tipo,
                "simbolo": simbolo,
                "cantidad": cantidad_vendida,
                "precio_compra": costo_total / cantidad_vendida if cantidad_vendida else 0,
                "precio_venta": venta["precio_venta"],
                "diferencia_valor": diferencia_valor,
                "descuentos": descuentos_totales,
                "rendimiento": rendimiento_total,
                "resultado": resultado,
            }
        )

    return finished_ops


def recompute_portfolio_rows(journal_rows: Iterable[dict]) -> List[dict]:
    """
    Recalcula el portafolio agregando todas las operaciones del journal.
    Devuelve filas listas para guardar en la tabla `portfolio`.
    """
    portfolio = {}
    for row in journal_rows:
        tipo = row.get("tipo")
        simbolo = row.get("simbolo")
        tipo_operacion = row.get("tipo_operacion")
        broker = row.get("broker") or "GENERAL"
        moneda = row.get("moneda") or "ARS"
        if not simbolo or not tipo_operacion:
            continue

        # Saltar efectivo líquido
        if tipo in ["Depósito ARS", "Depósito USD", "DepІsito ARS", "DepІsito USD"]:
            continue

        try:
            cantidad = _to_float(row.get("cantidad"))
            costo_total = _to_float(row.get("costo_total"))
        except Exception:
            continue

        key = (simbolo, broker)
        if key not in portfolio:
            portfolio[key] = {"tipo": tipo, "moneda": moneda, "cantidad": 0.0, "costo_acumulado": 0.0}

        if tipo_operacion in ["Compra", "Entrada"]:
            portfolio[key]["cantidad"] += cantidad
            portfolio[key]["costo_acumulado"] += costo_total
        elif tipo_operacion in ["Venta", "Salida"]:
            if portfolio[key]["cantidad"] < cantidad or portfolio[key]["cantidad"] == 0:
                continue
            proporcion = cantidad / portfolio[key]["cantidad"]
            costo_venta = portfolio[key]["costo_acumulado"] * proporcion
            portfolio[key]["cantidad"] -= cantidad
            portfolio[key]["costo_acumulado"] -= costo_venta

    rows_to_save = []
    for (simbolo, broker), data in portfolio.items():
        if data["cantidad"] > 0:
            precio_promedio = data["costo_acumulado"] / data["cantidad"]
            rows_to_save.append(
                {
                    "simbolo": simbolo,
                    "broker": broker,
                    "tipo": data["tipo"],
                    "moneda": data["moneda"],
                    "cantidad": data["cantidad"],
                    "precio_prom": precio_promedio,
                }
            )
    return rows_to_save


def next_plazo_fijo_number(journal_rows: Iterable[dict]) -> int:
    counter = 1
    for row in journal_rows:
        if row.get("tipo") == "Plazo Fijo":
            detalle = str(row.get("detalle", ""))
            if detalle.startswith("Plazo Fijo "):
                try:
                    num = int(detalle.split(" ")[2])
                    counter = max(counter, num + 1)
                except Exception:
                    continue
    return counter
def calcular_descuentos_y_totales(comision: float, derechos: float) -> dict:
    iva_basico = comision * 0.21
    iva_derechos = derechos * 0.21
    total_descuentos = comision + iva_basico + derechos + iva_derechos
    return {
        "iva_basico": iva_basico,
        "iva_derechos": iva_derechos,
        "total_descuentos": total_descuentos,
    }


def _rates_por_broker(
    broker: str,
    tipo: str,
    tipo_op: str,
    bmb_tier: str | None = None,
) -> tuple[float, float]:
    """
    Retorna (comisión%, derechos%) según broker/instrumento.
    Tasas expresadas en proporción (0.005 == 0.5%).
    """
    broker = (broker or "").upper()
    tipo = (tipo or "").upper()
    tipo_op = (tipo_op or "").upper()

    # Default histórico
    default_comm = 0.0
    default_derechos = 0.0
    if tipo_op == "RENDIMIENTO":
        default_comm = 0.01
    elif tipo in ["ACCIONES AR", "CEDEARS", "ETFS"]:
        default_comm = 0.006
        default_derechos = 0.0008
    elif tipo == "BONOS AR":
        default_comm = 0.005
        default_derechos = 0.0001

    # IOL (aprox. tope perfil bajo)
    if broker == "IOL":
        iol_rates = {
            "ACCIONES AR": (0.005, 0.0005),
            "CEDEARS": (0.005, 0.0005),
            "ETFS": (0.005, 0.0005),
            "BONOS AR": (0.005, 0.0001),
            "OPCIONES": (0.005, 0.0006),
            "CAUCIONES": (0.002, 0.0),
        }
        if tipo_op == "RENDIMIENTO":
            return 0.01, 0.0  # usamos mismo criterio por falta de dato específico
        if tipo in iol_rates:
            return iol_rates[tipo]
        return default_comm, default_derechos

    if broker == "BMB":
        tier = (bmb_tier or "digital").lower()
        bmb_rates = {
            "digital": {
                "ACCIONES AR": 0.005,
                "CEDEARS": 0.005,
                "BONOS AR": 0.005,
                "OPCIONES": 0.005,
                "EJERCICIOS": 0.005,
                "FUTUROS": 0.005,
                "LICITACIONES": 0.0025,
                "FCIS AR": 0.0,
                "FCIS": 0.0,
            },
            "active_trader": {
                "ACCIONES AR": 0.0025,
                "CEDEARS": 0.0025,
                "BONOS AR": 0.0025,
                "OPCIONES": 0.0025,
                "EJERCICIOS": 0.0025,
                "FUTUROS": 0.001,
                "LICITACIONES": 0.001,
                "FCIS AR": 0.0,
                "FCIS": 0.0,
            },
            "active_trader_plus": {
                "ACCIONES AR": 0.001,
                "CEDEARS": 0.001,
                "BONOS AR": 0.001,
                "OPCIONES": 0.001,
                "EJERCICIOS": 0.001,
                "FUTUROS": 0.001,
                "LICITACIONES": 0.001,
                "FCIS AR": 0.0,
                "FCIS": 0.0,
            },
        }
        tier_rates = bmb_rates.get(tier, bmb_rates["digital"])
        if tipo in tier_rates:
            return tier_rates[tipo], 0.0
        return default_comm, default_derechos

    return default_comm, default_derechos


def _parse_journal_date(value) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if not value:
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def compute_bmb_monthly_volume(
    journal_rows: Iterable[dict],
    target_date: datetime,
    include_row: Optional[dict] = None,
) -> float:
    eligible_types = {"ACCIONES AR", "CEDEARS", "BONOS AR", "OPCIONES", "EJERCICIOS"}
    total = 0.0

    def add_row(row: dict) -> None:
        nonlocal total
        if (row.get("broker") or "").upper() != "BMB":
            return
        tipo = (row.get("tipo") or "").upper()
        tipo_op = (row.get("tipo_operacion") or "").upper()
        if tipo not in eligible_types:
            return
        if tipo_op not in ("COMPRA", "VENTA"):
            return
        total_sin_desc = _to_float(row.get("total_sin_desc", 0))
        if total_sin_desc == 0:
            total_sin_desc = _to_float(row.get("cantidad", 0)) * _to_float(row.get("precio", 0))
        moneda = (row.get("moneda") or "ARS").upper()
        if moneda == "USD":
            tc = _to_float(row.get("tc_usd_ars", 0)) or 1.0
            total_sin_desc *= tc
        total += total_sin_desc

    for row in journal_rows:
        fecha = _parse_journal_date(row.get("fecha"))
        if not fecha:
            continue
        if fecha.year == target_date.year and fecha.month == target_date.month:
            add_row(row)

    if include_row:
        add_row(include_row)

    return total


def get_bmb_tier(volume_ars: float) -> str:
    if volume_ars >= 25_000_000:
        return "active_trader_plus"
    if volume_ars >= 5_000_000:
        return "active_trader"
    return "digital"


def calcular_operacion(
    tipo: str,
    tipo_op: str,
    cantidad: float,
    precio: float,
    rendimiento: float,
    broker: str = "",
    bmb_tier: str | None = None,
    intraday_bonus: bool = False,
) -> dict:
    total_sin_desc = cantidad * precio + rendimiento
    comm_rate, der_rate = _rates_por_broker(broker, tipo, tipo_op, bmb_tier=bmb_tier)
    comision = comm_rate * total_sin_desc
    if intraday_bonus:
        comision *= 0.5
    derechos = der_rate * total_sin_desc

    descuentos = calcular_descuentos_y_totales(comision, derechos)

    if tipo in ["Depósito ARS", "Depósito USD", "DEPÓSITO ARS", "DEPÓSITO USD"]:
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
            costo_total = total_sin_desc + descuentos["total_descuentos"]
            ingreso_total = 0
            balance = -costo_total
        else:
            costo_total = 0
            ingreso_total = total_sin_desc - descuentos["total_descuentos"]
            balance = ingreso_total

    return {
        "total_sin_desc": total_sin_desc,
        "comision": comision,
        "derechos": derechos,
        "iva_basico": descuentos["iva_basico"],
        "iva_derechos": descuentos["iva_derechos"],
        "total_descuentos": descuentos["total_descuentos"],
        "costo_total": costo_total,
        "ingreso_total": ingreso_total,
        "balance": balance,
    }
