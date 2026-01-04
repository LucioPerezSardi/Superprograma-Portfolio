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
