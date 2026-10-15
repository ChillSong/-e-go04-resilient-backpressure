"""
E-Go03 — Generador del log de eventos (JSONL, con duplicados inyectados)
==========================================================================

Simula un dia de "Hot Sale" en un marketplace LATAM: eventos de orden
(order_placed, payment_confirmed, inventory_reserved) publicados como
llegarian a un stream real -- incluyendo reintentos duplicados del
gateway de pagos, que es exactamente el problema que este ejercicio
te pide resolver (exactly-once por event_id).

Requisito: numpy==2.4.4
Uso: python3 generate_events_go03.py
Salida: eventos_hotsale.jsonl (~650-700 MB, ~3,000,000 eventos unicos
        + ~3% de duplicados inyectados, ~3,100,000 lineas totales)
"""

import numpy as np
import json
import uuid
from datetime import datetime, timedelta

NUMPY_VERSION_REQUIRED = "2.4.4"
assert np.__version__ == NUMPY_VERSION_REQUIRED, (
    f"numpy {np.__version__} detectado, se requiere {NUMPY_VERSION_REQUIRED}."
)

SEED = 20261005
rng = np.random.default_rng(seed=SEED)

N_ORDERS = 1_200_000       # ~3M eventos unicos resultantes (orden -> 1 a 3 eventos)
CHUNK_ORDERS = 100_000
N_CHUNKS = N_ORDERS // CHUNK_ORDERS

COUNTRIES = ["MX", "AR", "CO", "CL", "PE"]
PAYMENT_CONFIRM_RATE = 0.85     # % de ordenes que llegan a pago confirmado
INVENTORY_RESERVE_RATE = 0.80   # % de las pagadas que llegan a reserva de inventario
DUPLICATE_RATE = 0.03           # % de eventos que se reenvian (retry de gateway)

start = datetime(2026, 11, 14, 0, 0, 0)  # dia de Hot Sale simulado
out_path = "eventos_hotsale.jsonl"

total_unique = 0
total_duplicates = 0
counts_by_type = {"order_placed": 0, "payment_confirmed": 0, "inventory_reserved": 0}

with open(out_path, "w") as f:
    for chunk_i in range(N_CHUNKS):
        n = CHUNK_ORDERS
        order_ids = [str(uuid.uuid4()) for _ in range(n)]
        countries = rng.choice(COUNTRIES, size=n)
        amounts = np.clip(rng.lognormal(mean=3.8, sigma=0.8, size=n), 8, 900).round(2)
        base_offsets = rng.integers(0, 20 * 3600, size=n)  # ventana de 20h de Hot Sale
        has_payment = rng.random(n) < PAYMENT_CONFIRM_RATE
        has_inventory = has_payment & (rng.random(n) < INVENTORY_RESERVE_RATE)
        dup_rolls = rng.random((n, 3))  # una tirada de duplicado por cada posible evento del orden

        lines = []

        def emit(event_type, order_id, country, amount, ts_offset, dup_roll):
            global total_unique, total_duplicates
            event_id = str(uuid.uuid4())
            ts = (start + timedelta(seconds=int(ts_offset))).strftime("%Y-%m-%dT%H:%M:%S")
            payload = {
                "event_id": event_id,
                "event_type": event_type,
                "order_id": order_id,
                "country": country,
                "amount_usd": amount if event_type != "inventory_reserved" else None,
                "timestamp": ts,
            }
            lines.append(json.dumps(payload))
            total_unique += 1
            counts_by_type[event_type] += 1

            if dup_roll < DUPLICATE_RATE:
                retry_delay = int(rng.integers(2, 180))  # reintento 2s-3min despues
                dup_payload = dict(payload)
                dup_payload["timestamp"] = (
                    start + timedelta(seconds=int(ts_offset) + retry_delay)
                ).strftime("%Y-%m-%dT%H:%M:%S")
                lines.append(json.dumps(dup_payload))  # mismo event_id -- es el duplicado real
                total_duplicates += 1

        for j in range(n):
            emit("order_placed", order_ids[j], str(countries[j]), float(amounts[j]),
                 base_offsets[j], dup_rolls[j, 0])
            if has_payment[j]:
                emit("payment_confirmed", order_ids[j], str(countries[j]), float(amounts[j]),
                     base_offsets[j] + int(rng.integers(5, 300)), dup_rolls[j, 1])
            if has_inventory[j]:
                emit("inventory_reserved", order_ids[j], str(countries[j]), None,
                     base_offsets[j] + int(rng.integers(10, 600)), dup_rolls[j, 2])

        f.write("\n".join(lines) + "\n")
        print(f"chunk {chunk_i + 1}/{N_CHUNKS} -- {(chunk_i + 1) * CHUNK_ORDERS:,} ordenes procesadas")

print(f"\nListo: {out_path}")
print(f"Eventos unicos: {total_unique:,}")
print(f"Duplicados inyectados: {total_duplicates:,} ({total_duplicates / total_unique:.2%})")
print(f"Por tipo: {counts_by_type}")
print(f"Total de lineas en el archivo (unicos + duplicados): {total_unique + total_duplicates:,}")
