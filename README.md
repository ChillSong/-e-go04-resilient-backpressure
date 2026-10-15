# E-Go04 — Sistema resiliente con backpressure

Extiende el productor/consumidor de **E-Go03** (Redis Streams, dedup exactly-once)
para sobrevivir el pico real del Hot Sale: un spike de **10x durante 2+ minutos**
mientras el servicio externo de inventario se degrada y falla.

Para cada `inventory_reserved`, el consumidor debe confirmar contra
`inventory_service.py`. El sistema decide de forma explicita que se atrasa (la
verificacion, nunca el agregado), donde espera (un stream durable en Redis) y como
protege al servicio (circuit breaker + concurrencia adaptativa AIMD). Resultado:
**cero eventos perdidos**, solo retrasados.

## Arquitectura

```
                      producer (pacing por fases: 2.5k -> 25k x 121s -> 2.5k msg/s)
                          │ XADD
                          ▼
                 ┌──────────────────┐
                 │ stream "hotsale" │  (Redis)
                 └────────┬─────────┘
                          │ XREADGROUP g1
                          ▼
   ┌────────────────────────────────────────────────────┐
   │ consumer: loop principal (Go03, sin cambios)       │
   │   dedup por event_id  ->  agregado por pais        │  <- siempre en tiempo real
   │   inventory_reserved unicos:                       │
   │     MULTI { XADD inventory_checks ; XACK hotsale } │  <- hand-off atomico
   └────────────────────────┬───────────────────────────┘
                            ▼
              ┌────────────────────────────┐
              │ stream "inventory_checks"  │  <- lo que se atrasa vive AQUI (durable)
              └────────────┬───────────────┘
                           │ refill solo si buffer < low-water   (backpressure)
                           ▼
              ┌────────────────────────────┐
              │ Buffer acotado (2000)      │  queued + en vuelo cuentan contra la capacidad
              └────────────┬───────────────┘
                           │ dispatch: inflight < limite AIMD  &&  breaker listo
                           ▼
              ┌────────────────────────────┐      ┌───────────────────────────┐
              │ workers (concurrencia AIMD)│─────▶│ circuit breaker           │
              └────────────┬───────────────┘      │ closed / open / half-open │
                           │                      └────────────┬──────────────┘
          exito: XACK      │  fallo: Requeue al buffer          │ POST /check-inventory
          (solo entonces)  │  (nunca se descarta)               ▼
                           │                          inventory_service.py :8090
```

La verificacion de inventario se **confirma (XACK) solo despues de que el servicio
respondio 200**. Si el consumidor muere, lo no verificado sigue pendiente en Redis y
se recupera al reiniciar.

## Requisitos

- Go 1.26+
- Docker (Redis)
- Python 3 (stdlib; para `inventory_service.py`)
- El dataset de E-Go03 (`eventos_hotsale.jsonl`), que se reutiliza, no se regenera

## 1. Dataset (reutilizado de E-Go03)

```bash
mkdir -p data
# Opcion A: reutilizar el archivo de Go03
ln -s ../../e-go03/data/eventos_hotsale.jsonl data/eventos_hotsale.jsonl
# Opcion B: regenerarlo identico (misma semilla) con el generador de Go03
#   requiere numpy==2.4.4
cd data && python3 ../generate_events_go03.py && cd ..
```

## 2. Levantar dependencias

```bash
docker compose up -d                 # Redis 7 en el puerto host 6381
python3 inventory_service.py         # en otra terminal: servicio en :8090
curl -s localhost:8090/stats         # -> {"current_rps": 0, ...}
```

## 3. Compilar

```bash
go build -o producer ./cmd/producer
go build -o consumer ./cmd/consumer
```

## 4. Corrida completa con spike de 10x

El consumidor arranca primero (crea streams y grupos, resetea los contadores del
servicio) y el productor publica con el horario de fases:

```bash
# Terminal 1
./consumer --reset

# Terminal 2 — normal 20s, spike 10x por 121s, normal hasta EOF
./producer --reset=false --schedule "2500:20s,25000:121s,2500:0"
```

- El productor termina en ~2m42s.
- El consumidor termina cuando **todas** las verificaciones fueron confirmadas
  (~2 h: el limite es la capacidad del servicio, ver `decisions.md`).
- Durante la corrida imprime cada transicion del breaker y un resumen cada 30s.

### Evidencia que genera la corrida

| Archivo | Contenido |
|---|---|
| `producer_timeline.csv` | Mensajes publicados por segundo y fase — prueba el pacing normal / 10x |
| `consumer_timeline.csv` | Por segundo: backlog, buffer, en vuelo, limite AIMD, llamadas, errores, estado del breaker |
| `breaker_transitions.csv` | Cada transicion closed/open/half-open con su motivo y cooldown |

## Resultado verificado (corrida completa)

```
==================== CERO PERDIDA ==========================
  eventos unicos:                       3035188  esperado    3035188  [OK]
  duplicados descartados:                 91562  esperado      91562  [OK]
  inventory_reserved entregados:         815590  esperado     815590  [OK]
  inventory_reserved verificados:        815590  esperado     815590  [OK]
  verificaciones sin confirmar:      0
  RESULTADO: OK — ningun evento perdido, solo retrasado
```

| Metrica | Valor |
|---|---|
| Spike | 25,000 msg/s durante 121 s (normal 2,500 msg/s) |
| Stream principal durante el spike | al dia, p95 e2e 5 ms |
| Breaker | 144 aperturas, 4m49s abierto (4% de la corrida), 0 re-aperturas desde half-open |
| Backlog maximo | 797,675 verificaciones pendientes, en Redis |
| Buffer en memoria | maximo 2,000 de 2,000; nunca excedido |
| Llamadas al servicio | 1,215,019 (399,429 fallidas, todas reintentadas) |
| Drenado | ~113 confirmaciones/s; 2h0m35s de tiempo activo |

Detalle y justificacion de cada numero en `decisions.md`.

## Flags principales

### producer (extiende Go03)

| Flag | Default | Descripcion |
|---|---|---|
| `--schedule` | `` | Fases `tasa:duracion,...`; `0` = hasta EOF. Ej: `2500:20s,25000:121s,2500:0` |
| `--rate` | `0` | Tasa unica (comportamiento de Go03) si no hay `--schedule` |
| `--limit` | `0` | Publicar solo N lineas (smoke tests) |
| `--timeline` | `producer_timeline.csv` | Log por segundo |
| `--addr` | `localhost:6381` | Redis |

### consumer (extiende Go03)

| Flag | Default | Descripcion |
|---|---|---|
| `--breaker-threshold` | `20` | Fallos consecutivos que abren el breaker |
| `--breaker-cooldown` | `2s` | Tiempo abierto antes de half-open |
| `--breaker-max-cooldown` | `30s` | Tope del backoff exponencial del cooldown |
| `--breaker-probes` | `3` | Llamadas de prueba en half-open (todas deben salir bien) |
| `--conc-min` / `--conc-max` | `4` / `256` | Rango de concurrencia hacia el servicio |
| `--conc-step` | `4` | Incremento aditivo por segundo |
| `--conc-max-err` | `0.5` | Tasa de error que dispara la reduccion a la mitad |
| `--buffer` / `--low-water` | `2000` / `1000` | Buffer en memoria y marca de recarga |
| `--max-backlog` | `2000000` | Si se supera, se pausa el stream principal |
| `--inv-url` | `http://localhost:8090` | Servicio de inventario |
| `--expect-*` | conteos del generador | Para la verificacion final de cero perdida |

## Tests

```bash
go test -race ./internal/...
```

- `breaker`: abre tras N fallos consecutivos, rechaza durante el cooldown, half-open
  limita las probes, cierra si todas salen bien, re-abre con cooldown duplicado y
  topado, un resultado tardio de un estado anterior no corrompe el estado actual,
  contabilidad de tiempo abierto (reloj inyectado, sin sleeps).
- `backpressure`: el buffer rechaza trabajo nuevo al llenarse pero un reintento
  nunca se descarta; 8 goroutines concurrentes con fallos y reintentos no pierden
  ni duplican ningun item; AIMD sube, reduce a la mitad y cae al minimo.
- `dedup`: la ventana de Go03.

## Estructura

```
cmd/producer/            Go03 + pacing por fases y timeline
cmd/consumer/main.go     Loop de Go03 + hand-off atomico al stream de inventario
cmd/consumer/inventory.go  Pipeline de verificacion: refill, dispatch, workers, tick
cmd/consumer/report.go   Reporte final (Go03 + resiliencia + cero perdida)
internal/breaker/        Circuit breaker de 3 estados
internal/backpressure/   Buffer acotado, controlador AIMD, gauge del backlog
internal/inventory/      Cliente HTTP del servicio
internal/dedup/          (Go03) ventana LRU por event_id
internal/aggregate/      (Go03) agregado por pais
internal/event/          (Go03) modelo del evento
inventory_service.py     Servicio simulado (material del curso)
generate_events_go03.py  Generador del dataset de Go03 (para regenerarlo identico)
```
