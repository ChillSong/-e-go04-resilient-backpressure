# Decisiones tecnicas — E-Go04: Sistema resiliente con backpressure

Extiende el productor/consumidor de E-Go03 (Redis Streams, dedup exactly-once)
para sobrevivir un spike de 10x mientras el servicio de inventario del que depende
se degrada y falla. La respuesta que construi no es "procesar mas rapido" ni
"aceptar perder datos": es **decidir que se atrasa (solo la verificacion de
inventario), donde espera (un stream durable en Redis) y a que ritmo se le pide
al servicio (AIMD + circuit breaker)** — y demostrar con conteos que nada se perdio.

---

## 0. El ritmo normal y el spike (y por que no es 25k -> 250k)

El enunciado pide usar el throughput de E-Go03 como ritmo normal y 10x como spike.
Mi baseline de Go03 fue **25,004 msg/s**. Tomado literal:

```
spike = 250,000 msg/s x 120 s = 30,000,000 mensajes
archivo eventos_hotsale.jsonl  =  3,126,750 lineas
```

El spike duraria **12.5 s** antes de agotar el archivo, y meter ese volumen en
Redis costaria >10 GB de RAM. Tal cual, no es cumplible con este dataset.

**Decision:** anclar el **spike** en la cifra de Go03 y derivar el normal como 1/10:

| Fase | Tasa | Duracion | Mensajes |
|---|---|---|---|
| normal | 2,500 msg/s | 20 s | 50,000 |
| **spike (10x)** | **25,000 msg/s** = mi throughput sostenido de Go03 | **121 s** | 3,025,000 |
| normal | 2,500 msg/s | hasta EOF | 51,750 |

Asi el spike es exactamente la tasa que Go03 demostro sostener (p95 16 ms), la
relacion 10x se conserva, dura mas de 2 minutos, y el archivo completo cabe.

**Pacing real logrado** (`producer_timeline.csv`, log del productor):

```
phase 1 normal end:   50000 msgs in 20.001s   (achieved 2500 msg/s)
phase 2 spike  end:   3025000 msgs in 2m1s    (achieved 25000 msg/s)
phase 3 normal end:   51750 msgs in 20.701s   (achieved 2500 msg/s)
```

---

## 1. Definicion operativa de "no perder datos"

**Un evento esta procesado cuando:**
1. se aplico **exactamente una vez** al agregado por pais (dedup de Go03), y
2. si es `inventory_reserved`, el servicio de inventario lo **confirmo con un 200**.

**Perder un evento** = que alguna de las dos cosas no ocurra nunca. Atrasarlo no es
perderlo.

### Que se atrasa y que no

- **El agregado nunca se atrasa.** El loop principal no llama al servicio. Durante el
  spike consumio los 3,126,750 mensajes en 2m41.7s — siguio el ritmo de
  25,000 msg/s en tiempo real (p95 de latencia e2e 5 ms).
- **Lo que se atrasa es la verificacion de inventario**, y solo eso. Es una decision:
  el equipo de operaciones necesita el agregado en vivo durante el Hot Sale; la
  confirmacion de inventario puede llegar minutos u horas despues sin cambiar ninguna
  decision de negocio del dia.

### Donde vive lo que se atrasa — tres capas

| Capa | Que es | Limite | Que pasa si se llena |
|---|---|---|---|
| 1. Stream `inventory_checks` (Redis) | Cada `inventory_reserved` unico, persistido antes de confirmar el mensaje original | Memoria de Redis | Capa 3 |
| 2. Buffer en memoria | Ventana de trabajo del pipeline: encolados **+ en vuelo** | **2,000** items | Se deja de leer de la capa 1; el trabajo espera en Redis |
| 3. Stream principal `hotsale` | Si las verificaciones pendientes superan `--max-backlog` (2,000,000), el loop principal deja de leer | Memoria de Redis | Ultimo recurso; los mensajes esperan sin leer |

**Por que el buffer no puede perder un reintento:** la capacidad cuenta los items en
vuelo, no solo los encolados. Un check que falla vuelve al lugar que ya ocupaba
(`Requeue`), asi que nunca encuentra el buffer lleno. Solo el trabajo **nuevo** puede
ser rechazado (`Offer`), y ese trabajo no se pierde: se queda en el stream durable.
Hay un test con 8 goroutines, fallos y reintentos que verifica que cada item se
procesa exactamente las veces esperadas.

**Por que un crash no pierde nada:**
- El paso del stream principal al de inventario es **atomico**: `MULTI { XADD
  inventory_checks ...; XACK hotsale ... }`. O quedan ambas cosas o ninguna; nunca se
  confirma un mensaje cuya verificacion no quedo registrada en algun lugar durable.
- Una verificacion se confirma (`XACK`) **solo despues del 200**. Al reiniciar, el
  consumidor relee primero su lista de pendientes (`XREADGROUP ... 0`) y retoma todo
  lo que quedo sin confirmar.

**Limitacion conocida:** Redis corre sin persistencia (`--appendonly no`, heredado de
Go03 para no contaminar mediciones). Si **Redis** muere, los streams se pierden. En
produccion se activaria AOF (`appendfsync everysec`); el diseno de las tres capas no
cambia. El agregado y la ventana de dedup siguen en memoria del proceso (decision
documentada en Go03 seccion 5).

---

## 2. La senal de backpressure

**Senal principal: el backlog de verificaciones pendientes**

```
backlog = entregadas al stream de inventario - confirmadas por el servicio
```

Son dos contadores atomicos, exactos en todo momento. Se registra cada segundo en
`consumer_timeline.csv` junto con la ocupacion del buffer. Accion: el buffer solo se
recarga desde Redis cuando su ocupacion baja de la marca baja (1,000 de 2,000).

**Senal secundaria: la tasa de error del servicio por segundo**, que alimenta el
control de concurrencia (AIMD, seccion 3): cuanto se le puede pedir al servicio.

### Por que esas senales y no otras

- **No la latencia del loop principal:** esta desacoplado del servicio. Durante todo
  el spike esa latencia fue baja (p95 5 ms) aunque el servicio estuviera
  fallando; mediria el problema equivocado.
- **No la latencia del servicio:** engana justo cuando importa. Por encima de su
  limite, el servicio responde 503 en **20 ms** (fast-fail), y un rechazo de conexion
  tarda ~0 ms. Una senal de latencia diria "el servicio esta rapido" exactamente
  cuando 70% de las llamadas fallan. La tasa de error no tiene ese punto ciego.
- **La profundidad de la cola** mide directamente lo que se esta acumulando (trabajo
  sin confirmar) y no depende de ningun umbral del servicio.

### Lo que la senal registro en la corrida

- Backlog maximo: **797,675** verificaciones pendientes (al terminar el
  productor, con el spike ya absorbido).
- Buffer en memoria: profundidad maxima **2,000** de 2,000 — nunca se
  excedio el limite, aunque el backlog fue ~400 veces mayor.
- Pausas del stream principal por backlog: **0 s** (el limite de 2M no
  se alcanzo; la capa 3 quedo como red de seguridad).

### Por que no frene al productor

El productor representa a los gateways de pago: upstream que no controlo. El stream
es justamente el amortiguador entre su ritmo y el mio, y el loop principal sigue su
ritmo de todas formas. Frenarlo habria trasladado el problema a un sistema ajeno.

---

## 3. Parametros del circuit breaker (y del control de concurrencia)

### Primero: medir al servicio (dia 1)

Antes de fijar numeros caracterice el servicio con concurrencia fija y sin breaker,
6 s por nivel:

| Concurrencia | llamadas/s | **exitos/s** | 503 | errores de conexion |
|---|---|---|---|---|
| 1 | 40 | 40 | 0% | 0% |
| 4 | 60 | **58** | 2.5% | 0% |
| 8 | 69 | 62 | 9% | 1% |
| 16 | 88 | 67 | 13% | 10.5% |
| 32 | 152 | 87 | 14% | 28% |
| 64 | 234 | **119** | 15% | 34% |
| 128 | 696 | 144 | 15% | **64%** |

Dos hallazgos que definieron todo lo demas:

1. **Hay dos modos de falla, no uno.** Ademas de los 503, aparecen `connection
   refused` / `connection reset by peer`: el servidor Python acepta solo 5 conexiones
   en cola y habla HTTP/1.0 (una conexion TCP nueva por request). Esos rechazos llegan
   **en rafagas** — cuando se lanzan muchas conexiones juntas, se rechazan juntas.
2. **El techo de exitos esta ~150/s.** Mas alla, cada llamada extra es casi pura
   falla.

### Umbral de apertura: 20 fallos consecutivos

Con 10 (configuracion A), el breaker se abria **12 veces en 90 s** con tasas de error
de solo 11–35%: las rafagas de rechazos de conexion juntaban 10 fallos seguidos aunque
el servicio estuviera sirviendo. Eso es un falso positivo.

Con 20, en el regimen degradado (error <= 40%) practicamente no se abre, pero cuando el
servicio colapsa al fast-fail el error salta de ~40% a ~71% **en un segundo** y 20
fallos consecutivos llegan en milisegundos. El umbral separa "degradado pero
sirviendo" de "caido". Se ve en `consumer_timeline.csv`: cada apertura coincide con
el salto de error.

### Cooldown: 2 s base, backoff exponencial hasta 30 s

El servicio mide su propia carga en una ventana deslizante de 1 s: al dejar de
llamarlo, su `current_rps` vuelve a 0 en ~1 s. Un cooldown de 2 s es esa ventana mas
margen. Si la prueba en half-open falla, el cooldown se duplica (4, 8, 16, 30 s
tope): un servicio que sigue enfermo recibe cada vez menos intentos.

**Evidencia de que 2 s alcanza:** en la corrida hubo **0** re-aperturas desde
half-open, de 144 aperturas. Las 144 pruebas en half-open salieron bien al primer intento: el servicio siempre se habia recuperado dentro de los 2 s, asi que el backoff exponencial nunca hizo falta. Queda como proteccion para un servicio que siga caido, y esta cubierto por los tests.

### Half-open: 3 llamadas de prueba, las 3 deben salir bien

- Servicio recuperado (carga baja, ~2.5% de error): P(3 exitos) = 0.975^3 ~ **93%**.
- Servicio aun colapsado (70% de error): P(3 exitos) = 0.3^3 ~ **2.7%**.

Tres pruebas bastan para distinguir ambos casos con alta confianza, y son pocas:
nunca se le manda una rafaga a un servicio que acaba de caerse.

### Control de concurrencia: AIMD (min 4, +4/s, /2 si error > 50%)

- **min = 4**: el punto de la tabla con 97% de eficiencia; seguro aun con el servicio
  recien recuperado.
- **+4 por segundo** mientras haya backlog: sube gradual hacia el techo real.
- **/2 si el error supera 50%**: es el criterio de *adaptive throttling* de Google SRE
  (con K=2 un cliente tolera hasta ~50% de rechazos antes de auto-limitarse).
- **breaker abierto -> vuelve a min** (como un timeout en TCP).

El resultado es un **diente de sierra**: la concurrencia sube hasta que el servicio
cruza su techo (~245 llamadas/s, justo debajo de su limite documentado de 250, que el
sistema descubre sin conocerlo), el breaker abre, se enfria 2 s, prueba, cierra y
vuelve a subir. En la corrida completa actuaron las dos defensas: el /2 por tasa de error se disparo **36 veces** (cuando el error paso de 50% antes de juntarse 20 fallos seguidos; p. ej. cerca de t=101 s el error llego a 64% y la concurrencia bajo de 196 a 98) y el breaker **144 veces**.

### Afinacion medida (30,000 lineas, ~7,900 verificaciones cada corrida)

| Config | Umbral | Error para /2 | Paso | Min | Exitos/s | Eficiencia | Aperturas |
|---|---|---|---|---|---|---|---|
| A | 10 | 50% | +8 | 2 | ~58 | — | 12 en 90 s |
| **B (elegida)** | **20** | **50%** | **+4** | **4** | **102** | 70% | 1 |
| C | 10 | 25% | +2 | 4 | 68 | 81% | 0 |
| D = B + slow start | 20 | 50% | x2 hasta ssthresh | 4 | 96 | 70% | 3 |

- **C** es la mas "educada" (81% de eficiencia) pero nunca abre el breaker y drena 1.5x
  mas lento: no protege nada que haga falta proteger y alarga la recuperacion.
- **D (descartada con evidencia):** probe *slow start* estilo TCP para volver mas
  rapido tras cada apertura. Empeoro. Duplicar la concurrencia (32 -> 64) lanza 32
  conexiones nuevas a la vez, la cola de `accept` de 5 se desborda, los rechazos llegan
  en rafaga y el breaker se abre en falso. Con este servicio la rampa lineal es
  estructuralmente mejor.

---

## 4. Evidencia: sobrevivi el spike de 10x sin perdida

### Conteo final contra el esperado

| | Obtenido | Esperado (generador / Go03) | |
|---|---|---|---|
| Lineas del stream procesadas | 3,126,750 | 3,126,750 | OK |
| Eventos unicos aplicados al agregado | 3,035,188 | 3,035,188 | OK |
| Duplicados descartados | 91,562 | 91,562 | OK |
| `inventory_reserved` entregados al stream durable | 815,590 | 815,590 | OK |
| `inventory_reserved` **confirmados por el servicio** | 815,590 | 815,590 | OK |
| Verificaciones sin confirmar al final | 0 | 0 | OK |

Los mismos totales que Go03 (contemplando la deduplicacion), y cada una de las 815,590
reservas de inventario confirmada por el servicio, incluidas las que esperaron horas
en el backlog y las que llegaron mientras el breaker estaba abierto.

### Lo que paso en el camino

| Metrica | Valor |
|---|---|
| Llamadas al servicio | 1,215,019 |
| — exitosas | 815,590 |
| — fallidas (503 / timeout / conexion) | 399,429 (396,369 / 47 / 3,013) |
| Reintentos (todos volvieron al buffer, ninguno se descarto) | 399,429 |
| Veces que se abrio el breaker | **144** |
| Tiempo total abierto | **4m49s** (4.0% de la corrida) |
| Tiempo en half-open | 4.4 s |
| Concurrencia pico / promedio | 200 / 97.8 |
| Duracion total de la corrida | 2h0m35s de tiempo activo (ver nota abajo) |

**Cada llamada fallida es una llamada, no un evento:** fallaron 399,429
llamadas y se perdieron 0 eventos. Un mismo check puede fallar varias veces y
reintentarse hasta que el servicio lo confirma.

### Sobrevivencia al spike

- **Productor:** sostuvo 25,000 msg/s durante 121 s exactos (`producer_timeline.csv`).
- **Loop principal:** consumio el archivo completo en 2m41.7s, al ritmo del
  productor, y el agregado estuvo al dia durante todo el spike.
- **Servicio externo:** nunca se le mando mas de lo que el AIMD permitia, y el breaker
  corto cada colapso en milisegundos. El sistema nunca se cayo ni dejo goroutines
  colgadas: el limite de concurrencia acota las llamadas en vuelo y cada llamada tiene
  un timeout de 3 s.
- **Recuperacion:** el backlog se drena al ritmo que el servicio puede dar (~113
  confirmaciones/s promedio). Ese es el limite fisico del ejercicio: 815,590
  verificaciones contra un servicio que sano atiende ~60/s y en el mejor caso ~150/s.
  Ningun cliente puede drenarlo en menos de ~1.5 h sin martillar al servicio.

### Nota sobre los modos de falla en la corrida real

En la caracterizacion, a concurrencia alta dominaban los rechazos de conexion. En la
corrida completa fueron solo **3,013 de 399,429 fallos (0.75%)**; el 99.2% fueron 503.
La diferencia es la rampa: el AIMD sube +4 por segundo, asi que nunca lanza una
rafaga de conexiones simultaneas que desborde la cola de `accept` del servicio. Es la
misma razon por la que se descarto el slow start (seccion 3).

### Nota sobre el tiempo de la corrida (una prueba de resiliencia no planeada)

La corrida empezo a las 13:20 y termino a las 17:33 de reloj de pared (~4h13m), pero
el consumidor reporta **2h0m35s**. La diferencia es que **la Mac se durmio varias veces
a mitad de la corrida** (estaba con bateria). Durante el sueno los procesos se
congelaron; el reloj monotono de Go no avanza mientras macOS duerme, y el timeline
por segundo tiene **7,232 filas** (~2h0m32s), consistente con el reporte. 2h0m35s es el
tiempo activo real.

Al despertar, el consumidor siguio drenando sin intervencion y sin perder nada: el
conteo final cuadra exacto. Las llamadas que quedaron colgadas durante el sueno
expiraron por el timeout de 3 s (47 timeouts en toda la corrida) y volvieron al buffer
como cualquier otro fallo. No fue una prueba disenada, pero es evidencia de que el
sistema retoma solo despues de una pausa arbitraria.
