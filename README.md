# Laboratorio DataOps con Apache Airflow — CO2 per cápita

**Asignatura:** Gestión de Proyectos en IA y Ciencia de Datos (UAO)
**Proyecto:** ETL con arquitectura Medallón orquestado en Apache Airflow, tema Green Data.

## 1. Objetivo

Integrar dos estimaciones independientes de emisiones de CO2 per cápita por
país y calcular la diferencia entre ambas, usando arquitectura Medallón
(Bronze/Silver/Gold) orquestada con Apache Airflow, con foco en monitoreo,
trazabilidad y diagnóstico de fallas (no en modelos de ML ni EDA completo).

## 2. Fuentes de datos

| Fuente | Tipo | Indicador | Cobertura |
|---|---|---|---|
| Banco Mundial | API pública | `EN.GHG.CO2.PC.CE.AR5` — CO2e per cápita excl. LULUCF (Climate Watch) | hasta 2024 |
| Our World in Data | CSV | `co2-emissions-per-capita` — CO2 per cápita (Global Carbon Project) | hasta 2025 |

Ambas fuentes fueron validadas contra datos reales antes de construir nada
(ver [`validacion/validacion_fuentes.md`](validacion/validacion_fuentes.md)),
incluyendo por qué se descartaron las opciones de energía, agua y
agricultura.

## 3. Indicador

```text
Diferencia absoluta (t CO2/cápita) = | CO2/cápita Banco Mundial - CO2/cápita OWID |
Diferencia porcentual (%)          = Diferencia absoluta / CO2/cápita Banco Mundial * 100
```

Si la diferencia porcentual supera 10%, la fila se marca
`hallazgo_calidad = divergencia_alta` en Gold. Un país/año que solo existe
en una fuente se marca `sin_dato_para_comparar` — nunca se descarta.

## 4. Arquitectura

```text
   API Banco Mundial          CSV Our World in Data
          |                           |
          v                           v
   (tmp local) -------- publica a SMB -------- (tmp local)
          |                           |
   reportes\co2_percapita_etl\bronze\world_bank\*.json
   reportes\co2_percapita_etl\bronze\owid\*.csv
          |                           |
          v                           v
   (descarga de SMB, transforma localmente, publica de nuevo)
          |                           |
   reportes\co2_percapita_etl\silver\world_bank_co2_percapita.csv
   reportes\co2_percapita_etl\silver\owid_co2_percapita.csv
          |                           |
          +------------+--------------+
                       v
          (descarga ambas, integra, publica)
                       v
   reportes\co2_percapita_etl\gold\co2_percapita_comparativo.csv
```

Las capas Bronze/Silver/Gold se publican en el **share SMB `reportes`**
(conexión de Airflow `dwh_smb_reports`, usado como DWH de archivos del
proyecto — mismo patrón ya probado en otros DAGs del equipo), no en el
sistema de archivos del worker de Airflow: cada tarea escribe en un
directorio temporal local (siempre escribible, sin depender de permisos de
ningún volumen) y publica/descarga del share con verificación por hash
(`dags/src/smb_storage.py`). Esto también resuelve el problema de fondo si
las tareas llegaran a correr en workers distintos, ya que el disco local
de un worker no sería visible para el siguiente.

Orquestado por el DAG `co2_percapita_etl` (`dags/co2_percapita_etl_dag.py`),
con tareas TaskFlow: `extraer_banco_mundial`, `extraer_owid`,
`transformar_banco_mundial`, `transformar_owid`, `punto_de_falla_controlada`,
`cargar_gold`.

**Nota de compatibilidad (el servidor corre Airflow 3.1.8):** el DAG usa
`from airflow.sdk import Variable, dag, task` — no `airflow.decorators` ni
`airflow.models.Variable`, que son la API vieja de Airflow 2.x y están
deprecadas en Airflow 3 (el acceso directo a `Variable` desde código de
tarea ya no funciona así). Se verificó localmente que el archivo parsea sin
errores con Airflow 3.4.0 instalado vía pip (en Windows no se pudo levantar
el `DagBag` completo porque el núcleo de Airflow requiere `fcntl`,
exclusivo de Linux — no aplica al servidor Ubuntu real) y que el grafo de
dependencias resultante es el esperado: `extraer_* → transformar_* →
cargar_gold`, con `punto_de_falla_controlada` como compuerta previa a Gold.

## 5. Estructura del repositorio

```text
dags/
  co2_percapita_etl_dag.py   # DAG de Airflow (orquesta vía SMB)
  src/
    extract.py                # Bronze: descarga Banco Mundial / OWID
    transform.py               # Silver: limpieza y estandarización
    load.py                    # Gold: integración y diferencia
    smb_storage.py              # publicar/descargar del share SMB
  data/{bronze,silver,gold}/  # solo para pruebas locales sin SMB (ignorado en git)
validacion/                   # hallazgos de la fase de validación de fuentes
tests/                        # pruebas unitarias de la lógica de Gold
evidencias/                   # capturas/logs de ejecución exitosa y fallida
requirements.txt
```

`dags/` se diseñó para copiarse completa (el `.py` **y** la subcarpeta
`src/`) a `/opt/airflow/dags/co2_percapita_etl/` en el servidor — es la
única carpeta que el montaje estándar de Docker/Airflow garantiza. Los
datos de Bronze/Silver/Gold **no** viven ahí ni en ningún disco local del
worker: se publican y se leen del share SMB `reportes` (ver sección 4 y el
incidente más abajo), así que no dependen de qué monte o no el
`docker-compose` del servidor.

## 6. Despliegue

1. Copiar `dags/co2_percapita_etl_dag.py` y `dags/src/` completo a
   `/opt/airflow/dags/co2_percapita_etl/` en el servidor Ubuntu.
2. Asegurarse de que el entorno de Airflow tenga `requests`, `pandas` y
   `smbprotocol` instalados (ver `requirements.txt`).
3. Confirmar que la conexión de Airflow `dwh_smb_reports` existe y tiene
   host/usuario/contraseña del share `reportes` (Admin → Connections).
4. El DAG aparece **pausado** (`is_paused_upon_creation=True`) — activarlo
   manualmente desde la UI solo después de revisarlo.
5. Ejecutarlo manualmente ("Trigger DAG") y revisar el Grid/Graph view. Las
   carpetas `co2_percapita_etl/{bronze,silver,gold}/` se crean solas en el
   share la primera vez que una tarea publica un archivo
   (`smbclient.makedirs(..., exist_ok=True)`), no hace falta crearlas a mano.

### Incidente: permisos de escritura en `dags/data/` (resuelto moviendo las capas a SMB)

En el primer despliegue real, `extraer_banco_mundial` falló con
`PermissionError: [Errno 13] Permission denied` al intentar crear
`dags/data/bronze/world_bank/` dentro del volumen montado de `dags/` en
Docker: el usuario `airflow` del contenedor no tenía permiso de escritura
ahí. Es el riesgo que ya se había marcado como pendiente de verificar
(sección 7 del documento de propuesta).

**Solución adoptada:** en vez de buscar una ruta local escribible, las
capas Bronze/Silver/Gold se movieron al share SMB `reportes` (conexión
`dwh_smb_reports`), reutilizando un patrón ya probado en otros DAGs del
equipo. Cada tarea solo necesita un directorio temporal local para su
propio procesamiento (`tempfile.mkdtemp()`, siempre escribible en
cualquier contenedor Linux) y publica/descarga del share — ver
`dags/src/smb_storage.py` y la sección 4. Esto también es más robusto que
fijar una ruta local: no depende de qué volumen monte el `docker-compose`
del servidor, y seguiría funcionando si las tareas corrieran en workers
distintos.

## 7. Provocar una falla controlada (para la evidencia de monitoreo)

En la UI de Airflow: **Admin → Variables** → crear `co2_simular_fallo` con
valor `true` → disparar el DAG → la tarea `punto_de_falla_controlada` falla
y se puede inspeccionar la causa en su log. Volver a `false` (o borrar la
Variable) para que el DAG corra normalmente.

## 8. Pruebas locales

```bash
pip install -r requirements.txt
pytest tests/
```

Las pruebas cubren la lógica de integración de Gold (cálculo de diferencia
y marcado de hallazgos) sin necesidad de Airflow ni de red.

## 9. Alcance

El tema (CO2 per cápita), las fuentes elegidas, el indicador y la
estructura del repositorio son decisiones del equipo. Lo exigido en clase
—Green Data, dos fuentes, arquitectura Medallón, ETL, Apache Airflow,
monitoreo y entrega en GitHub— se documenta en
[`docs/propuesta_proyecto.md`](docs/propuesta_proyecto.md).
