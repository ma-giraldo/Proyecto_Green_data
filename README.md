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
| Banco Mundial | API pública | `EN.GHG.CO2.PC.CE.AR5` — CO2e per cápita excl. LULUCF (Climate Watch) | hasta 2023 |
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
   dags/data/bronze/world_bank   dags/data/bronze/owid   (JSON/CSV crudo)
          |                           |
          v                           v
   dags/data/silver/world_bank_co2_percapita.csv
   dags/data/silver/owid_co2_percapita.csv       (limpio, país ISO3 + año)
          |                           |
          +------------+--------------+
                       v
   dags/data/gold/co2_percapita_comparativo.csv   (integrado + diferencia)
```

Orquestado por el DAG `co2_percapita_etl` (`dags/co2_percapita_etl_dag.py`),
con tareas TaskFlow: `extraer_banco_mundial`, `extraer_owid`,
`transformar_banco_mundial`, `transformar_owid`, `punto_de_falla_controlada`,
`cargar_gold`.

## 5. Estructura del repositorio

```text
dags/
  co2_percapita_etl_dag.py   # DAG de Airflow
  src/                        # extract.py, transform.py, load.py
  data/{bronze,silver,gold}/  # capas Medallón (contenido ignorado en git)
validacion/                   # hallazgos de la fase de validación de fuentes
tests/                        # pruebas unitarias de la lógica de Gold
evidencias/                   # capturas/logs de ejecución exitosa y fallida
requirements.txt
```

`dags/` se diseñó para copiarse completa (el `.py` **y** la subcarpeta
`src/`) a `/opt/airflow/dags/co2_percapita_etl/` en el servidor — es la
única carpeta que el montaje estándar de Docker/Airflow garantiza. Las
carpetas de datos viven dentro de `dags/data/` por el mismo motivo: no se
asume que el `docker-compose` del servidor monte nada adicional de la raíz
del repo.

## 6. Despliegue

1. Copiar `dags/co2_percapita_etl_dag.py` y `dags/src/` completo a
   `/opt/airflow/dags/co2_percapita_etl/` en el servidor Ubuntu.
2. Asegurarse de que el entorno de Airflow tenga `requests` y `pandas`
   instalados (ver `requirements.txt`).
3. El DAG aparece **pausado** (`is_paused_upon_creation=True`) — activarlo
   manualmente desde la UI solo después de revisarlo.
4. Ejecutarlo manualmente ("Trigger DAG") y revisar el Grid/Graph view.

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
