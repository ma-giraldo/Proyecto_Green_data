# Laboratorio DataOps con Apache Airflow

**Asignatura:** Gestión de Proyectos en IA y Ciencia de Datos  
**Proyecto:** Implementación de un pipeline ETL con arquitectura Medallón y orquestación mediante Apache Airflow

## 1. Objetivo del proyecto

Desarrollar un proceso **ETL (extracción, transformación y carga)** utilizando dos fuentes de información relacionadas con **Green Data**, implementando la **arquitectura Medallón** y automatizando su ejecución mediante **Apache Airflow**.

El propósito principal del laboratorio es aplicar principios de **DataOps**, especialmente la orquestación, el monitoreo, la trazabilidad y la detección de errores durante la ejecución de un pipeline de datos.

## 2. Requerimientos establecidos por el profesor

Según la transcripción de la clase del **17 de noviembre de 2026**:

| Componente | Requerimiento |
|---|---|
| Temática | Green Data: sostenibilidad, energía, agricultura, agua o clima |
| Fuentes de datos | Dos fuentes, como API, CSV, Excel o bases de datos |
| Procesamiento | ETL: extracción, transformación y carga |
| Arquitectura | Medallón: Bronze, Silver y Gold |
| Orquestación | Apache Airflow |
| Automatización | Ejecución programada o manual desde Airflow |
| Monitoreo | Logs, estado de tareas, historial y detección de errores |
| Entregable | Repositorio del proyecto en GitHub |
| Sustentación | No requiere presentación oral |
| Alcance | No se requiere entrenar modelos de Machine Learning ni realizar un EDA completo |

El profesor enfatizó que el objetivo no es únicamente procesar información, sino **garantizar que el proceso pueda ejecutarse, monitorearse y diagnosticar fallas**.

> **Nota sobre el plazo:** En la transcripción se mencionan tanto «el próximo lunes» como «el 21, el sábado». Conviene verificar en la plataforma la fecha definitiva de entrega.

## 3. Proyecto que proponemos desarrollar

### Emisiones de CO2 per cápita: comparación entre fuentes independientes

**Objetivo específico:** Integrar dos estimaciones independientes de emisiones de CO2 per cápita por país para calcular un indicador comparativo y detectar divergencias metodológicas entre ambas fuentes.

| Fuente propuesta | Tipo | Indicador | Cobertura verificada |
|---|---|---|---|
| Banco Mundial | API pública | `EN.GHG.CO2.PC.CE.AR5` — CO2e per cápita excl. LULUCF (fuente: Climate Watch) | hasta 2023 |
| Our World in Data | Archivo CSV | `co2-emissions-per-capita` — CO2 per cápita (fuente: Global Carbon Project) | hasta 2025 |

**Nota de validación (verificado con datos reales, 19-nov-2026):** se probaron también las opciones de energía, agua y agricultura.
- **Energía** (BM `EG.ELC.RNEW.ZS` + OWID `electricity-prod-source-stacked`): fuentes independientes, pero el Banco Mundial solo tiene datos hasta **2021** para los países de prueba.
- **Agua** y **Agricultura**: los CSV de OWID candidatos (`freshwater-withdrawals-as-a-share-of-internal-resources`, `fertilizer-use-in-kg-per-hectare-of-arable-land`) resultaron ser **espejos literales de los mismos indicadores del Banco Mundial** (`ER.H2O.FWTL.ZS`, `AG.CON.FERT.ZS` — valores idénticos comprobados), por lo que no aportan una segunda fuente realmente independiente.
- **CO2 per cápita** quedó como la mejor opción: fuentes genuinamente independientes (Climate Watch vs. Global Carbon Project), datos más recientes, y diferencias metodológicas reales y pequeñas entre ambas que justifican un chequeo de calidad en Silver/Gold.

**Importante:** Esta fuente y este indicador son una **propuesta del equipo**, no una imposición del profesor. Ambas fuentes usan código de país ISO3 (`COL`, `BRA`, `CHL`...), lo que permite el cruce directo por país y año.

## 4. Arquitectura esperada

```text
                        FUENTES DE DATOS
                 /                           \
     API Banco Mundial                  CSV Our World in Data
     EN.GHG.CO2.PC.CE.AR5                co2-emissions-per-capita
     (Climate Watch)                      (Global Carbon Project)
                 \                           /
                  v                         v
             +-------------------------+
             |        BRONZE           |
             | Datos originales        |
             +-------------------------+
                          |
                          v
             +-------------------------+
             |        SILVER           |
             | Limpieza y validación   |
             +-------------------------+
                          |
                          v
             +-------------------------+
             |         GOLD            |
             | Integración e indicadores|
             +-------------------------+
                          |
                          v
                    Resultado ETL
```

**Apache Airflow** orquesta las tareas de extracción, transformación y carga en todas las etapas, registra sus ejecuciones y permite consultar errores y logs.

### Responsabilidades por capa

- **Bronze:** extraer y conservar los datos originales de ambas fuentes (país, año, valor, tal como vienen).
- **Silver:** validar estructuras, estandarizar código de país (ISO3) y año, manejar duplicados y valores faltantes (ej. años sin dato en alguna fuente), preparar claves de integración país+año.
- **Gold:** unir los dos conjuntos de datos por país+año, calcular la diferencia entre ambas estimaciones y generar el indicador final.
- **Airflow:** programar o ejecutar manualmente el DAG, coordinar dependencias y registrar estados y logs.

## 5. Resultado funcional esperado

Al finalizar, el proyecto deberá permitir:

1. **Extraer** datos desde una API y un archivo CSV.
2. **Almacenar** los datos originales en Bronze.
3. **Transformar y validar** los registros en Silver.
4. **Integrar** las dos fuentes y generar indicadores en Gold.
5. **Ejecutar** el pipeline desde un DAG de Apache Airflow, manualmente o de forma programada.
6. **Consultar el historial** de ejecuciones exitosas y fallidas.
7. **Provocar una falla controlada** y consultar su causa mediante los logs.
8. **Reproducir el proceso** mediante el código y las instrucciones publicadas en GitHub.

### Indicador propuesto

```text
Diferencia absoluta entre fuentes (t CO2/capita) =
    | CO2 per cápita (Banco Mundial) - CO2 per cápita (OWID) |

Diferencia porcentual (%) =
    Diferencia absoluta / CO2 per cápita (Banco Mundial) * 100
```

Una diferencia por encima de un umbral definido (a validar con datos reales, p. ej. 10%) se registra como **hallazgo de calidad** en Gold, sin descartar ningún valor — ambas estimaciones se conservan.

**Resultado en Gold (estructura ilustrativa, valores reales de referencia obtenidos al validar las fuentes):**

| País | Año | CO2/cápita Banco Mundial (t) | CO2/cápita OWID (t) | Diferencia absoluta | Diferencia % |
|---|---|---:|---:|---:|---:|
| Colombia | 2022 | Por cargar | 1.96 | Por calcular | Por calcular |
| Brasil | 2022 | Por cargar | 2.29 | Por calcular | Por calcular |
| Chile | 2022 | Por cargar | 4.23 | Por calcular | Por calcular |

*(Los valores de Banco Mundial y los años 2023-2025 se cargan en la fase de extracción del DAG; los de OWID mostrados aquí ya se verificaron en la fase de validación de fuentes.)*

## 6. Evidencias que deberíamos entregar

| Evidencia | Qué demuestra |
|---|---|
| Repositorio GitHub | Código fuente y estructura del proyecto |
| Archivos Bronze, Silver y Gold | Transformación e integración de datos |
| DAG configurado | Orquestación y orden de tareas |
| Ejecución exitosa | Funcionamiento completo del ETL |
| Ejecución fallida controlada | Detección de errores |
| Logs de Airflow | Trazabilidad y diagnóstico |
| README.md | Instrucciones para reproducir el laboratorio |

## 7. Infraestructura disponible

Ya contamos con:

- **Servidor Ubuntu**.
- **Docker** instalado.
- **Apache Airflow** desplegado en ese servidor.

Por tanto, **no se requiere volver a instalar Airflow en macOS**. El desarrollo del ETL se puede realizar en Visual Studio Code y su ejecución orquestada en el servidor Ubuntu, una vez verificada la configuración actual del despliegue.

## 8. Propuesta de estructura del repositorio

```text
laboratorio-dataops-green-data/
├── dags/
│   └── co2_percapita_etl_dag.py
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── src/
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── validacion/
│   └── validacion_fuentes.md
├── tests/
├── evidencias/
├── requirements.txt
├── .gitignore
└── README.md
```

Esta estructura es **propuesta**, no un formato obligatorio indicado por el profesor. La integración de los archivos y dependencias con el despliegue existente de Airflow dependerá de cómo se encuentre configurado Docker en Ubuntu.

## 9. Criterio de éxito

El laboratorio estará terminado cuando sea posible **ejecutar desde Apache Airflow un proceso ETL completo con dos fuentes Green Data, obtener un resultado integrado en la capa Gold y demostrar que el sistema registra y permite diagnosticar tanto ejecuciones exitosas como fallidas**.

El entregable principal será un **repositorio de GitHub** con el proyecto y las evidencias de funcionamiento.

---

**Distinción de alcance:** El tema de sostenibilidad energética, las fuentes propuestas, el indicador y la estructura sugerida son decisiones del equipo. El uso de Green Data, dos fuentes, arquitectura Medallón, ETL, Apache Airflow, monitoreo y entrega en GitHub corresponden a lo solicitado en clase.
