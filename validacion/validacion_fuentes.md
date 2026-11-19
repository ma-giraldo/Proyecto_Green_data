# Validación de fuentes (fase de solo lectura, antes de construir el DAG)

**Fecha:** 2026-11-19

Antes de escribir cualquier código de extracción se probaron en vivo cuatro
temas posibles dentro de Green Data, contra las APIs/CSV reales (no se
asumió compatibilidad de ningún indicador).

## Temas evaluados

| Tema | Fuente API (Banco Mundial) | Fuente CSV (OWID) | ¿Fuentes independientes? | Cobertura real verificada |
|---|---|---|---|---|
| Energía | `EG.ELC.RNEW.ZS` (IEA) | `electricity-prod-source-stacked` (Ember/Energy Institute) | Sí | BM hasta 2021 (2022-2023 = `null`), OWID hasta 2025 |
| **CO2 per cápita (elegido)** | `EN.GHG.CO2.PC.CE.AR5` (Climate Watch) | `co2-emissions-per-capita` (Global Carbon Project) | Sí | BM hasta 2023, OWID hasta 2025 |
| Agua | `SH.H2O.SMDW.ZS` / `ER.H2O.FWTL.ZS` | `freshwater-withdrawals-as-a-share-of-internal-resources` | **No** — mismo valor exacto que `ER.H2O.FWTL.ZS` del Banco Mundial | hasta 2023 |
| Agricultura | `AG.YLD.CREL.KG` / `AG.LND.AGRI.ZS` | `fertilizer-use-in-kg-per-hectare-of-arable-land` | **No** — mismo valor exacto que `AG.CON.FERT.ZS` del Banco Mundial | hasta 2023 |

## Hallazgos relevantes

1. **Indicadores archivados en la API del Banco Mundial:** `EG.ELC.RNEW.KH`,
   `EG.USE.ELEC.KH` y `EN.ATM.CO2E.PC` devuelven
   `"The indicator was not found. It may have been deleted or archived."`
   con HTTP 200 (el error viene en el cuerpo, no en el status code — hay que
   revisarlo explícitamente, no basta con `raise_for_status()`).
2. **El indicador de energía del Banco Mundial (`EG.ELC.RNEW.ZS`) no tiene
   datos 2022-2023** para Colombia/Brasil/Chile (`value: null`), mientras
   que OWID sí. Limita la comparación a años ≤2021.
3. **Los CSV de OWID para agua y agricultura son espejos del Banco Mundial**,
   no fuentes independientes — se comprobó descargando ambos y comparando
   valores punto a punto (idénticos). Usarlos no aportaría una verdadera
   segunda fuente para contrastar.
4. **CO2 per cápita resultó la mejor opción:** las dos fuentes son
   metodológicamente distintas (Climate Watch vs. Global Carbon Project) y
   producen valores cercanos pero no idénticos — útil para un indicador de
   calidad real en Gold, en vez de una comparación vacía.
5. **Formato de error de la API del Banco Mundial:** respuesta normal es una
   lista de 2 elementos `[metadata, filas]`; respuesta de error es una lista
   de 1 elemento `[{"message": [...]}]`. `extract.py` distingue ambos casos
   explícitamente.
6. **Compatibilidad de claves:** ambas fuentes usan código de país ISO3
   (`countryiso3code` en Banco Mundial, `code` en OWID) — el cruce en Silver
   se hace por `pais_iso3` + `anio` sin necesidad de un mapeo de nombres.

## Valores de referencia verificados (OWID, `co2-emissions-per-capita`, t CO2/cápita)

| País | 2020 | 2021 | 2022 | 2023 |
|---|---:|---:|---:|---:|
| Brasil | 2.14 | 2.37 | 2.29 | 2.30 |
| Chile | 4.16 | 4.32 | 4.23 | 3.82 |
| Colombia | 1.85 | 2.02 | 1.96 | 2.01 |

Los valores equivalentes del Banco Mundial (`EN.GHG.CO2.PC.CE.AR5`) se
cargan en Bronze al ejecutar el DAG; para Brasil 2021 el valor de referencia
observado fue 2.42 t CO2e/cápita (vs. 2.37 de OWID) — una diferencia
pequeña pero real, consistente con la diferencia CO2 vs. CO2e.

## Periodo de comparación válido

Año **2000 a 2024** (confirmado ejecutando el pipeline completo el
19-nov-2026: el Banco Mundial sí llega hasta 2024 para estos 10 países,
no se queda en 2023 como sugería la revisión inicial). OWID 2025 queda
disponible en Silver/Gold pero sin contraparte del Banco Mundial todavía
(se marca `sin_dato_para_comparar`, nunca se descarta).

## Hallazgo de la primera corrida end-to-end

Al ejecutar el pipeline completo contra las APIs reales (sin Airflow,
directo con Python), la tabla Gold salió con **1487 de 1737 filas**
marcadas `sin_dato_para_comparar`: OWID trae historia desde 1750-1850 para
varios países, y el *outer join* contra el Banco Mundial (que solo se pidió
desde 2000) generaba miles de filas sin sentido comparativo. Se corrigió
acotando `transform_owid()` a `anio >= 2000` (la misma constante
`ANIO_MINIMO` usada como límite inferior de `WB_DATE_RANGE` en
`extract.py`). Tras el fix, Gold quedó en 260 filas (10 países × 26 años):
227 `ok`, 23 `divergencia_alta` (diferencia > 10%, sobre todo Colombia y
China) y 10 `sin_dato_para_comparar` (2025, año sin dato aún del Banco
Mundial). Verificado corriendo `pytest tests/` (2 pruebas, ambas en verde)
y el pipeline completo con Python 3.12.8 local.
