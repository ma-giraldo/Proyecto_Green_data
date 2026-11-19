"""DAG Green Data: ETL de CO2 per cápita (Banco Mundial vs. OWID), arquitectura Medallón.

Despliegue: copiar esta carpeta `dags/` completa (el .py y la subcarpeta `src/`)
a `/opt/airflow/dags/co2_percapita_etl/` en el servidor. Las carpetas de datos
(`data/bronze`, `data/silver`, `data/gold`) se crean automáticamente ahí mismo
en la primera corrida.
"""
from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Variable

sys.path.append(str(Path(__file__).resolve().parent / "src"))

from extract import extract_owid, extract_world_bank  # noqa: E402
from load import construir_gold  # noqa: E402
from transform import transform_owid, transform_world_bank  # noqa: E402

PAISES_ISO3 = {"COL", "BRA", "CHL", "MEX", "ARG", "PER", "ECU", "USA", "DEU", "CHN"}

default_args = {
    "retries": 1,
}


@dag(
    dag_id="co2_percapita_etl",
    description="ETL Green Data: CO2 per cápita, Banco Mundial vs. OWID, arquitectura Medallón",
    default_args=default_args,
    schedule=None,  # ejecución manual; cambiar a un cron explícito cuando se decida programarlo
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,  # activar manualmente solo tras validarlo en el servidor
    tags=["green-data", "co2", "medallion"],
)
def co2_percapita_etl():
    @task
    def extraer_banco_mundial(fecha_ejecucion: str) -> str:
        return str(extract_world_bank(date.fromisoformat(fecha_ejecucion)))

    @task
    def extraer_owid(fecha_ejecucion: str) -> str:
        return str(extract_owid(date.fromisoformat(fecha_ejecucion)))

    @task
    def transformar_banco_mundial(bronze_path: str) -> str:
        return str(transform_world_bank(Path(bronze_path)))

    @task
    def transformar_owid(bronze_path: str) -> str:
        return str(transform_owid(Path(bronze_path), PAISES_ISO3))

    @task
    def punto_de_falla_controlada() -> None:
        """Se activa con la Variable de Airflow `co2_simular_fallo` (default: false),
        sin tocar este archivo, para demostrar monitoreo y diagnóstico de errores."""
        if Variable.get("co2_simular_fallo", default_var="false").lower() == "true":
            raise RuntimeError(
                "Fallo simulado vía Variable 'co2_simular_fallo' (prueba de monitoreo)."
            )

    @task
    def cargar_gold(silver_bm_path: str, silver_owid_path: str) -> str:
        return str(construir_gold(Path(silver_bm_path), Path(silver_owid_path)))

    fecha_ejecucion = "{{ ds }}"

    bronze_bm = extraer_banco_mundial(fecha_ejecucion)
    bronze_owid = extraer_owid(fecha_ejecucion)

    silver_bm = transformar_banco_mundial(bronze_bm)
    silver_owid = transformar_owid(bronze_owid)

    chequeo_fallo = punto_de_falla_controlada()
    gold = cargar_gold(silver_bm, silver_owid)

    chequeo_fallo >> gold


co2_percapita_etl()
