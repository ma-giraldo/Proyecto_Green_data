"""DAG Green Data: ETL de CO2 per cápita (Banco Mundial vs. OWID), arquitectura Medallón.

Las capas Bronze/Silver/Gold se publican en el share SMB `reportes`
(conexión de Airflow `dwh_smb_reports`, ver `src/smb_storage.py`), no en el
sistema de archivos local del worker: evita depender de permisos de
escritura en el volumen de `dags/` y funciona igual si las tareas corren en
workers distintos. Cada tarea escribe en un directorio temporal local
(siempre escribible) y publica/descarga del share antes y después de
procesar con pandas.

Despliegue: copiar esta carpeta `dags/` completa (el .py y la subcarpeta
`src/`) a `/opt/airflow/dags/co2_percapita_etl/` en el servidor.
"""
from __future__ import annotations

import sys
import tempfile
from datetime import date, datetime
from pathlib import Path

from airflow.sdk import Variable, dag, task

sys.path.append(str(Path(__file__).resolve().parent / "src"))

import smb_storage  # noqa: E402
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
        tmp_dir = Path(tempfile.mkdtemp(prefix="co2_bronze_wb_"))
        archivo_local = extract_world_bank(date.fromisoformat(fecha_ejecucion), bronze_dir=tmp_dir)
        return smb_storage.subir_archivo(archivo_local, f"{smb_storage.PROYECTO}/bronze/world_bank")

    @task
    def extraer_owid(fecha_ejecucion: str) -> str:
        tmp_dir = Path(tempfile.mkdtemp(prefix="co2_bronze_owid_"))
        archivo_local = extract_owid(date.fromisoformat(fecha_ejecucion), bronze_dir=tmp_dir)
        return smb_storage.subir_archivo(archivo_local, f"{smb_storage.PROYECTO}/bronze/owid")

    @task
    def transformar_banco_mundial(ruta_smb_bronze: str) -> str:
        tmp_dir = Path(tempfile.mkdtemp(prefix="co2_silver_wb_"))
        bronze_local = smb_storage.descargar_archivo(
            ruta_smb_bronze, tmp_dir / smb_storage.nombre_archivo(ruta_smb_bronze)
        )
        silver_local = transform_world_bank(bronze_local, silver_dir=tmp_dir)
        return smb_storage.subir_archivo(silver_local, f"{smb_storage.PROYECTO}/silver")

    @task
    def transformar_owid(ruta_smb_bronze: str) -> str:
        tmp_dir = Path(tempfile.mkdtemp(prefix="co2_silver_owid_"))
        bronze_local = smb_storage.descargar_archivo(
            ruta_smb_bronze, tmp_dir / smb_storage.nombre_archivo(ruta_smb_bronze)
        )
        silver_local = transform_owid(bronze_local, PAISES_ISO3, silver_dir=tmp_dir)
        return smb_storage.subir_archivo(silver_local, f"{smb_storage.PROYECTO}/silver")

    @task
    def punto_de_falla_controlada() -> None:
        """Se activa con la Variable de Airflow `co2_simular_fallo` (default: false),
        sin tocar este archivo, para demostrar monitoreo y diagnóstico de errores."""
        if Variable.get("co2_simular_fallo", default="false").lower() == "true":
            raise RuntimeError(
                "Fallo simulado vía Variable 'co2_simular_fallo' (prueba de monitoreo)."
            )

    @task
    def cargar_gold(ruta_smb_silver_bm: str, ruta_smb_silver_owid: str) -> str:
        tmp_dir = Path(tempfile.mkdtemp(prefix="co2_gold_"))
        silver_bm_local = smb_storage.descargar_archivo(
            ruta_smb_silver_bm, tmp_dir / smb_storage.nombre_archivo(ruta_smb_silver_bm)
        )
        silver_owid_local = smb_storage.descargar_archivo(
            ruta_smb_silver_owid, tmp_dir / smb_storage.nombre_archivo(ruta_smb_silver_owid)
        )
        gold_local = construir_gold(silver_bm_local, silver_owid_local, gold_dir=tmp_dir)
        return smb_storage.subir_archivo(gold_local, f"{smb_storage.PROYECTO}/gold")

    fecha_ejecucion = "{{ ds }}"

    bronze_bm = extraer_banco_mundial(fecha_ejecucion)
    bronze_owid = extraer_owid(fecha_ejecucion)

    silver_bm = transformar_banco_mundial(bronze_bm)
    silver_owid = transformar_owid(bronze_owid)

    chequeo_fallo = punto_de_falla_controlada()
    gold = cargar_gold(silver_bm, silver_owid)

    chequeo_fallo >> gold


co2_percapita_etl()
