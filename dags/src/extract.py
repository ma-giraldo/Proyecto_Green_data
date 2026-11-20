"""Extracción Bronze: descarga datos crudos de Banco Mundial y OWID sin transformarlos."""
from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path

import requests

WB_INDICATOR = "EN.GHG.CO2.PC.CE.AR5"  # CO2e per cápita excl. LULUCF (fuente: Climate Watch)
WB_COUNTRIES = "COL;BRA;CHL;MEX;ARG;PER;ECU;USA;DEU;CHN"
WB_DATE_RANGE = "2000:2025"

OWID_CSV_URL = (
    "https://ourworldindata.org/grapher/co2-emissions-per-capita.csv"
    "?v=1&csvType=full&useColumnShortNames=true"
)

DEFAULT_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
# Permite apuntar los datos a una ruta escribible distinta a dags/data (ej. si el
# volumen de dags/ en el servidor es de solo lectura para el usuario de Airflow).
DATA_DIR = Path(os.environ.get("CO2_DATA_DIR", str(DEFAULT_DATA_DIR)))
BRONZE_DIR = DATA_DIR / "bronze"


def extract_world_bank(run_date: date, bronze_dir: Path = BRONZE_DIR) -> Path:
    """Descarga el indicador de CO2 per cápita del Banco Mundial y guarda el JSON crudo."""
    url = (
        f"https://api.worldbank.org/v2/country/{WB_COUNTRIES}/indicator/{WB_INDICATOR}"
        f"?format=json&date={WB_DATE_RANGE}&per_page=2000"
    )
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    payload = response.json()

    # La API del Banco Mundial responde HTTP 200 incluso cuando el indicador no existe
    # o el formato es inválido; el error viene en el cuerpo, no en el status code.
    if len(payload) == 1 and isinstance(payload[0], dict) and "message" in payload[0]:
        raise RuntimeError(f"Banco Mundial API devolvió un error: {payload[0]['message']}")

    out_dir = bronze_dir / "world_bank"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"co2_percapita_{run_date.isoformat()}.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return out_path


def extract_owid(run_date: date, bronze_dir: Path = BRONZE_DIR) -> Path:
    """Descarga el CSV de OWID (CO2 per cápita, Global Carbon Project) sin modificarlo."""
    response = requests.get(OWID_CSV_URL, timeout=30)
    response.raise_for_status()

    out_dir = bronze_dir / "owid"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"co2_percapita_{run_date.isoformat()}.csv"
    out_path.write_bytes(response.content)
    return out_path
