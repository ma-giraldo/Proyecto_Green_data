"""Transformación Silver: limpieza, estandarización de país/año y claves de integración."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

SILVER_DIR = Path(__file__).resolve().parent.parent / "data" / "silver"


def transform_world_bank(bronze_path: Path, silver_dir: Path = SILVER_DIR) -> Path:
    payload = json.loads(Path(bronze_path).read_text(encoding="utf-8"))
    rows = payload[1] or []

    df = pd.DataFrame.from_records(
        [
            {
                "pais_iso3": row["countryiso3code"],
                "pais": row["country"]["value"],
                "anio": int(row["date"]),
                "co2_percapita_bm": row["value"],
            }
            for row in rows
        ]
    )
    # El Banco Mundial reporta años recientes sin dato todavía como value=None:
    # se conservan como NaN (hueco documentado), nunca se descartan filas.
    df = df.drop_duplicates(subset=["pais_iso3", "anio"])

    silver_dir.mkdir(parents=True, exist_ok=True)
    out_path = silver_dir / "world_bank_co2_percapita.csv"
    df.to_csv(out_path, index=False)
    return out_path


def transform_owid(
    bronze_path: Path, paises_iso3: set[str], silver_dir: Path = SILVER_DIR
) -> Path:
    df = pd.read_csv(bronze_path)
    df = df.rename(
        columns={
            "entity": "pais",
            "code": "pais_iso3",
            "year": "anio",
            "emissions_total_per_capita": "co2_percapita_owid",
        }
    )
    df = df[df["pais_iso3"].isin(paises_iso3)]
    df = df.drop_duplicates(subset=["pais_iso3", "anio"])

    silver_dir.mkdir(parents=True, exist_ok=True)
    out_path = silver_dir / "owid_co2_percapita.csv"
    df.to_csv(out_path, index=False)
    return out_path
