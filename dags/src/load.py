"""Carga Gold: integra ambas fuentes por país+año y calcula la diferencia entre estimaciones."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

GOLD_DIR = Path(__file__).resolve().parent.parent / "data" / "gold"
UMBRAL_DIFERENCIA_PORCENTUAL = 10.0


def construir_gold(
    silver_bm_path: Path, silver_owid_path: Path, gold_dir: Path = GOLD_DIR
) -> Path:
    df_bm = pd.read_csv(silver_bm_path)
    df_owid = pd.read_csv(silver_owid_path)

    # outer join: un país/año que solo aparece en una fuente no se descarta,
    # queda con NaN en la columna de la otra fuente.
    df = df_bm.merge(
        df_owid[["pais_iso3", "anio", "co2_percapita_owid"]],
        on=["pais_iso3", "anio"],
        how="outer",
    )

    hay_ambas_fuentes = df["co2_percapita_bm"].notna() & df["co2_percapita_owid"].notna()

    df["diferencia_absoluta"] = pd.NA
    df["diferencia_porcentual"] = pd.NA
    df.loc[hay_ambas_fuentes, "diferencia_absoluta"] = (
        df.loc[hay_ambas_fuentes, "co2_percapita_bm"]
        - df.loc[hay_ambas_fuentes, "co2_percapita_owid"]
    ).abs()
    df.loc[hay_ambas_fuentes, "diferencia_porcentual"] = (
        df.loc[hay_ambas_fuentes, "diferencia_absoluta"]
        / df.loc[hay_ambas_fuentes, "co2_percapita_bm"]
        * 100
    )

    df["hallazgo_calidad"] = "sin_dato_para_comparar"
    df.loc[hay_ambas_fuentes, "hallazgo_calidad"] = df.loc[
        hay_ambas_fuentes, "diferencia_porcentual"
    ].apply(lambda pct: "divergencia_alta" if pct > UMBRAL_DIFERENCIA_PORCENTUAL else "ok")

    gold_dir.mkdir(parents=True, exist_ok=True)
    out_path = gold_dir / "co2_percapita_comparativo.csv"
    df.sort_values(["pais_iso3", "anio"]).to_csv(out_path, index=False)
    return out_path
