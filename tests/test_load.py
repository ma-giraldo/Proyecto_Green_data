import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent / "dags" / "src"))

from load import construir_gold  # noqa: E402


def test_construir_gold_calcula_diferencias(tmp_path):
    bm = pd.DataFrame(
        {
            "pais_iso3": ["COL", "BRA"],
            "pais": ["Colombia", "Brazil"],
            "anio": [2022, 2022],
            "co2_percapita_bm": [2.0, 2.3],
        }
    )
    owid = pd.DataFrame(
        {
            "pais_iso3": ["COL", "BRA"],
            "pais": ["Colombia", "Brazil"],
            "anio": [2022, 2022],
            "co2_percapita_owid": [1.96, 2.29],
        }
    )
    bm_path = tmp_path / "bm.csv"
    owid_path = tmp_path / "owid.csv"
    bm.to_csv(bm_path, index=False)
    owid.to_csv(owid_path, index=False)

    out_path = construir_gold(bm_path, owid_path, gold_dir=tmp_path)
    resultado = pd.read_csv(out_path).set_index("pais_iso3")

    assert round(resultado.loc["COL", "diferencia_absoluta"], 2) == 0.04
    assert resultado.loc["COL", "hallazgo_calidad"] == "ok"


def test_construir_gold_marca_pais_sin_dato_en_una_fuente(tmp_path):
    bm = pd.DataFrame(
        {
            "pais_iso3": ["CHL"],
            "pais": ["Chile"],
            "anio": [2024],
            "co2_percapita_bm": [None],
        }
    )
    owid = pd.DataFrame(
        {
            "pais_iso3": ["CHL"],
            "pais": ["Chile"],
            "anio": [2024],
            "co2_percapita_owid": [3.74],
        }
    )
    bm_path = tmp_path / "bm.csv"
    owid_path = tmp_path / "owid.csv"
    bm.to_csv(bm_path, index=False)
    owid.to_csv(owid_path, index=False)

    out_path = construir_gold(bm_path, owid_path, gold_dir=tmp_path)
    resultado = pd.read_csv(out_path)

    assert resultado.iloc[0]["hallazgo_calidad"] == "sin_dato_para_comparar"
