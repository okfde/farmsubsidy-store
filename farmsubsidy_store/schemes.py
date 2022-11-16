import os
from functools import lru_cache

import pandas as pd
from fingerprints import generate

schemes = pd.read_csv(os.path.join(os.path.dirname(__file__), "schemes.csv"))
schemes = schemes.set_index("scheme_id")
schemes["scheme_id_lower"] = schemes.index.str.lower()
schemes["scheme_article_lower"] = schemes["scheme_article"].str.lower()
schemes["scheme_name_lower"] = schemes["scheme_name"].str.lower()
schemes["scheme_full_name"] = schemes.index + " - " + schemes["scheme_name"]


@lru_cache(None)
def guess_scheme(value: str):
    if not generate(value):
        return
    value = str(value)
    value = value.lower()
    for _, row in schemes.iterrows():
        if row["scheme_id_lower"] in value:
            return row["scheme_full_name"]
        if not pd.isna(row["scheme_name_lower"]):
            if value in row["scheme_name_lower"]:
                return row["scheme_full_name"]
    return value


DESCRIPTIONS = schemes.set_index("scheme_full_name")
DESCRIPTIONS = schemes["scheme_description"].to_dict()
