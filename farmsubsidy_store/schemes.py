import os
import re
from functools import lru_cache

import pandas as pd
from fingerprints import generate

schemes = pd.read_csv(os.path.join(os.path.dirname(__file__), "schemes.csv"))
schemes = schemes.set_index("scheme_id")
schemes["scheme_id_lower"] = schemes.index.str.lower()
schemes["scheme_article_lower"] = schemes["scheme_article"].str.lower()
schemes["scheme_name_lower"] = schemes["scheme_name"].str.lower()
schemes["scheme_full_name"] = (
    schemes.index + " - " + schemes["scheme_name"].fillna("")
).str.rstrip(" - ")


re_ids = re.compile(
    "|".join(
        [
            "(" + i.replace("/", "\/").replace(".", "\.") + "\s)"  # noqa
            for i in sorted(schemes.index, key=lambda x: len(x), reverse=True)
        ],
    ),
    re.IGNORECASE,
)
# MANUAL = {
#     "single payment": schemes.loc[""]
# }


@lru_cache(None)
def guess_scheme(value: str):
    if not generate(value):
        return
    value = str(value)
    m = re_ids.search(value)
    if m is not None:
        return schemes.loc[m.group().strip()]["scheme_full_name"]
    for _, row in schemes.iterrows():
        if not pd.isna(row["scheme_name_lower"]):
            if value.lower() in row["scheme_name_lower"]:
                return row["scheme_full_name"]
    return value


DESCRIPTIONS = schemes.set_index("scheme_full_name")
DESCRIPTIONS = DESCRIPTIONS["scheme_description"].to_dict()
