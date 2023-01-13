import pandas as pd
from furl import furl
from pydantic import BaseModel


def get_slice(page: int, limit: int | None = None) -> tuple[int, int]:
    if limit is not None:
        start = (page - 1) * limit
        end = start + limit
        return start, end
    return None, None


def to_csv(df: list[BaseModel] | pd.DataFrame, fpath: str | None = None):
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(dict(i) for i in df)
    df = df.applymap(
        lambda x: ";".join(sorted(str(i) for i in x)) if isinstance(x, list) else x
    )
    if fpath is None:
        return df.fillna("").to_csv(index=False)
    df.fillna("").to_csv(fpath, index=False)


def get_page_url(page: int, url: str, change: int):
    new_page = page + change
    url = furl(url)
    url.args["p"] = new_page
    return str(url)
