import re
from typing import Any, Optional, Tuple, Union

import pandas as pd
from click import File

from .logging import get_logger

log = get_logger(__name__)


def read_csv(
    infile: Union[File, str],
    do_raise: Optional[bool] = True,
    fillna: Optional[Any] = "",
    delimiter: Optional[str] = ",",
    **kwargs,
) -> pd.DataFrame:
    read_kwargs = {**{"on_bad_lines": "warn"}, **kwargs}
    df = None
    if isinstance(infile, str):
        fname = infile
    else:
        fname = infile.name

    try:
        if fname.endswith(".gz"):
            df = pd.read_csv(fname, compression="gzip", **read_kwargs)
        else:
            df = pd.read_csv(infile, **read_kwargs)
    except Exception as e:
        handle_error(log, e, do_raise, fpath=infile.name)
    if df is not None:
        return df.fillna(fillna)


def get_context_from_filename(fname: str) -> Tuple[Union[str, None], Union[str, None]]:
    m = re.match(r".*(?P<country>[a-z\D]{2})_(?P<year>[\d]{4})", fname)
    if m:
        return m.groups()
    m = re.match(r".*(?P<country>[a-z\D]{2})_Subsidies_(?P<year>[\d]{4})", fname)
    if m:
        return m.groups()
    return None, None


def to_json(value):
    try:
        if int(value) == float(value):
            return int(value)
        return float(value)
    except ValueError:
        return str(value)


def handle_error(logger, e: Union[Exception, str], do_raise: bool, **kwargs):
    if isinstance(e, str):
        e = Exception(e)
    if do_raise:
        raise e
    logger.error(f"{e.__class__.__name__}: `{e}`", **kwargs)


def clear_lru():
    import functools
    import gc

    gc.collect()
    wrappers = [
        a for a in gc.get_objects() if isinstance(a, functools._lru_cache_wrapper)
    ]

    for wrapper in wrappers:
        wrapper.cache_clear()
