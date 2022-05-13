import re
from typing import Any, Iterable, Optional, Tuple, Union

import pandas as pd
from click import File

from .logging import get_logger

log = get_logger(__name__)


def handle_error(logger, e: Exception, do_raise: bool, **kwargs):
    if do_raise:
        raise e
    logger.error(f"{e.__class__.__name__}: `{e}`", **kwargs)


def get_context_from_filename(fname: str) -> Tuple[Union[str, None], Union[str, None]]:
    m = re.match(r".*(?P<country>[a-z\D]{2})_(?P<year>[\d]{4})", fname)
    if m:
        return m.groups()
    return None, None


def read_csv(
    infile: Union[File, str],  # FIXME
    do_raise: Optional[bool] = True,
    fillna: Optional[Any] = "",
    delimiter: Optional[str] = ",",
    names: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    read_kwargs = {"dtype": str, "on_bad_lines": "warn", "names": names}
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


def to_json(value):
    try:
        if int(value) == float(value):
            return int(value)
        return float(value)
    except ValueError:
        return str(value)
