import re
from typing import Optional, Tuple, Union

import pandas as pd
from click import File

from .logging import get_logger


log = get_logger(__name__)


def handle_error(logger, e: Exception, do_raise: bool, **kwargs):
    if do_raise:
        raise e
    logger.error(f"{e.__class__.__name__}: `{e}`", **kwargs)


def get_context_from_filename(fname: str) -> Tuple[Union[str, None], Union[str, None]]:
    m = re.match(r".*(?P<country>[\w]{2})_(?P<year>[\d]{4})", fname)
    if m:
        return m.groups()
    return None, None


def read_csv(infile: File, do_raise: Optional[bool] = True) -> pd.DataFrame:
    read_kwargs = {"dtype": str, "on_bad_lines": "warn"}
    try:
        if infile.name.endswith(".gz"):
            df = pd.read_csv(infile.name, compression="gzip", **read_kwargs)
        else:
            df = pd.read_csv(infile, **read_kwargs)
    except Exception as e:
        handle_error(log, e, do_raise, fpath=infile.name)
    return df
