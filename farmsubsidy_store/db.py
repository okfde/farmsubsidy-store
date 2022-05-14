import time
from typing import Optional

import pandas as pd

from . import settings
from .aggregations import AGGREGATIONS
from .drivers import get_driver, Driver
from .logging import get_logger
from .util import handle_error

log = get_logger(__name__)


def init(driver: Optional[Driver] = None, recreate: Optional[bool] = False):
    driver = driver or get_driver()
    try:
        driver.init(recreate=recreate)
    except Exception as e:
        if not recreate:
            log.error(
                str(e),
                hint="\nUse the `--recreate` flag to recreate the db from scratch",
            )
        else:
            log.exception(str(e), exception=e)


def insert(
    df: pd.DataFrame,
    driver: Optional[Driver] = None,
    do_raise: Optional[bool] = True,
    fpath: Optional[str] = None,
) -> int:
    driver = driver or get_driver()
    try:
        res = driver.insert(df)
        return res
    except Exception as e:
        handle_error(log, e, do_raise, fpath=fpath)


def get_aggregations(driver: Optional[Driver] = None):
    """print some aggs"""
    driver = driver or get_driver()
    data = {}
    for key, get_agg in AGGREGATIONS.items():
        df = get_agg(driver)
        data[key] = [v for v in df.T.to_dict().values()]
    return data


def measure_time(query):
    """for testing/debugging - it uses the default connection values"""
    for driver in settings.SUPPORTED_DRIVERS:
        _driver = get_driver(driver, settings.DEFAULT_DATABASE_URIS[driver])
        start = time.time()
        _driver.execute(query)
        end = time.time()
        log.info(f"[{driver}] took: {end - start} sec.", driver=_driver)
