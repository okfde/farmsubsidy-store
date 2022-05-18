import json
import sys
import time

import click

from . import clean, db, drivers, settings
from .logging import configure_logging, get_logger
from .util import get_context_from_filename, read_csv, to_json

log = get_logger(__name__)


@click.group()
@click.option(
    "--log-level",
    default=settings.LOG_LEVEL,
    help="Set logging level",
    show_default=True,
)
def cli(log_level):
    """
    Farmsubsidy Data Cleaner & Importer
    """
    configure_logging(log_level, sys.stderr)


@cli.command("clean")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.option("-h", "--header", help="Comma seperated header row, if not in file")
@click.option("-d", "--delimiter", help="CSV delimiter", default=",", show_default=True)
@click.option(
    "--ignore-errors/--no-ignore-errors",
    type=bool,
    default=False,
    help="Don't fail on data validation errors",
    show_default=True,
)
@click.option("--year", type=int)
@click.option("--country", type=str)
@click.option("--currency", type=str)
def db_clean(
    infile,
    outfile,
    header=None,
    delimiter=",",
    ignore_errors=False,
    year=None,
    country=None,
    currency=None,
):
    """
    Apply data cleaning and currency conversion from `infile` to `outfile`
    """
    is_stream = infile.name == "<stdin>"
    if header is not None:
        header = header.split(",")
    df = read_csv(
        infile, not ignore_errors, delimiter=delimiter, names=header, dtype=str
    )

    # try to get year and country from filename
    if year is None or country is None:
        if not is_stream:
            _country, _year = get_context_from_filename(infile.name)
            year = year or _year
            country = country or _country

    df = clean.clean(
        df,
        ignore_errors,
        year,
        country,
        currency,
        fpath=infile.name if not is_stream else None,
    )
    if df is not None:
        df.fillna("").to_csv(outfile, index=False)
        if not is_stream:
            log.info(f"Cleaned `{infile.name}`.", outfile=outfile.name)


def _get_driver(obj, **kwargs):
    return drivers.get_driver(
        obj["driver"], uri=obj["uri"], table=obj["table"], **kwargs
    )


@cli.group("db", help="Database related operations")
@click.option(
    "--driver",
    help="Database: `clickhouse` or `duckdb`",
    default=settings.DRIVER,
    show_default=True,
)
@click.option(
    "--uri",
    help="Database connection URI",
    default=settings.DATABASE_URI,
    show_default=True,
)
@click.option(
    "--table",
    help="Database table",
    default=settings.DATABASE_TABLE,
    show_default=True,
)
@click.pass_context
def cli_db(ctx, driver, uri, table):
    if driver not in ("duckdb", "clickhouse"):
        raise click.ClickException(
            "Invalid `driver` argument (none of: `psql`, `duckdb`, `clickhouse`)"
        )
    ctx.obj = {"driver": driver, "uri": uri, "table": table}
    log.info(f"Using database driver: `{driver}` ({settings.DATABASE_URI})")


@cli_db.command("init", help="Initialize database and table.")
@click.option(
    "--recreate/--no-recreate",
    help="Recreate database if it already exists.",
    default=False,
    show_default=True,
)
@click.pass_obj
def db_init(obj, recreate):
    driver = _get_driver(obj, read_only=False)
    db.init(driver, recreate=recreate)


@cli_db.command("import", help="Import cleaned csv into database")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option(
    "--ignore-errors/--no-ignore-errors",
    type=bool,
    default=False,
    help="Don't fail on errors, only log them.",
    show_default=True,
)
@click.pass_obj
def db_import(obj, infile, ignore_errors):
    driver = _get_driver(obj, read_only=False)
    # https://clickhouse-driver.readthedocs.io/en/latest/features.html#numpy-pandas-support
    df = read_csv(infile, dtype=object)
    df = df.applymap(lambda x: None if x == "" else x)
    res = db.insert(df, driver, not ignore_errors, infile.name)
    log.info(f"Inserted {res} rows.", db=settings.DATABASE_URI, infile=infile.name)


@cli_db.command("aggregations", help="Print out some useful aggregations as json")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.pass_obj
def db_aggregations(obj, outfile):
    log.info("Build aggregations...", db=settings.DATABASE_URI)
    driver = _get_driver(obj, read_only=True)
    start = time.time()
    res = db.get_aggregations(driver)
    end = time.time()
    log.info(
        f"Build aggregations complete. Took {end - start} sec.",
        db=driver,
    )
    json.dump(res, outfile, default=to_json)


@cli_db.command("query", help="Execute query and print result to outfile")
@click.argument("query")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.pass_obj
def db_query(obj, query, outfile):
    driver = _get_driver(obj, read_only=True)
    df = driver.query(query)
    df.to_csv(outfile, index=False)


@cli_db.command("time", help="Display query execution time for all drivers")
@click.option("-q", "--query", required=True)
def db_time(query):
    db.measure_time(query)
