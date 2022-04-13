import sys
import time

import click

from . import clean, db, settings
from .logging import configure_logging, get_logger
from .util import get_context_from_filename, read_csv

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
@click.option(
    "--ignore-errors/--no-ignore-errors",
    type=bool,
    default=False,
    help="Don't fail on data validation errors",
    show_default=True,
)
@click.option("--year", type=int)
@click.option("--country", type=str)
def db_clean(infile, outfile, ignore_errors, year=None, country=None):
    """
    Apply data cleaning and currency conversion from `infile` to `outfile`
    """
    is_stream = infile.name == "<stdin>"
    df = read_csv(infile, not ignore_errors)

    # try to get year and country from filename
    if year is None or country is None:
        if not is_stream:
            _country, _year = get_context_from_filename(infile.name)
            year = year or _year
            country = country or _country

    df = clean.clean(
        df, ignore_errors, year, country, fpath=infile.name if not is_stream else None
    )
    df.fillna("").to_csv(outfile, index=False)
    if not is_stream:
        log.info(f"Cleaned `{infile.name}`.", outfile=outfile.name)


@cli.group("db", help="Duckdb related operations")
def cli_db():
    pass


@cli_db.command("init", help="Initialize duckdb database and table.")
@click.option(
    "--recreate/--no-recreate",
    help="Recreate database if it already exists.",
    default=False,
    show_default=True,
)
def db_init(recreate):
    db.init(recreate)


@cli_db.command("import", help="Import cleaned csv into duckdb")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option(
    "--ignore-errors/--no-ignore-errors",
    type=bool,
    default=False,
    help="Don't fail on errors, only log them.",
    show_default=True,
)
def db_import(infile, ignore_errors):
    df = read_csv(infile)
    res = db.insert(df, not ignore_errors, infile.name)
    log.info(f"Inserted {res} rows.", duckdb=settings.DUCKDB_PATH, infile=infile.name)


@cli_db.command("index", help="Build database index for recipient search")
def db_index():
    log.info("Build indexes...", duckdb=settings.DUCKDB_PATH)
    start = time.time()
    db.index()
    end = time.time()
    log.info(
        f"Build index complete. Took {end - start} sec.", duckdb=settings.DUCKDB_PATH
    )
