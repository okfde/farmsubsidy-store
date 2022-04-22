import json
import sys
import time

import click

from . import clean, db, settings
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


@cli.group("db", help="Database related operations")
@click.option(
    "--driver",
    help="Database: `psql` or `duckdb`",
    default=settings.DRIVER,
    show_default=True,
)
@click.pass_context
def cli_db(ctx, driver):
    if driver not in ("psql", "duckdb"):
        raise click.ClickException(
            "Invalid `driver` argument (none of: `psql`, `duckdb`)"
        )
    ctx.obj = {"driver": driver}
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
    db.init(obj["driver"], recreate)


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
    df = read_csv(infile)
    res = db.insert(obj["driver"], df, not ignore_errors, infile.name)
    log.info(f"Inserted {res} rows.", db=settings.DATABASE_URI, infile=infile.name)


@cli_db.command("index", help="Build database index for recipient search")
@click.pass_obj
def db_index(obj):
    log.info("Build indexes...", db=settings.DATABASE_URI)
    start = time.time()
    db.index(obj["driver"])
    end = time.time()
    log.info(f"Build index complete. Took {end - start} sec.", db=settings.DATABASE_URI)


@cli_db.command("aggregations", help="Print out some useful aggregations as json")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.pass_obj
def db_aggregations(obj, outfile):
    log.info("Build aggregations...", db=settings.DATABASE_URI)
    start = time.time()
    res = db.get_aggregations(obj["driver"])
    end = time.time()
    log.info(
        f"Build aggregations complete. Took {end - start} sec.",
        db=settings.DATABASE_URI,
    )
    json.dump(res, outfile, default=to_json)
