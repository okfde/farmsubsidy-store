# farmsubsidy-store

Scripts and pipeline to import [farmsubsidy data](https://data.farmsubsidy.org/latest/)
into different database backends.

And API to query data powered by [flask_restful](https://flask-restful.readthedocs.io/en/latest/).

Currently supported database backends:
- [duckdb](https://duckdb.org/) useful for smaller data subsets and for testing
- [clickhouse](https://clickhouse.com/) used for production deployment of the whole dataset

Pipeline steps:
- download data
- clean source data, including currency conversion for the `amount` field
- import cleaned data

## cli

cleaning and importing is done by a simple command line tool:

    fscli --help

that takes a few environment variables (or a default):

    DRIVER         # default: "clickhouse", alternative: "duckdb"
    DATA_ROOT      # default: "./data", storing downloaded & cleaned data & duckdb
    DATABASE_URI   # default: "localhost" for "clickhouse" DRIVER or "./{$DATA_ROOT}/farmsubsidy.duckdb"
                   # for "duckdb" DRIVER

The client either axcepts csv as `stdin`/`stdout` streams or as argument `-i`/`-o` to a file:

    cat ./data.csv | fscli clean > ./data.cleaned.csv

    fscli clean -i ./data -o ./data.cleaned.csv

of course, cleaning & importing can be done in 1 step:

    cat ./data.csv | fscli clean | fscli import

csv files (or input stream) always needs 1st row as header.


### clean

    fscli clean --help

pass `--ignore-errors` to only log validateion errors but not fail during exceptions.

    fscli clean --ignore-errors

### import

    fscli db import --help

Create the table:

    fscli db init

It will raise an error if the table already exists, force recreation (and deletion of all data):

    fscli db init --recreate

### other db related commands

generate basic aggregations:

    fscli db aggregations > aggregations.json

debug execution time for sample query:

    fscli db time -q "select count(distinct recipient_id), count(distinct recipient_fingerprint) from farmsubsidy"

## pipeline

The full (or partial) pipeline can be executed via the `Makefile`

    make all

this will include:

    make init
    make download
    make clean
    make import

Already downloaded files will only be replaced by newer ones.

**TODO**: If a cleaned csv already exists, it will not be re-generated.

If the table `farmsubsidy` already exists, it will be deleted!

### parallel cleaning

The cleaning script in the `Makefile` requires [GNU Parallel](https://www.gnu.org/software/parallel/)

For *clickhouse*, parallel importing is also possible.


## code style

use [Black](https://black.readthedocs.io/en/stable/)

```config
[flake8]
max-line-length = 88
select = C,E,F,W,B,B950
extend-ignore = E203, E501
```
