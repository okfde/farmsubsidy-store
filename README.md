# farmsubsidy-import

Scripts and pipeline to import [farmsubsidy data](https://data.farmsubsidy.org/latest/)
into a [duckdb database](https://duckdb.org/).

Pipeline steps:
- download data
- clean source data, including currency conversion for the `amount` field
- import cleaned data

## cli

cleaning and importing is done by a simple command line tool:

    fscli --help

that takes a few environment variables (or a default):

    DATA_ROOT      # default: ./data, storing downloaded & cleaned data
    DUCKDB_PATH    # default: ./farmsubsidy.duckdb

The client either axcepts csv as `stdin`/`stdout` streams or as argument to a file:

    cat ./data.csv | fscli clean > ./data.cleaned.csv

    fscli clean -i ./data -o ./data.cleaned.csv

csv files (or input stream) always needs 1st row as header.


### clean

    fscli clean --help

pass `--ignore-errors` to only log validateion errors but not fail.

    fscli clean --ignore-errors

### import

    fscli db import --help

Create the duckdb table:

    fscli db init

It will raise an error if the table already exists, force recreation (and deletion of all data):

    fscli db init --recreate

### other db related commands

Create the FTS for recipients search:

    fscli db index

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

If the duckdb table `farmsubsidy` already exists, it will be deleted!

### parallel cleaning

The cleaning script in the `Makefile` requires [GNU Parallel](https://www.gnu.org/software/parallel/)


## code style

use [Black](https://black.readthedocs.io/en/stable/)

```config
[flake8]
max-line-length = 88
select = C,E,F,W,B,B950
extend-ignore = E203, E501
```
