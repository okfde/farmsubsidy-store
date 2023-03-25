# farmsubsidy-store

Scripts and pipeline to import [farmsubsidy data](https://data.farmsubsidy.org/latest/)
into different database backends.

And API to query data powered by [fastapi](https://fastapi.tiangolo.com/).

Currently supported database backends:
- [duckdb](https://duckdb.org/) useful for smaller data subsets and for testing
- [clickhouse](https://clickhouse.com/) used for production deployment of the whole dataset

Pipeline steps:
- download data
- clean source data, including currency conversion for the `amount` field
- import cleaned data

## tl;dr

Use the docker container to make things easier.

1.) generate data

    export DATA_ROOT=./data
    export DATA_BASIC_AUTH=farmsubsidy:***
    export DOCKER_IMAGE=ghcr.io/okfde/farmsubsidy:main

    make download

    docker run -v `realpath $DATA_ROOT`:/farmsubsidy/data -e PARALLEL="-j6" $DOCKER_IMAGE make clean

2.) import data

Up and running clickhouse: `make clickhouse`

    docker run -v `realpath $DATA_ROOT`:/farmsubsidy/data -e PARALLEL="-j6" $DOCKER_IMAGE make import


## cli

The cli requires **Python 3.10** or above because of the use of recent type annotations.

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

**source data required columns**

- country
- year
- recipient_name
- amount
- currency

Although, country, currency and year could be set via command line during
cleaning, but it is good practice to have it in the source csv.

**additional columns that are taken if present**

- recipient_id (*helps for deduping if source supplies an identifier*)
- recipient_address
- recipient_street
- recipient_street1
- recipient_street2
- recipient_postcode
- recipient_country
- recipient_url (source url to original data platform?)
- scheme (EU measurement)
- scheme_name
- scheme_code
- scheme_code_short
- scheme_description
- scheme_1
- scheme_2
- amount_original
- currency_original

**output columns**

- pk
- country
- year
- recipient_id
- recipient_name
- recipient_fingerprint
- recipient_address
- recipient_country
- recipient_url
- scheme_id
- scheme
- scheme_code
- scheme_description
- amount
- currency
- amount_original
- currency_original

Options:

    fscli clean --help

pass `--ignore-errors` to only log validate on errors but not fail during exceptions.

    fscli clean --ignore-errors

### import

    fscli db import --help

Create the table:

    fscli db init

It will raise an error if the table already exists, force recreation (and deletion of all data):

    fscli db init --recreate

### other db related commands

execute raw queries:

    fscli db query "select * from farmsubsidy where recipient_id = '4a7ccb6345a2a3d8cf9a2478e408f0cd962e4883'"

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

If the table `farmsubsidy` already exists, it will be deleted!

### parallel cleaning

The cleaning script in the `Makefile` requires [GNU Parallel](https://www.gnu.org/software/parallel/)

For *clickhouse*, parallel importing is also possible.

## API

spin up dev:

    make api

env vars:

- `ALLOWED_ORIGIN`=<origin domain for cors>
- `API_KEY`=<secret api key for frontend app to allow bigger exports>
- `API_HTPASSWD`=<path to nginx .htpasswd>
- `API_TOKEN_SECRET`="secret sign token"   # openssl rand -hex 32
- `API_TOKEN_LIFETIME`=<token lifetime in minutes>

### authentication:

Some data (everything older than the last two years) is hidden for anonymous
requests. Therefore, users are managed via a `.htpasswd` file from which the
api can create JWT tokens to authenticate the frontend app. One can check
`/authenticated` with an `Authorization`-Header in the form of `Bearer <jwt token>`
to get the current auth status for the token.

create a `.htpasswd` file with bcrypt encryption and set env var `API_HTPASSWD`
to the path of this file.

    htpasswd -cbB .htpasswd testuser testpw

The api uses this as a user database. Tokens can obtained using basic auth at
the `/token` endpoint:

```bash
curl -X 'POST' \
  'http://127.0.0.1:8000/login' \
  -H 'Authorization: Basic <...>'
```

That returns a [JWT](https://jwt.io/) token valid for `API_TOKEN_LIFETIME`

```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0dXNlciIsImV4cCI6MTY3MzUzMjI1MH0.vybbse9bNaz1TJJvOJXquh0zSmKGWLhnrBCfkf-2uCY",
  "token_type": "bearer"
}
```

Which can be used for subsequent requests then:

```bash
curl -X 'GET' \
  'http://127.0.0.1:8000/authenticated' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0dXNlciIsImV4cCI6MTY3MzUzMjI1MH0.vybbse9bNaz1TJJvOJXquh0zSmKGWLhnrBCfkf-2uCY'
```


## code style

use [Black](https://black.readthedocs.io/en/stable/)

```config
[flake8]
max-line-length = 88
select = C,E,F,W,B,B950
extend-ignore = E203, E501
```
