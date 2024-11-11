# CHANGELOG

The data displayed on the platform changes constantly. We get new data scraped
from the various sources or spend some more time into cleaning or
deduplication. This means numbers displayed on the platform might change over
time and differ from reported numbers in media publications or official
reports.

Below a list of data updates since the relaunch of the platform on Dec. 1st, 2022.

## 2023-11-10

**2022 data added**

For most of the countries we added the most recent data for the last year, 2022.

This update marks the iteration of the public available data: From now on, the years 2021 and 2022 are publicly available, all others require a research account.

For some countries, we are still trying to get the data and will update them as soon as possible. Countries that are **not updated yet**:

- Poland
- Sweden
- Finland
- The Netherlands
- Italy
- Malta
- Romania

## 2023-01-19

- Added [EU NUTS levels](https://web.archive.org/web/20231219020551/https://ec.europa.eu/eurostat/web/nuts/national-structures) data & aggregations.
- Fixed some data cleaning, this slightly changes some of the aggregated numbers on the platform.

## 2023-01-12

Improved deduping and changed handling of empty names:

- try to take a recipient id from the source, if any
- generate random identifiers for empty names

This slightly changes some of the aggregated numbers on the platform.

## 2022-12-14

Poland data for 2021 was scraped and added.

This changes the order of the top countries in Europe. The
[media stories](https://farmsubsidy.org/stories) published around 1st of
December might mention a different list of top countries in Europe. That's not
a mistake, that's just a small step forward in the effort of trying to
consolidate a diverse dataset scraped from
[different governmental data sources](https://agriculture.ec.europa.eu/common-agricultural-policy/financing-cap/beneficiaries_en#bycountry)
and obtained by freedom of information requests.
