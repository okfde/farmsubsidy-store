# converting farmsubsidy data to ftm model

https://docs.alephdata.org/developers/followthemoney/ftm


1. Get the data from https://s3.aleph.ninja/farmsubsidy/cleaned

2. execute mapping:

        ftm map-csv ./mapping.yml | ftm store write -d farmsubsidy
