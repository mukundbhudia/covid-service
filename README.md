# covid-service

Collects, processes and aggregates live COVID-19, and historical time series data from Johns Hopkins University. The aggregated data is then used to populate a mongoDB database which supplies data to https://github.com/mukundbhudia/covid-api and subsequently https://github.com/mukundbhudia/covid-web.

**Note:** This project will be archived soon. It has been re-written in Rust and replaced by https://github.com/mukundbhudia/covid-service-rs.

## Pre-requisites

- NodeJs - see: https://nodejs.org/en/download/ for install instructions for your platform
- mongoDB - see: https://docs.mongodb.com/manual/installation/ for install instructions for your platform

## Running the project

- By default a connection to `'mongodb://localhost:27017'` is made to the local mongoDB.
- Create a `.env` file with mongo URI for the `MONGO_URI` enviroment variable if you wish to connect to a different mongo database.
- Run `npm start` in the project directory.

## Resources & Thanks

- To [Johns Hopkins CSSE](https://github.com/CSSEGISandData/COVID-19) for the hard work providing and collating the data.
