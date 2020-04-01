const axios = require('axios')

const sourceURLs = {
  urlConfirmed: 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv',
  urlRecovered: 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv',
  urlDeaths: 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv',
  urlConfirmedGlobal: 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv',
  urlDeathsGlobal: 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv',
  urlConfirmedUS: 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv',
  urlDeathsUS: 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv',
  urlNyTimesUS_states: 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv'
}

const getGhTimeSeriesConfirmed = () => {
  return axios.get(sourceURLs.urlConfirmedGlobal)
}

const getGhTimeSeriesRecovered = () => {
  return axios.get(sourceURLs.urlRecovered)
}

const getGhTimeSeriesDeaths = () => {
  return axios.get(sourceURLs.urlDeathsGlobal)
}

const getGhUS_TimeSeriesConfirmed = () => {
  return axios.get(sourceURLs.urlConfirmedUS)
}

const getGhUS_TimeSeriesDeaths = () => {
  return axios.get(sourceURLs.urlDeathsUS)
}

module.exports = {
  getGhTimeSeriesConfirmed,
  // getGhTimeSeriesRecovered,
  getGhTimeSeriesDeaths,
  getGhUS_TimeSeriesConfirmed,
  getGhUS_TimeSeriesDeaths,
}
