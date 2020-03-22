const axios = require('axios')

const sourceURLs = {
  urlConfirmed: 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv',
  urlRecovered: 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv',
  urlDeaths: 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv',
}

const getGhTimeSeriesConfirmed = () => {
  return axios.get(sourceURLs.urlConfirmed)
}

const getGhTimeSeriesRecovered = () => {
  return axios.get(sourceURLs.urlRecovered)
}

const getGhTimeSeriesDeaths = () => {
  return axios.get(sourceURLs.urlDeaths)
}

module.exports = {
  getGhTimeSeriesConfirmed,
  getGhTimeSeriesRecovered,
  getGhTimeSeriesDeaths,
}
