#!/usr/bin/env node

const logger = require('../logger').initLogger()

const { connectDB, getDBClient, getClient, disconnectDB } = require('../src/dbClient')
const processing = require('../src/services/csvProcessing')
const {
  getGisCasesByCountry,
  getGisTotalConfirmed,
  getGisTotalRecovered,
  getGisTotalDeaths
} = require('../src/services/gis')

const {
  getGhTimeSeriesConfirmed,
  getGhTimeSeriesRecovered,
  getGhTimeSeriesDeaths
} = require('../src/services/gitHub')

require('dotenv').config()

const timeSeriesData = async () => {
  const confirmedCases = await getGhTimeSeriesConfirmed()
  const recoveredCases = await getGhTimeSeriesRecovered()
  const deathCases = await getGhTimeSeriesDeaths()
  const result = processing.combineDataFromSources(confirmedCases.data, recoveredCases.data, deathCases.data)
  if (result) {
    const keys = Object.keys(result.stats.globalCasesByDate)
    const timeSeries = []
    keys.forEach(day => {
      result.stats.globalCasesByDate[day].day = day
      timeSeries.push(result.stats.globalCasesByDate[day])
    })
    result.stats.globalCasesByDate = timeSeries
    return result
  } else {
    return null
  }
}

const casesByLocation = async () => {
  const { data } = await getGisCasesByCountry()
  return data.features.map(({ attributes }) => ({
    active: attributes.Active,
    confirmed: attributes.Confirmed,
    country: attributes.Country_Region,
    deaths: attributes.Deaths,
    lastUpdate: attributes.Last_Update,
    latitude: attributes.Lat,
    longitude: attributes.Long_,
    // objectId: attributes.OBJECTID,
    province: attributes.Province_State,
    recovered: attributes.Recovered
  }))
}

const totalConfirmed = async () => {
  const { data } = await getGisTotalConfirmed()
  return data.features[0].attributes.value
}

const totalRecovered = async () => {
  const { data } = await getGisTotalRecovered()
  return data.features[0].attributes.value
}

const totalDeaths = async () => {
  const { data } = await getGisTotalDeaths()
  return data.features[0].attributes.value
}

const replaceGis = async () => {
  logger.info("Fetching data...")
  await connectDB()
  const dbClient = getDBClient()
  const session = getClient().startSession()

  const cases = await casesByLocation()
  const timeSeriesCases = await timeSeriesData()
  
  const confirmed = await totalConfirmed()
  const recovered = await totalRecovered()
  const deaths = await totalDeaths()

  const allTotals = {
    confirmed: confirmed,
    recovered: recovered,
    deaths: deaths,
    active: confirmed - (recovered + deaths),
    allCountries: [],
    timeSeriesTotalCasesByDate: timeSeriesCases.stats.globalCasesByDate,
    timeStamp: new Date(),
  }

  if (timeSeriesCases && cases &&
    cases.length > 0 &&
    allTotals.confirmed > 0 &&
    allTotals.recovered > 0 &&
    allTotals.deaths > 0
  ) {
    let combinedCountryCasesWithTimeSeries = []
    let countryFoundMap = {}

    timeSeriesCases.collection.forEach((ghCase) => {
      cases.forEach((gisCase) => {
        if (gisCase.country === ghCase.countryRegion) {
          if ((gisCase.province === ghCase.provinceState) || (gisCase.province === null && ghCase.provinceState === '')) {
            if (gisCase.province !== null) {
              gisCase.provincesList = [gisCase.province]
            } else {
              gisCase.provincesList = []
            }
            if (countryFoundMap[gisCase.country]) { // More than one country so it'll have many provinces/regions
            
              countryFoundMap[gisCase.country].provincesList.push(gisCase.province)
              countryFoundMap[gisCase.country].confirmed += gisCase.confirmed
              countryFoundMap[gisCase.country].active += gisCase.active
              countryFoundMap[gisCase.country].recovered += gisCase.recovered
              countryFoundMap[gisCase.country].deaths += gisCase.deaths
            } else {
              countryFoundMap[gisCase.country] = {
                active: gisCase.active,
                confirmed: gisCase.confirmed,
                country: gisCase.country,
                deaths: gisCase.deaths,
                lastUpdate: gisCase.lastUpdate,
                latitude: gisCase.latitude,
                longitude: gisCase.longitude,
                province: null,
                recovered: gisCase.recovered,
                casesByDate: ghCase.casesByDate,
              }
              if (countryFoundMap[gisCase.country].province === null) {
                countryFoundMap[gisCase.country].provincesList = []
              } else {
                countryFoundMap[gisCase.country].provincesList = [gisCase.province]
              }
            }
            gisCase.casesByDate = ghCase.casesByDate
            combinedCountryCasesWithTimeSeries.push(gisCase)
          }
        }
      })
    })

    const allCountriesFound = Object.keys(countryFoundMap)
    allTotals.allCountries = allCountriesFound
    allCountriesFound.forEach((countryName) => {
      let countryWithProvince = countryFoundMap[countryName]
      if (countryWithProvince.provincesList.length > 0) {
        combinedCountryCasesWithTimeSeries.push(countryWithProvince)
      }
    })

    logger.info(`Countries/Regions total: ${combinedCountryCasesWithTimeSeries.length}. Total distinct countries: ${allCountriesFound.length}. (From ${cases.length} GIS cases and ${timeSeriesCases.collection.length} GH cases)`)

    await session.withTransaction(async () => {
      await dbClient.collection('totals').deleteMany({})
      await dbClient.collection('totals').insertOne(allTotals)

      await dbClient.collection('casesByLocation').deleteMany({})
      await dbClient.collection('casesByLocation').insertMany(combinedCountryCasesWithTimeSeries)
      logger.info("Saved to database.")
    })
  }

  await session.endSession()
  await disconnectDB()
}

const fetchAndReplace = () => {
  try {
    replaceGis()
  } catch (err) {
    logger.error(err)
  } 
}

const SERVICE_FETCH_INTERVAL_IN_MINS = 30

// Initial load
logger.info("Service loader started...")
fetchAndReplace()

setInterval(() => {
  try {
    fetchAndReplace()
  } catch (err) {
    logger.error(err)
  }
}, 1000 * 60 * SERVICE_FETCH_INTERVAL_IN_MINS)

module.exports = {
  fetchAndReplace: fetchAndReplace
}
