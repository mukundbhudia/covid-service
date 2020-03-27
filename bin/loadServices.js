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
  let result = null
  try {
    const confirmedCases = await getGhTimeSeriesConfirmed()
    // const recoveredCases = await getGhTimeSeriesRecovered()
    const deathCases = await getGhTimeSeriesDeaths()
    result = processing.combineDataFromSources(confirmedCases.data, deathCases.data)
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
      return result
    }
  } catch (error) {
    console.log(error)
    logger.error(error)
    return result
  }
}

const casesByLocation = async () => {
  try {
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
  } catch (err) {
    console.log(err)
    logger.error(err)
    return []
  }
}

const totalConfirmed = async () => {
  try {
    const { data } = await getGisTotalConfirmed()
    return data.features[0].attributes.value
  } catch (err) {
    console.log(err)
    logger.error(err)
    return 0
  }
}

const totalRecovered = async () => {
  try {
    const { data } = await getGisTotalRecovered()
    return data.features[0].attributes.value
  } catch (err) {
    console.log(err)
    logger.error(err)
    return 0
  }
}

const totalDeaths = async () => {
  try {
    const { data } = await getGisTotalDeaths()
    return data.features[0].attributes.value
  } catch (err) {
    console.log(err)
    logger.error(err)
    return 0
  }
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

  if (timeSeriesCases && cases &&
    cases.length > 0 &&
    confirmed > 0 &&
    recovered > 0 &&
    deaths > 0
  ) {
    const allTotals = {
      confirmed: confirmed,
      recovered: recovered,
      deaths: deaths,
      active: confirmed - (recovered + deaths),
      allCountries: [],
      timeSeriesTotalCasesByDate: timeSeriesCases.stats.globalCasesByDate,
      timeStamp: new Date(),
    }
    let combinedCountryCasesWithTimeSeries = []
    let countryFoundMap = {}

    timeSeriesCases.collection.forEach((ghCase) => {
      cases.forEach((gisCase) => {
        if (gisCase.country === ghCase.countryRegion) {
          if ((gisCase.province === ghCase.provinceState) ||
            (gisCase.province === null && ghCase.provinceState === '') ||
            (gisCase.province === null && ghCase.provinceState === ghCase.countryRegion) ) 
          {

            gisCase.provincesList = []
            if (gisCase.province !== null && ghCase.provinceState !== ghCase.countryRegion) {
              gisCase.hasProvince = false
              gisCase.idKey = (gisCase.country + ' ' + gisCase.province).replace(/,/g, '').replace(/\s+/g, '-').toLowerCase()
            } else {
              gisCase.hasProvince = false
              gisCase.idKey = (gisCase.country).replace(/,/g, '').replace(/\s+/g, '-').toLowerCase()
            }
            if (countryFoundMap[gisCase.country]) { // More than one country so it'll have many provinces/regions
              if (gisCase.province === null) {
                gisCase.province = 'mainland'
                gisCase.hasProvince = false
                gisCase.idKey = (gisCase.country + ' ' + gisCase.province).replace(/,/g, '').replace(/\s+/g, '-').toLowerCase()
              }
              countryFoundMap[gisCase.country].provincesList.push({idKey: gisCase.idKey, province: gisCase.province})
              countryFoundMap[gisCase.country].confirmed += gisCase.confirmed
              countryFoundMap[gisCase.country].active += gisCase.active
              countryFoundMap[gisCase.country].recovered += gisCase.recovered
              countryFoundMap[gisCase.country].deaths += gisCase.deaths

              countryFoundMap[gisCase.country].casesByDate.forEach((caseByDate) => {
                ghCase.casesByDate.forEach((ghCaseByDate) => {
                  if (ghCaseByDate.day === caseByDate.day) {
                    caseByDate.confirmed += ghCaseByDate.confirmed
                    // caseByDate.active += ghCaseByDate.active
                    // caseByDate.recovered += ghCaseByDate.recovered
                    caseByDate.deaths += ghCaseByDate.deaths
                  }
                })
              })
            } else {
              if (gisCase.province === null && ghCase.provinceState === ghCase.countryRegion) {
                gisCase.province = 'mainland'
                gisCase.hasProvince = false
                gisCase.idKey = (gisCase.country + ' ' + gisCase.province).replace(/,/g, '').replace(/\s+/g, '-').toLowerCase()
              }
              countryFoundMap[gisCase.country] = {
                idKey: (gisCase.country).replace(/\s+/g, '-').toLowerCase(),
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
                provincesList: [],
                hasProvince: true
              }
              if (gisCase.province !== null) {
                gisCase.hasProvince = false
                countryFoundMap[gisCase.country].hasProvince = true
                countryFoundMap[gisCase.country].provincesList.push({idKey: gisCase.idKey, province: gisCase.province})
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

// Initial load
logger.info("Service loader started...")
fetchAndReplace()
