const csv2json = require('csvjson-csv2json')
const logger = require('../../logger').initLogger()

const sortAlphabeticallyByCountryName = (casesArray) => {
  return casesArray.sort((a, b) => {
    var nameA = a['Country/Region'].trim().toUpperCase(); // ignore upper and lowercase
    var nameB = b['Country/Region'].trim().toUpperCase(); // ignore upper and lowercase
    if (nameA < nameB) {
      return -1
    }
    if (nameA > nameB) {
      return 1
    }
    return 0
  })
}

const processCsvFromSources = (csv_confirmedCases, csv_deathCases) => {
  let collection = []
  let badRows = 0
  let stats = { 
    confirmed: 0,
    // recovered: 0,
    deaths: 0,
    // active: 0,
    globalCasesByDate: {},
  }

  for (let i = 0; i < csv_confirmedCases.length; i++) {
    const confirmedCase = csv_confirmedCases[i]
    // const recoveredCases = csv_recoveredCases[i]
    const deathCases = csv_deathCases[i]

    const countryRegion = confirmedCase['Country/Region'].trim()
    const provinceState = confirmedCase['Province/State'].trim()
    const latitude = confirmedCase['Lat']
    const longtitude = confirmedCase['Long']
  
    const rowKeys = Object.keys(confirmedCase)
  
    let processedData = {
      countryRegion: countryRegion,
      provinceState: provinceState,
      latitude: latitude,
      longitude: longtitude,
      casesByDate: []
    }

    let j = 0
    let daysCounter = 0
    rowKeys.forEach((date) => {
      if (date.match(/^\d{1,2}\/\d{1,2}\/\d{1,2}$/g)) {
        daysCounter ++
        const parsedConfirmed = parseInt(confirmedCase[date], 10)
        // const parsedRecovered = parseInt(recoveredCases[date], 10)
        const parsedDeaths = parseInt(deathCases[date], 10)
        // const parsedActive = parsedConfirmed - parsedRecovered - parsedDeaths

        if (Number.isInteger(parsedConfirmed) && Number.isInteger(parsedDeaths)) {
          if (Math.sign(parsedConfirmed) === -1 || Math.sign(parsedDeaths) === -1) {
            badRows++
            logger.error(`${countryRegion} in ${provinceState} is bad`)
          }
          
          const casesTotalPerDay  = { 
            confirmed: parsedConfirmed,
            // recovered: parsedRecovered,
            deaths: parsedDeaths,
            // active: parsedActive,
            day: date
          }
  
          processedData.casesByDate.push(casesTotalPerDay)
  
          if (
            stats.globalCasesByDate[date]
          ) {
            stats.globalCasesByDate[date].confirmed += parsedConfirmed
            // stats.globalCasesByDate[date].recovered += parsedRecovered
            stats.globalCasesByDate[date].deaths += parsedDeaths
            // stats.globalCasesByDate[date].active += parsedActive
          } else {
            stats.globalCasesByDate[date] = { 
              confirmed: 0,
              // recovered: 0,
              deaths: 0,
              // active: 0,
            }        
          }
  
          if (j + 1 === rowKeys.length) {
            stats.confirmed += parsedConfirmed
            // stats.recovered += parsedRecovered
            stats.deaths += parsedDeaths
            // stats.active += parsedActive
            stats.daysSinceFirstCase = daysCounter
          }
        }
      }
      j ++
    });
    collection.push(processedData)
  }
  if (badRows > 0) { logger.error(`Found ${badRows} badRows`) }
  return { stats: stats, collection: collection }
}

const combineDataFromSources = (confirmedCasesSource, deathCasesSource) => {
  const jsonCsv_confirmedCases = csv2json(confirmedCasesSource)
  // const jsonCsv_recoveredCases = csv2json(recoveredCasesSource)
  const jsonCsv_deathCases = csv2json(deathCasesSource)

  if (
    jsonCsv_confirmedCases.length === jsonCsv_deathCases.length) 
  {
    const sortedConfirmed = sortAlphabeticallyByCountryName(jsonCsv_confirmedCases)
    // const sortedRecovered = sortAlphabeticallyByCountryName(jsonCsv_recoveredCases)
    const sortedDeaths = sortAlphabeticallyByCountryName(jsonCsv_deathCases)
    
    return processCsvFromSources(sortedConfirmed, sortedDeaths)
  } else {
    const errorMsg = `CSV data from multiple sources differs in length. Confirmed: ${jsonCsv_confirmedCases.length}, deaths: ${jsonCsv_deathCases.length}`
    logger.error(errorMsg)
    console.error(errorMsg)
    return null
  }
}

module.exports = {
  combineDataFromSources: combineDataFromSources
}
