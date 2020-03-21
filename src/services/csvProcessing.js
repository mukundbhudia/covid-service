const csv2json = require('csvjson-csv2json')

const processCsvFromSources = (csv_confirmedCases, csv_recoveredCases, csv_deathCases) => {
  let collection = []
  let badRows = 0
  let stats = { 
    confirmed: 0,
    recovered: 0,
    deaths: 0,
    active: 0,
    globalCasesByDate: {},
  }

  for (let i = 0; i < csv_confirmedCases.length; i++) {
    const confirmedCase = csv_confirmedCases[i]
    const recoveredCases = csv_recoveredCases[i]
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
    rowKeys.forEach((key) => {
      if (key.match(/^\d{1,2}\/\d{1,2}\/\d{1,2}$/g)) {
        daysCounter ++
        // const keyDate = new Date(key)
        const parsedConfirmed = parseInt(confirmedCase[key], 10)
        const parsedRecovered = parseInt(recoveredCases[key], 10)
        const parsedDeaths = parseInt(deathCases[key], 10)
        const parsedActive = parsedConfirmed - parsedRecovered - parsedDeaths

        if (Math.sign(parsedConfirmed) === -1 || Math.sign(parsedRecovered) === -1 || Math.sign(parsedDeaths) === -1) {
          badRows++
          console.error(`${countryRegion} in ${provinceState} is bad`)
        }
        
        const casesTotalPerDay  = { 
          confirmed: parsedConfirmed,
          recovered: parsedRecovered,
          deaths: parsedDeaths,
          active: parsedActive,
          day: key
        }

        processedData.casesByDate.push(casesTotalPerDay)

        if (
          stats.globalCasesByDate[key]
        ) {
          stats.globalCasesByDate[key].confirmed += parsedConfirmed
          stats.globalCasesByDate[key].recovered += parsedRecovered
          stats.globalCasesByDate[key].deaths += parsedDeaths
          stats.globalCasesByDate[key].active += parsedActive
        } else {
          stats.globalCasesByDate[key] = { 
            confirmed: 0,
            recovered: 0,
            deaths: 0,
            active: 0,
          }        
        }

        if (j + 1 === rowKeys.length) {
          stats.confirmed += parsedConfirmed
          stats.recovered += parsedRecovered
          stats.deaths += parsedDeaths
          stats.active += parsedActive
          stats.daysSinceFirstCase = daysCounter
        }
      }
      j ++
    });
    collection.push(processedData)
  }
  if (badRows > 0) { console.error(`Found ${badRows} badRows`) }
  return { stats: stats, collection: collection }
}

const combineDataFromSources = (confirmedCasesSource, recoveredCasesSource, deathCasesSource) => {
  const jsonCsv_confirmedCases = csv2json(confirmedCasesSource)
  const jsonCsv_recoveredCases = csv2json(recoveredCasesSource)
  const jsonCsv_deathCases = csv2json(deathCasesSource)

  if (jsonCsv_confirmedCases.length === jsonCsv_recoveredCases.length &&
    jsonCsv_confirmedCases.length === jsonCsv_deathCases.length &&
    jsonCsv_recoveredCases.length === jsonCsv_deathCases.length) 
  {
    return processCsvFromSources(jsonCsv_confirmedCases, jsonCsv_recoveredCases, jsonCsv_deathCases)
  } else {
    console.error("CSV data from multiple sources differs in length")
  }
}

module.exports = {
  combineDataFromSources: combineDataFromSources
}
