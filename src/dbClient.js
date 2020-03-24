const { MongoClient } = require('mongodb')
const logger = require('../logger').initLogger()

let client
let dbName = 'covid19'

const connectDB = async () => {
  let dbURI = process.env.MONGO_URI

  if (process.env.LOCAL) {
    dbURI = 'mongodb://localhost:27017'
  }

  try {
    client = await MongoClient.connect(dbURI, { useUnifiedTopology: true })
  } catch (error) {
    logger.error(error)
  }
}

const disconnectDB = () => client.close()
const getDBClient = () => client.db(dbName)
const getClient = () => client

module.exports = {
  connectDB,
  disconnectDB,
  getDBClient,
  getClient,
}
