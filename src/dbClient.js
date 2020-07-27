const { MongoClient } = require('mongodb')
const logger = require('../logger').initLogger()

let client
let dbName = 'covid19'

const connectDB = async () => {
  let dbURI = process.env.MONGO_URI || 'mongodb://localhost:27017'

  try {
    client = await MongoClient.connect(dbURI, { useUnifiedTopology: true })
  } catch (error) {
    console.error(error)
    logger.error(error)
    process.exit(1)
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

