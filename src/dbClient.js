const { MongoClient } = require('mongodb')
const logger = require('../logger').initLogger()

let client

const connectDB = async () => {
  try {
    client = await MongoClient.connect(process.env.MONGO_URI, { useUnifiedTopology: true })
  } catch (error) {
    logger.error(error)
  }
}

const disconnectDB = () => client.close()
const getDBClient = () => client.db()
const getClient = () => client

module.exports = {
  connectDB,
  disconnectDB,
  getDBClient,
  getClient,
}
