import { MongoClient } from "mongodb"

let client

export const connectDB = async () => {
  try {
    client = await MongoClient.connect(process.env.MONGO_URI, { useUnifiedTopology: true })
  } catch (error) {
    console.error(error)
  }
}

export const disconnectDB = () => client.close()
export const getDBClient = () => client.db()
export const getClient = () => client
