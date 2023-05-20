import 'source-map-support/register'
import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda'
import * as middy from 'middy'
import { cors, httpErrorHandler } from 'middy/middlewares'
import { getUserId } from '../utils';
import * as elasticsearch from 'elasticsearch'
import * as httpAwsEs from 'http-aws-es'

const esHost = process.env.ES_ENDPOINT

const es = new elasticsearch.Client({
  hosts: [esHost],
  connectionClass: httpAwsEs
})

export const handler = middy(
  async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
    const { text } = JSON.parse(event.body)
    const userId = getUserId(event)
    const param = {
      "bool": {
        "must": [
          {
            "match": {
              "userId": userId
            }
          },
          {
            "match": {
              "name": text
            }
          }
        ]
      }
    }
    let items = []
    try {
      const result = await es.search({
        index: "todos-index",
        type: 'todos',
        body: {
          "query": param
        }
      })
      let response = result.hits.hits;
      for (const item of response) {
        items.push(item._source)
      }
      return {
        statusCode: 200,
        body: JSON.stringify({ items })
      }
    } catch (error) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error })
      }
    }
  }
)

handler
  .use(httpErrorHandler())
  .use(
    cors({
      credentials: true
    })
  )