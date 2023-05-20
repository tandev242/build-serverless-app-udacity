import { DynamoDBStreamEvent, DynamoDBStreamHandler } from 'aws-lambda'
import 'source-map-support/register'
import * as elasticsearch from 'elasticsearch'
import * as httpAwsEs from 'http-aws-es'

const esHost = process.env.ES_ENDPOINT

const es = new elasticsearch.Client({
  hosts: [ esHost ],
  connectionClass: httpAwsEs
})

export const handler: DynamoDBStreamHandler = async (event: DynamoDBStreamEvent) => {
  console.log('Processing events batch from DynamoDB', JSON.stringify(event))

  for (const record of event.Records) {
    console.log('Processing record', JSON.stringify(record))
    if (record.eventName === 'INSERT') {
      const newItem = record.dynamodb.NewImage

      const body = {
        userId: newItem.userId.S,
        todoId: newItem.todoId.S,
        name: newItem.name.S,
        done: newItem.done.S,
        createdAt: newItem.createdAt.S,
        dueDate: newItem.dueDate.S
      }

      await es.index({
        index: 'todos-index',
        type: 'todos',
        id: newItem.todoId.S,
        body
      })
    } else if (record.eventName === 'MODIFY') {
      const updatedItem = record.dynamodb.NewImage;
      
      const doc = {
        userId: updatedItem.userId.S,
        todoId: updatedItem.todoId.S,
        name: updatedItem.name.S,
        done: updatedItem.done.S,
        createdAt: updatedItem.createdAt.S,
        dueDate: updatedItem.dueDate.S,
        attachmentUrl: updatedItem.attachmentUrl.S
      }

      // Prepare the Elasticsearch update request
      await es.update({
        index: 'todos-index',
        type: 'todos',
        id: updatedItem.todoId.S,
        body: { doc }
      })
    }
  }
}
