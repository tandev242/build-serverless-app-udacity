// DONE: Once your application is deployed, copy an API id here so that the frontend could interact with it
const apiId = '75ttozgqkh'
export const apiEndpoint = `https://${apiId}.execute-api.us-east-1.amazonaws.com/dev`

export const authConfig = {
  // DONE: Create an Auth0 application and copy values from it into this map. For example:
  // domain: 'dev-nd9990-p4.us.auth0.com',
  domain: 'dev-id6esl3bnm41llll.us.auth0.com',            // Auth0 domain
  clientId: 'LaoGJdyOwITEsP0Gj6gyCtV9a9OTzBru',          // Auth0 client id
  callbackUrl: 'http://localhost:3000/callback'
}
