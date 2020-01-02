import Amplify, { API, Auth } from "aws-amplify";
import { retryWrapper } from "./retryWrapper";

const settings = window.s3f2Settings || {};
const region = settings.region || "eu-west-1";

Amplify.configure({
  Auth: {
    identityPoolId: settings.cognitoIdentityPool,
    region,
    mandatorySignIn: true,
    userPoolId: settings.cognitoUserPoolId,
    userPoolWebClientId: settings.cognitoUserPoolClientId
  },
  API: {
    endpoints: [
      {
        name: "apiGateway",
        endpoint: settings.apiUrl,
        region,
        custom_header: async () => {
          const session = await Auth.currentSession();
          const token = session.getIdToken().getJwtToken();
          return { Authorization: `Bearer ${token}` };
        }
      },
      {
        name: "glue",
        endpoint: `https://glue.${region}.amazonaws.com`,
        region,
        service: "glue"
      },
      {
        name: "sts",
        endpoint: `https://sts.${region}.amazonaws.com`,
        region,
        service: "sts"
      }
    ]
  }
});

const apiWrapper = (api, endpoint, options) => {
  const { data, headers, method } = options;
  return retryWrapper(() =>
    API[method || "get"](api, endpoint, {
      body: data || {},
      headers: Object.assign(
        {},
        { "Content-Type": "application/json" },
        headers || {}
      )
    })
  );
};

export const apiGateway = (endpoint, options = {}) =>
  apiWrapper("apiGateway", endpoint, options);

export const glueGateway = (endpointName, data) =>
  apiWrapper("glue", "", {
    data,
    headers: {
      "Content-Type": "application/x-amz-json-1.1",
      "X-Amz-Target": `AWSGlue.${endpointName}`
    },
    method: "post"
  });

export const stsGateway = (endpoint, data) =>
  apiWrapper("sts", `${endpoint}`, {
    data,
    headers: {
      "Content-Type": "application/json"
    },
    method: "post"
  });
