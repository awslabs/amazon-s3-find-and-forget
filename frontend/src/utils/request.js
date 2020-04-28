import Amplify, { API, Auth } from "aws-amplify";
import { retryWrapper } from "./retryWrapper";

const settings = window.s3f2Settings || {};
const region = settings.region || "eu-west-1";

const getRegionalConfig = serviceName => ({
  name: serviceName,
  endpoint: `https://${serviceName}.${region}.amazonaws.com`,
  region,
  service: serviceName
});

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
        endpoint: `${settings.apiUrl}v1/`,
        region,
        custom_header: async () => {
          const session = await Auth.currentSession();
          const token = session.getIdToken().getJwtToken();
          return { Authorization: `Bearer ${token}` };
        }
      },
      getRegionalConfig("glue"),
      getRegionalConfig("sts")
    ]
  }
});

const apiWrapper = (api, endpoint, options) => {
  const { data, headers, method, response } = options;
  const reqOptions = {
    headers: headers || { "Content-Type": "application/json" },
    response: response || false
  };
  if (data) reqOptions.body = data;
  return retryWrapper(() => API[method || "get"](api, endpoint, reqOptions));
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

export const stsGateway = endpoint =>
  apiWrapper("sts", endpoint, { method: "post" });
