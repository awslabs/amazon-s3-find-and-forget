import Amplify, { API, Auth } from "aws-amplify";
import { retryWrapper } from "./retryWrapper";

const settings = window.s3f2Settings || {};
const region = settings.region || "eu-west-1";

const EMPTY_BODY_SHA256 =
  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

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
        endpoint: settings.apiUrl,
        region,
        custom_header: async () => {
          const session = await Auth.currentSession();
          const token = session.getIdToken().getJwtToken();
          return { Authorization: `Bearer ${token}` };
        }
      },
      getRegionalConfig("glue"),
      getRegionalConfig("s3"),
      getRegionalConfig("sts")
    ]
  }
});

const apiWrapper = (api, endpoint, options) => {
  const { data, headers, method } = options;
  const reqOptions = {
    headers: headers || { "Content-Type": "application/json" }
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

export const s3Gateway = endpoint =>
  apiWrapper("s3", endpoint, {
    headers: { "X-Amz-Content-Sha256": EMPTY_BODY_SHA256 }
  });

export const stsGateway = endpoint =>
  apiWrapper("sts", endpoint, { method: "post" });
