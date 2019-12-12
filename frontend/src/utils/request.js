import Amplify, { API } from "aws-amplify";
import { retryWrapper } from "./index";

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
    endpoints: [{ name: "apiGateway", endpoint: settings.apiUrl, region }]
  }
});

export default (endpoint, { data, headers, method }) =>
  retryWrapper(() =>
    API[method || "get"]("apiGateway", endpoint, {
      body: data || {},
      headers: Object.assign(
        {},
        { "Content-Type": "application/json" },
        headers || {}
      )
    })
  );
