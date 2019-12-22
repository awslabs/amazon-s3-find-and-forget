import Amplify, { API, Auth } from "aws-amplify";
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
      }
    ]
  }
});

export default (endpoint, options = {}) => {
  const { data, headers, method } = options;
  return retryWrapper(() =>
    API[method || "get"]("apiGateway", endpoint, {
      body: data || {},
      headers: Object.assign(
        {},
        { "Content-Type": "application/json" },
        headers || {}
      )
    })
  );
};
