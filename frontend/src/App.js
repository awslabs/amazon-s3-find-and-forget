import React, { useState } from "react";
import { Authenticator, Greetings, SignUp } from "aws-amplify-react";

import Header from "./components/Header";

import gateway from "./utils/gateway";

export default () => {
  const [authState, setAuthState] = useState(undefined);

  const classNames = ["App"];
  if (authState !== "signedIn") classNames.push("amplify-auth");

  return (
    <div className={classNames.join(" ")}>
      <Header />
      <Authenticator
        onStateChange={s => setAuthState(s)}
        hide={[Greetings, SignUp]}
      >
        {authState === "signedIn" && <div>Welcome</div>}
      </Authenticator>
    </div>
  );
};
