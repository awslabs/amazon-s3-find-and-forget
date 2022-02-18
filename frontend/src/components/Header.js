import { Auth } from "aws-amplify";
import React, { useEffect, useState } from "react";
import { Button, Navbar } from "react-bootstrap";

import { retryWrapper } from "../utils/retryWrapper";
import "./Header.css";

const Header = ({ signedIn }) => {
  const [authError, setAuthError] = useState(null);
  const [userEmail, setUserEmail] = useState(undefined);

  const reload = () => window.location.reload();

  const signOut = () => Auth.signOut().then(reload).catch(reload);

  useEffect(() => {
    if (signedIn) {
      retryWrapper(() => Auth.currentAuthenticatedUser())
        .then(user => setUserEmail(user.username))
        .catch(setAuthError);
    }
  }, [signedIn]);

  return (
    <Navbar variant="dark">
      <Navbar.Brand>
        <div className="awslogo" />
      </Navbar.Brand>
      <Navbar.Toggle />
      {(userEmail || authError) && (
        <Navbar.Collapse className="justify-content-end">
          <Navbar.Text>
            {authError && (
              <>
                <span className="auth-error">
                  Authentication error: {authError}
                </span>
                <Button variant="link" className="headerLink" onClick={reload}>
                  Retry
                </Button>
              </>
            )}
            {userEmail && (
              <>
                Welcome {userEmail}
                <Button variant="link" className="headerLink" onClick={signOut}>
                  Sign Out
                </Button>
              </>
            )}
          </Navbar.Text>
        </Navbar.Collapse>
      )}
    </Navbar>
  );
};

export default Header;
