import { Auth } from "aws-amplify";
import React, { useEffect, useState } from "react";
import { Button, Navbar } from "react-bootstrap";

export default ({ signedIn }) => {
  const [userEmail, setUserEmail] = useState(undefined);

  const signOut = () => {
    Auth.signOut().then(() => window.location.reload());
  };

  useEffect(() => {
    if (signedIn) {
      Auth.currentAuthenticatedUser()
        .then(user => setUserEmail(user.username))
        .catch(() => {});
    }
  }, [signedIn]);

  return (
    <Navbar style={{ backgroundColor: "#232f3e" }} variant="dark">
      <Navbar.Brand>
        <div className="awslogo" />
      </Navbar.Brand>
      <Navbar.Toggle />
      {userEmail && (
        <Navbar.Collapse className="justify-content-end">
          <Navbar.Text>
            Welcome {userEmail}
            <Button variant="link" className="headerLink" onClick={signOut}>
              Sign Out
            </Button>
          </Navbar.Text>
        </Navbar.Collapse>
      )}
    </Navbar>
  );
};
