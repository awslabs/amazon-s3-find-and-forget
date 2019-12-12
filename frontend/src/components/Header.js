import React from "react";
import { Navbar } from "react-bootstrap";

export default () => (
  <Navbar
    style={{ backgroundColor: "#232f3e", marginBottom: "20px" }}
    variant="dark"
  >
    <Navbar.Brand>
      <div className="awslogo" />
    </Navbar.Brand>
  </Navbar>
);
