import React from "react";
import { Alert } from "react-bootstrap";

import Icon from "./Icon";

export default ({ children, title, type }) => (
  <Alert variant={type === "error" ? "danger" : "info"}>
    <span className="icon">
      <Icon type={`alert-${type}`} size="32" />
    </span>
    <div className="alert-content">
      <Alert.Heading>{title}</Alert.Heading>
      <p>{children}</p>
    </div>
  </Alert>
);
