import React from "react";
import { Alert as BootstrapAlert } from "react-bootstrap";

import Icon from "./Icon";

const Alert = ({ children, title, type }) => (
  <BootstrapAlert variant={type === "error" ? "danger" : "info"}>
    <span className="icon">
      <Icon type={`alert-${type}`} size="32" />
    </span>
    <div className="alert-content">
      <BootstrapAlert.Heading>{title}</BootstrapAlert.Heading>
      <p>{children}</p>
    </div>
  </BootstrapAlert>
);

export default Alert;
