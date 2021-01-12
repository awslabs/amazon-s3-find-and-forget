import React from "react";

import Icon from "./Icon";

const CellError = ({ error }) => (
  <>
    <Icon type="alert-warning" />
    <span style={{ marginLeft: "4px", color: "#d13212" }}>{error}</span>
  </>
);

export default CellError;
