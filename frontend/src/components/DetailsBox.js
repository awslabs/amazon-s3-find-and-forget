import React from "react";

const DetailsBox = ({
  children,
  className = "value",
  fullWidth = false,
  label,
  noSeparator = false
}) => (
  <div
    className={`details-box${noSeparator ? "" : " with-border-bottom"}${
      fullWidth ? " w100" : ""
    }`}
  >
    <div className="label">{label}</div>
    <div className={className}>{children}</div>
  </div>
);

export default DetailsBox;
