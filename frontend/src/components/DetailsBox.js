import React from "react";

export default ({
  children,
  className = "value",
  label,
  noSeparator = false
}) => (
  <div className={`details-box${noSeparator ? "" : " with-border-bottom"}`}>
    <div className="label">{label}</div>
    <div className={className}>{children}</div>
  </div>
);
