import React from "react";

const SVGWrapper = ({ children }) => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    viewBox="0 0 16 16"
    focusable="false"
    aria-hidden="true"
    width="16"
    height="16"
  >
    {children}
  </svg>
);

export default ({ type }) => {
  if (type === "alert-error")
    return (
      <SVGWrapper>
        <circle
          className="stroke-linejoin-round"
          cx="8"
          cy="8"
          r="7"
          stroke="#d13212"
          fill="none"
          strokeWidth="2px"
        />
        <path
          d="M10.828 5.172l-5.656 5.656M10.828 10.828L5.172 5.172"
          stroke="#d13212"
          fill="none"
          strokeWidth="2px"
        />
      </SVGWrapper>
    );

  if (type === "alert-warning")
    return (
      <SVGWrapper>
        <circle
          className="stroke-linejoin-round"
          cx="8"
          cy="8"
          r="7"
          stroke="#0073bb"
          fill="none"
          strokeWidth="2px"
        />
        <path d="M8 11V8H6" stroke="#0073bb" fill="none" strokeWidth="2px" />
        <path
          className="stroke-linejoin-round"
          d="M10 11H6"
          stroke="#0073bb"
          fill="none"
          strokeWidth="2px"
        />
        <path
          d="M7.99 5H8v.01h-.01z"
          stroke="#0073bb"
          fill="none"
          strokeWidth="2px"
        />
      </SVGWrapper>
    );

  if (type === "breadcrumb")
    return (
      <SVGWrapper>
        <path d="M4 1l7 7-7 7" stroke="#545b64" fill="none" />
      </SVGWrapper>
    );

  if (type === "close")
    return (
      <SVGWrapper>
        <path d="M2 2l12 12M14 2L2 14" stroke="#545b64" strokeWidth="2px" />
      </SVGWrapper>
    );

  if (type === "hamburger")
    return (
      <SVGWrapper>
        <path d="M15 8H1M15 3H1M15 13H1" stroke="#545b64" strokeWidth="2px" />
      </SVGWrapper>
    );

  if (type === "info")
    return (
      <SVGWrapper>
        <circle
          className="stroke-linejoin-round"
          cx="8"
          cy="8"
          r="7"
          stroke="#545b64"
          strokeWidth="2px"
        />
        <path d="M8 11V8H6" stroke="#545b64" strokeWidth="2px" />
        <path
          className="stroke-linejoin-round"
          d="M10 11H6"
          stroke="#545b64"
          strokeWidth="2px"
        />
        <path d="M7.99 5H8v.01h-.01z" stroke="#545b64" strokeWidth="2px" />
      </SVGWrapper>
    );

  if (type === "new-window")
    return (
      <SVGWrapper>
        <path
          className="stroke-linecap-square"
          d="M10 2h4v4"
          stroke="#545b64"
          fill="none"
          strokeWidth="2px"
        />
        <path d="M6 10l8-8" stroke="#545b64" fill="none" strokeWidth="2px" />
        <path
          className="stroke-linejoin-round"
          d="M14 9.048V14H2V2h5"
          stroke="#545b64"
          fill="none"
          strokeWidth="2px"
        />
      </SVGWrapper>
    );
  return "";
};
