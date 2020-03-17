import React from "react";
import { Button } from "react-bootstrap";

import { isUndefined } from "../utils";

import "./MetricsDashboard.css";

export default ({ title, description, goToPage, metrics }) => {
  const openLink = link => {
    if (!isUndefined(link)) {
      const isString = typeof link === "string";
      return isString ? window.open(link, "_new") : goToPage(link);
    }
  };

  return (
    <div className="page-table form-container metrics-dashboard">
      <h2>{title}</h2>
      <p className="header-description">{description}</p>
      <div className="content">
        {metrics.map((metric, index) => (
          <div key={index} className="metric-container">
            <p className="metric-label">{metric.title}</p>
            <Button onClick={() => openLink(metric.link)} variant="link">
              {metric.value}
            </Button>
          </div>
        ))}
      </div>
    </div>
  );
};
