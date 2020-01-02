import React, { useEffect, useState } from "react";
import { Col, Row, Spinner } from "react-bootstrap";

import Alert from "../Alert";
import MetricsDashboard from "../MetricsDashboard";
import StartDeletionJob from "../StartDeletionJob";

import { daysSinceDateTime, formatErrorMessage } from "../../utils";

const { region, version } = window.s3f2Settings;

export default ({ gateway, goToJobDetails, goToPage }) => {
  const [errorDetails, setErrorDetails] = useState(undefined);
  const [formState, setFormState] = useState("initial");
  const [metrics, setMetrics] = useState([]);

  useEffect(() => {
    const fetchDashboardData = async () => {
      try {
        const [queue, jobs, dataMappers] = await Promise.all([
          gateway.getQueue(),
          gateway.getLastJob(),
          gateway.getDataMappers()
        ]);

        const anyJob = jobs.Jobs.length > 0;
        const daysSinceLastRun = anyJob
          ? daysSinceDateTime(jobs.Jobs[0].JobFinishTime)
          : "∞";

        setMetrics([
          {
            title: "Deletion Queue size",
            value: queue.MatchIds.length,
            link: 2
          },
          {
            title: "Days since last job run",
            value: daysSinceLastRun,
            link: 3
          },
          {
            title: "Data Mappers",
            value: dataMappers.DataMappers.length,
            link: 1
          },
          { title: "Solution Version", value: version }
        ]);
        setFormState("data-loaded");
      } catch (e) {
        setFormState("error");
        setErrorDetails(formatErrorMessage(e));
      }
    };

    fetchDashboardData();
  }, [gateway]);

  return (
    <>
      <Row>
        <Col>
          <h1>Dashboard</h1>
        </Col>
        <Col className="buttons-right">
          <StartDeletionJob
            className="home-button"
            gateway={gateway}
            goToJobDetails={goToJobDetails}
          />
        </Col>
      </Row>
      {formState === "error" && (
        <div className="form-container">
          <Alert type="error" title="An error happened">
            {errorDetails}
          </Alert>
        </div>
      )}
      {formState === "initial" && (
        <Spinner animation="border" role="status" className="spinner" />
      )}
      {formState === "data-loaded" && (
        <MetricsDashboard
          title="Service overview"
          description={`Viewing data from ${region} region`}
          goToPage={goToPage}
          metrics={metrics}
        />
      )}
    </>
  );
};
