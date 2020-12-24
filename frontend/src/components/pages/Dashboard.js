import React, { useEffect, useState } from "react";
import { Col, Row, Spinner } from "react-bootstrap";

import Alert from "../Alert";
import MetricsDashboard from "../MetricsDashboard";
import StartDeletionJob from "../StartDeletionJob";

import {
  daysSinceDateTime,
  findMin,
  formatErrorMessage,
  formatFileSize,
  repoUrl,
} from "../../utils";

const { region, version } = window.s3f2Settings;

const Dashboard = ({ gateway, goToJobDetails, goToPage }) => {
  const [errorDetails, setErrorDetails] = useState(undefined);
  const [formState, setFormState] = useState("initial");
  const [metrics, setMetrics] = useState([]);

  useEffect(() => {
    const fetchDashboardData = async () => {
      try {
        const [queue, jobs] = await Promise.all([
          gateway.getQueue(),
          gateway.getLastJob(),
        ]);

        const anyJob = jobs.Jobs.length > 0;
        const anyQueueItem = queue.MatchIds.length > 0;
        const daysSinceLastRun = anyJob
          ? daysSinceDateTime(jobs.Jobs[0].JobFinishTime)
          : "n/a";

        let deletionQueueSummary = queue.MatchIds.length;
        if (anyQueueItem)
          deletionQueueSummary += ` (≈${formatFileSize(queue.ContentLength)})`;

        const daysSinceOldestQueueItemAdded = anyQueueItem
          ? daysSinceDateTime(findMin(queue.MatchIds, "CreatedAt").CreatedAt)
          : "n/a";

        setMetrics([
          {
            title: "Deletion Queue size",
            value: deletionQueueSummary,
            link: 2,
          },
          {
            title: "Days since oldest Queue Item added",
            value: daysSinceOldestQueueItemAdded,
            link: 3,
          },
          {
            title: "Days since last job run",
            value: daysSinceLastRun,
            link: 3,
          },
          {
            title: "Solution Version",
            value: version,
            link: repoUrl("blob/master/CHANGELOG.md"),
          },
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

export default Dashboard;
