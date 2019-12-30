import React, { useCallback, useEffect, useState } from "react";
import { Button, Col, Row, Spinner } from "react-bootstrap";

import Alert from "../Alert";
import DetailsBox from "../DetailsBox";
import Icon from "../Icon";

import {
  formatDateTime,
  formatErrorMessage,
  formatFileSize,
  isEmpty,
  isUndefined
} from "../../utils";

const withDefault = (x, formatter = () => x) =>
  isEmpty(x) ? "-" : formatter(x);

const COUNTDOWN_INTERVAL = 10;

export default ({ gateway, jobId }) => {
  const [countDownLeft, setCoundDownLeft] = useState(COUNTDOWN_INTERVAL);
  const [errorDetails, setErrorDetails] = useState(undefined);
  const [formState, setFormState] = useState("initial");
  const [job, setJob] = useState(undefined);
  const [renderTableCount, setRenderTableCount] = useState(0);

  const refreshJob = useCallback(() => {
    setCoundDownLeft(COUNTDOWN_INTERVAL);
    setRenderTableCount(renderTableCount + 1);
  }, [renderTableCount, setCoundDownLeft, setRenderTableCount]);

  const withCountDown = job && job.JobStats === "RUNNING";

  const successJobClass =
    job && job.JobStatus === "COMPLETED"
      ? "success"
      : job && job.jobStatus === "RUNNING"
      ? "info"
      : "error";

  const errorCountClass = x =>
    x === 0 || isUndefined(x) ? "success" : "error";

  useEffect(() => {
    if (withCountDown) {
      if (countDownLeft === 0) refreshJob();
      else {
        const timer = setInterval(
          () => setCoundDownLeft(countDownLeft - 1),
          1000
        );
        return () => clearInterval(timer);
      }
    }
  }, [countDownLeft, refreshJob, withCountDown]);

  useEffect(() => {
    const fetchJob = async () => {
      setFormState("initial");
      try {
        const jobDetails = await gateway.getJob(jobId);
        setJob(jobDetails);
        setFormState("details");
      } catch (e) {
        setFormState("error");
        setErrorDetails(formatErrorMessage(e));
      }
    };

    fetchJob();
  }, [gateway, jobId, renderTableCount]);

  return (
    <div className="page-table">
      <Row style={{ borderBottom: "1px solid #eaeded" }}>
        <Col>
          <h2>Job Overview</h2>
        </Col>
        <Col className="buttons-right" md="auto">
          <Button className="aws-button action-button" onClick={refreshJob}>
            <Icon type="refresh" />
            {withCountDown && (
              <span style={{ marginLeft: "10px" }}>
                Refreshing in {countDownLeft}s...
              </span>
            )}
          </Button>
        </Col>
      </Row>
      {formState === "initial" && (
        <Spinner animation="border" role="status" className="spinner" />
      )}
      {formState === "error" && (
        <Alert type="error" title={errorDetails}>
          Please retry later.
        </Alert>
      )}
      {formState === "details" && (
        <div className="details content">
          <DetailsBox label="Job Id">{job.JobId}</DetailsBox>
          <DetailsBox
            label="Status"
            className={`status-label ${successJobClass}`}
          >
            <Icon type={`alert-${successJobClass}`} />
            <span>{job.JobStatus}</span>
          </DetailsBox>
          <DetailsBox label="Start Time">
            {formatDateTime(job.JobStartTime)}
          </DetailsBox>
          <DetailsBox label="Finish Time">
            {job.JobFinishTime ? formatDateTime(job.JobFinishTime) : "-"}
          </DetailsBox>
          <DetailsBox label="Total Query Count" noSeparator>
            {withDefault(job.TotalQueryCount)}
          </DetailsBox>
          <DetailsBox
            label="Total Query Failed Count"
            className={`status-label ${errorCountClass(
              job.TotalQueryFailedCount
            )}`}
            noSeparator
          >
            {!isUndefined(job.TotalQueryFailedCoun) && (
              <Icon
                type={`alert-${errorCountClass(job.TotalQueryFailedCount)}`}
              />
            )}
            <span>{withDefault(job.TotalQueryFailedCount)}</span>
          </DetailsBox>
          <DetailsBox label="Total Query Time">
            {withDefault(job.TotalQueryCount, x => `${x}s`)}
          </DetailsBox>
          <DetailsBox label="Total Query Scanned Bytes">
            {withDefault(
              job.TotalQueryScannedInBytes,
              x => `${x} ${x > 0 ? "(" + formatFileSize(x) + ")" : ""}`
            )}
          </DetailsBox>
          <DetailsBox label="Total Object Updated Count" noSeparator>
            {withDefault(job.TotalObjectUpdatedCount)}
          </DetailsBox>
          <DetailsBox
            label="Total Object Update Failed Count"
            className={`status-label ${errorCountClass(
              job.TotalObjectUpdateFailedCount
            )}`}
            noSeparator
          >
            {!isUndefined(job.TotalObjectUpdateFailedCount) && (
              <Icon
                type={`alert-${errorCountClass(
                  job.TotalObjectUpdateFailedCount
                )}`}
              />
            )}
            <span>{withDefault(job.TotalObjectUpdateFailedCount)}</span>
          </DetailsBox>
        </div>
      )}
    </div>
  );
};
