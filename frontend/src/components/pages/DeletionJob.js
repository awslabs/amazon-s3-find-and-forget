import React, { useCallback, useEffect, useState } from "react";
import ReactJson from "react-json-view";
import { Button, Col, Row, Spinner } from "react-bootstrap";

import Alert from "../Alert";
import DetailsBox from "../DetailsBox";
import Icon from "../Icon";

import {
  formatDateTime,
  formatErrorMessage,
  formatFileSize,
  isUndefined,
  withDefault,
  successJobClass
} from "../../utils";

const COUNTDOWN_INTERVAL = 10;

export default ({ gateway, jobId }) => {
  const [countDownLeft, setCoundDownLeft] = useState(COUNTDOWN_INTERVAL);
  const [errorDetails, setErrorDetails] = useState(undefined);
  const [formState, setFormState] = useState("initial");
  const [job, setJob] = useState(undefined);
  const [renderTableCount, setRenderTableCount] = useState(0);
  const [report, setReport] = useState(undefined);
  const [reportShown, showReport] = useState(false);

  const refreshJob = useCallback(() => {
    setCoundDownLeft(COUNTDOWN_INTERVAL);
    setRenderTableCount(renderTableCount + 1);
  }, [renderTableCount, setCoundDownLeft, setRenderTableCount]);

  const withCountDown =
    job && (job.JobStatus === "RUNNING" || job.JobStatus === "QUEUED");

  const errorCountClass = x =>
    x === 0 || isUndefined(x) ? "success" : "error";

  const loadReport = async () => {
    if (!report) {
      setFormState("loading-report");
      try {
        const reportContent = await gateway.getObject(job.JobReportLocation);
        setReport(reportContent);
        setFormState("details");
      } catch (e) {
        setFormState("error");
        setErrorDetails(formatErrorMessage(e));
      }
    }
  };

  const openReport = async () => {
    await loadReport();
    showReport(true);
  };

  const downloadReport = async () => {
    await loadReport();
    const el = document.getElementById("download-report-link");
    if (el) el.click();
  };

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
    <>
      <div className="page-table">
        <Row className="header">
          <Col>
            <h2>Job Overview</h2>
          </Col>
          <Col className="buttons-right" md="auto">
            {withCountDown && (
              <Button className="aws-button action-button" onClick={refreshJob}>
                <Icon type="refresh" />
                <span className="refresh-counter">
                  Refreshing in {countDownLeft}s...
                </span>
              </Button>
            )}
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
        {job && (
          <div className="details content">
            <DetailsBox label="Job Id">{job.JobId}</DetailsBox>
            <DetailsBox
              label="Status"
              className={`status-label ${successJobClass(job.JobStatus)}`}
            >
              <Icon type={`alert-${successJobClass(job.JobStatus)}`} />
              <span>{job.JobStatus}</span>
            </DetailsBox>
            <DetailsBox label="Start Time">
              {formatDateTime(job.JobStartTime)}
            </DetailsBox>
            <DetailsBox label="Finish Time">
              {formatDateTime(job.JobFinishTime)}
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
              {!isUndefined(job.TotalQueryFailedCount) && (
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
      {job && job.JobReportLocation && (
        <div className="page-table">
          <Row className="header">
            <Col>
              <h2>Job Report</h2>
            </Col>
            <Col className="buttons-right" md="auto">
              {!reportShown && (
                <Button
                  className="aws-button action-button"
                  onClick={openReport}
                >
                  Open Report
                </Button>
              )}
              <Button
                className="aws-button action-button"
                onClick={downloadReport}
              >
                Download Report
              </Button>
              <a
                href={`data:text/json;charset=utf-8,${encodeURIComponent(
                  JSON.stringify(report)
                )}`}
                id="download-report-link"
                className="hide"
                download={`${job.JobId}.json`}
              >
                Download link
              </a>
            </Col>
          </Row>
          <div className="details content">
            <DetailsBox label="Report Location" fullWidth>
              {job.JobReportLocation}
            </DetailsBox>
            {formState === "loading-report" && (
              <Spinner animation="border" role="status" className="spinner" />
            )}
            {report && reportShown && (
              <DetailsBox fullWidth className="json-visualiser">
                <ReactJson
                  displayDataTypes={false}
                  indentWidth={2}
                  name={false}
                  src={report}
                />
              </DetailsBox>
            )}
          </div>
        </div>
      )}
    </>
  );
};
