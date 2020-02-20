import React, { useCallback, useEffect, useState } from "react";
import { Button, Col, Form, Row, Spinner, Table } from "react-bootstrap";

import Alert from "../Alert";
import DetailsBox from "../DetailsBox";
import JsonModal from "../JsonModal";
import Icon from "../Icon";

import {
  formatDateTime,
  formatErrorMessage,
  formatFileSize,
  isUndefined,
  isEmpty,
  successJobClass,
  withDefault
} from "../../utils";

const COUNTDOWN_INTERVAL = 10;

export default ({ gateway, jobId }) => {
  const [countDownLeft, setCountDownLeft] = useState(COUNTDOWN_INTERVAL);
  const [deletionQueueShown, showDeletionQueue] = useState(false);
  const [errorDetails, setErrorDetails] = useState(undefined);
  const [eventsErrorDetails, setEventsErrorDetails] = useState(undefined);
  const [eventsState, setEventsState] = useState("initial");
  const [formState, setFormState] = useState("initial");
  const [job, setJob] = useState(undefined);
  const [jobEvents, setJobEvents] = useState([]);
  const [nextStart, setNextStart] = useState(false);
  const [renderTableCount, setRenderTableCount] = useState(0);
  const [selectedEvent, setSelectedEvent] = useState(undefined);

  const refreshJob = useCallback(() => {
    setCountDownLeft(COUNTDOWN_INTERVAL);
    setRenderTableCount(renderTableCount + 1);
  }, [renderTableCount, setCountDownLeft, setRenderTableCount]);

  const loadMoreEvents = useCallback(
    watermark => {
      const fetchJobEvents = async () => {
        setEventsState("loading");
        try {
          const jobEventsList = await gateway.getJobEvents(jobId, watermark);
          setJobEvents(j => j.concat(jobEventsList.JobEvents));
          setNextStart(jobEventsList.NextStart);
          setEventsState("loaded");
        } catch (e) {
          setEventsState("error");
          setEventsErrorDetails(formatErrorMessage(e));
        }
      };

      fetchJobEvents();
    },
    [gateway, jobId, setJobEvents, setNextStart]
  );

  const withCountDown =
    job &&
    ["RUNNING", "QUEUED", "FORGET_COMPLETED_CLEANUP_IN_PROGRESS"].indexOf(
      job.JobStatus
    ) !== -1;

  const errorCountClass = x =>
    x === 0 || isUndefined(x) ? "success" : "error";

  useEffect(() => {
    if (withCountDown) {
      if (countDownLeft === 0) refreshJob();
      else {
        const timer = setInterval(
          () => setCountDownLeft(countDownLeft - 1),
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

  useEffect(() => {
    loadMoreEvents();
  }, [gateway, jobId, loadMoreEvents]);

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
            <DetailsBox label="Job Id">{job.Id}</DetailsBox>
            <DetailsBox
              label="Status"
              className={`status-label ${successJobClass(job.JobStatus)}`}
            >
              <Icon type={`alert-${successJobClass(job.JobStatus)}`} />
              <span>{job.JobStatus}</span>
            </DetailsBox>
            <DetailsBox label="Deletion Queue Size" fullWidth>
              {job.DeletionQueueItems.length}{" "}
              {job.DeletionQueueItems.length > 0 && (
                <>
                  (
                  <Button
                    variant="link"
                    style={{ padding: 0 }}
                    onClick={() => showDeletionQueue(true)}
                  >
                    View Deletion Queue
                  </Button>
                  )
                </>
              )}
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
              {withDefault(
                job.TotalQueryTimeInMillis,
                x => `${(x / 1000).toFixed(0)}s`
              )}
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
      {eventsState === "error" && (
        <Alert type="error" title={eventsErrorDetails}>
          Unable to load Events
        </Alert>
      )}
      {job && (
        <>
          <JsonModal
            object={selectedEvent}
            onHide={() => setSelectedEvent(undefined)}
            show={!isEmpty(selectedEvent)}
            title={selectedEvent && `Job Event: ${selectedEvent.Sk}`}
          />
          <JsonModal
            object={job.DeletionQueueItems}
            onHide={() => showDeletionQueue(false)}
            show={deletionQueueShown}
            title={job && `Deletion Queue for job: ${job.Id}`}
          />
          <div className="page-table">
            <Row>
              <Col>
                <h2>Job Settings</h2>
              </Col>
            </Row>
            <div className="details content">
              <DetailsBox label="Safe Mode">
                {job.SafeMode.toString().toUpperCase()}
              </DetailsBox>
              <DetailsBox label="Athena Concurrency Limit">
                {job.AthenaConcurrencyLimit}
              </DetailsBox>
              <DetailsBox label="Query Execution Wait Duration (Seconds)">
                {job.QueryExecutionWaitSeconds}
              </DetailsBox>
              <DetailsBox label="Query Queue Wait Duration (Seconds)">
                {job.QueryQueueWaitSeconds}
              </DetailsBox>
              <DetailsBox label="Deletion Tasks Max Number">
                {job.DeletionTasksMaxNumber}
              </DetailsBox>
              <DetailsBox label="Forget Queue Wait Duration (Seconds)">
                {job.ForgetQueueWaitSeconds}
              </DetailsBox>
              {
                job.Expires && <DetailsBox label="Job Details Expiration">
                  {formatDateTime(job.Expires)}
                </DetailsBox>
              }
            </div>
          </div>
          <div className="page-table">
            <Row>
              <Col>
                <h2>Job Events ({jobEvents.length})</h2>
              </Col>
            </Row>
            <Form>
              <Table>
                <thead>
                  <tr>
                    <td></td>
                    <td>Event Name</td>
                    <td>Event Time</td>
                    <td>Event Emitter</td>
                    <td></td>
                  </tr>
                </thead>
                <tbody>
                  {jobEvents &&
                    jobEvents.map((e, index) => (
                      <tr key={index}>
                        <td></td>
                        <td>{withDefault(e.EventName)}</td>
                        <td>{formatDateTime(e.CreatedAt)}</td>
                        <td>{withDefault(e.EmitterId)}</td>
                        <td>
                          <Button
                            variant="link"
                            style={{ padding: 0 }}
                            onClick={() => setSelectedEvent(e)}
                          >
                            View Event
                          </Button>
                        </td>
                      </tr>
                    ))}
                </tbody>
              </Table>
              {isEmpty(jobEvents) && eventsState !== "loading" && (
                <div className="content centered">
                  <b>No events have been received yet</b>
                </div>
              )}
            </Form>
          </div>
        </>
      )}
      <div className="centered">
        {eventsState === "loading" ? (
          <Spinner animation="border" role="status" className="spinner" />
        ) : nextStart ? (
          <Button
            disabled={eventsState === "loading"}
            className="aws-button action-button"
            onClick={() => loadMoreEvents(nextStart)}
          >
            Load More
          </Button>
        ) : (
          <p>All events loaded</p>
        )}
      </div>
    </>
  );
};
