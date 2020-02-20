import React, { useState } from "react";
import { Button, Modal, Spinner } from "react-bootstrap";

import Alert from "./Alert";
import DetailsBox from "./DetailsBox";

import { formatErrorMessage } from "../utils";

export default ({ className, gateway, goToJobDetails }) => {
  const [errorDetails, setErrorDetails] = useState(undefined);
  const [formState, setFormState] = useState("initial");
  const [summary, setSummary] = useState(undefined);
  const [starting, setStarting] = useState(false);

  const open = async () => {
    setFormState("wait");
    setStarting(true);
    try {
      const [{Settings}, {MatchIds}, {DataMappers}] = await Promise.all([
        gateway.getSettings(),
        gateway.getQueue(),
        gateway.getDataMappers()
      ]);

      setSummary({
        "Deletion Queue Size": MatchIds.length,
        "Data Mappers Count": DataMappers.length,
        "Safe Mode": Settings.SafeMode.toString().toUpperCase(),
        "Job Details Retention (Days)": Settings.JobDetailsRetentionDays,
        "Athena Concurrency Limit": Settings.AthenaConcurrencyLimit,
        "Query Execution Wait Duration (Seconds)":
          Settings.QueryExecutionWaitSeconds,
        "Query Queue Wait Duration (Seconds)":
          Settings.QueryQueueWaitSeconds,
        "Deletion Tasks Max Number":
          Settings.DeletionTasksMaxNumber,
        "Forget Queue Wait Duration (Seconds)":
          Settings.ForgetQueueWaitSeconds
      });

      setFormState("confirm");
    } catch (e) {
      setFormState("error");
      setErrorDetails(formatErrorMessage(e));
    }
  };

  const close = () => {
    setFormState("initial");
    setStarting(false);
  };

  const startJob = async () => {
    setFormState("wait");
    try {
      const job = await gateway.processQueue();
      setFormState("initial");
      goToJobDetails(job.Id);
    } catch (e) {
      setFormState("error");
      setErrorDetails(formatErrorMessage(e));
    }
  };

  return (
    <>
      <Button className={`aws-button ${className}`} onClick={open}>
        Start a Deletion Job
      </Button>
      <Modal centered show={starting} size="lg" onHide={close}>
        <Modal.Header closeButton>
          <Modal.Title>Start a Deletion Job</Modal.Title>
        </Modal.Header>
        <Modal.Body>
          {formState === "confirm" && (
            <>
              <Alert type="info">
                Are you sure you want to start a new Deletion Job with the
                current configuration?
              </Alert>
              <div className="details content">
                {Object.keys(summary).map((item, index) => (
                  <DetailsBox label={item} key={index}>
                    {summary[item]}
                  </DetailsBox>
                ))}
              </div>
            </>
          )}
          {formState === "wait" && (
            <Spinner animation="border" role="status" className="spinner" />
          )}
          {formState === "error" && (
            <Alert type="error" title={errorDetails}>
              Please retry later.
            </Alert>
          )}
        </Modal.Body>
        {formState === "confirm" && (
          <Modal.Footer>
            <Button className="aws-button cancel" onClick={close}>
              Cancel
            </Button>
            <Button className="aws-button" onClick={startJob}>
              Start a Deletion Job
            </Button>
          </Modal.Footer>
        )}
      </Modal>
    </>
  );
};
