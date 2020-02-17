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
      const [settings, queue, dataMappers] = await Promise.all([
        gateway.getSettings(),
        gateway.getQueue(),
        gateway.getDataMappers()
      ]);

      setSummary({
        "Matches in the Deletion Queue": queue.MatchIds.length,
        "Data Mappers Count": dataMappers.DataMappers.length,
        "Safe Mode": settings.Settings.SafeMode.toString().toUpperCase(),
        "Athena Concurrency Limit": settings.Settings.AthenaConcurrencyLimit,
        "Wait Duration Query Execution":
          settings.Settings.WaitDurationQueryExecution,
        "Wait Duration Query Queue": settings.Settings.WaitDurationQueryQueue,
        "Deletion Tasks Max Number": settings.Settings.DeletionTasksMaxNumber,
        "Wait Duration Forget Queue": settings.Settings.WaitDurationForgetQueue
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
