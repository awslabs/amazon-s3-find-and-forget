import React, { useState } from "react";
import { Button, Modal, Spinner } from "react-bootstrap";

import Alert from "./Alert";

import { formatErrorMessage } from "../utils";

export default ({ className, gateway, goToJobDetails }) => {
  const [errorDetails, setErrorDetails] = useState(undefined);
  const [formState, setFormState] = useState("initial");
  const [starting, setStarting] = useState(false);

  const close = () => {
    setFormState("initial");
    setStarting(false);
  };

  const startJob = async () => {
    setFormState("starting");
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
      <Button
        className={`aws-button ${className}`}
        onClick={() => setStarting(true)}
      >
        Start a Deletion Job
      </Button>
      <Modal centered show={starting} size="lg" onHide={close}>
        <Modal.Header closeButton>
          <Modal.Title>Start a Deletion Job</Modal.Title>
        </Modal.Header>
        <Modal.Body>
          {formState === "initial" && (
            <>Are you sure you want to start a new Deletion Job?</>
          )}
          {formState === "starting" && (
            <Spinner animation="border" role="status" className="spinner" />
          )}
          {formState === "error" && (
            <Alert type="error" title={errorDetails}>
              Please retry later.
            </Alert>
          )}
        </Modal.Body>
        {formState === "initial" && (
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
