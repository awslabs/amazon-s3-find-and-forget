import React from "react";
import { Button, Col, Row } from "react-bootstrap";

export default ({ onStartDeletionJobClick }) => (
  <>
    <Row>
      <Col>
        <h1>Dashboard</h1>
      </Col>
      <Col className="buttons-right">
        <Button
          className="aws-button home-button"
          onClick={onStartDeletionJobClick}
        >
          Start a Deletion Job
        </Button>
      </Col>
    </Row>

    <p>
      This page will contain a summary of the current deletion queue and the
      last 10 jobs.
    </p>
  </>
);
