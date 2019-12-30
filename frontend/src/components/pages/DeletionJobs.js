import React, { useState, useEffect } from "react";
import { Button, Col, Form, Row, Spinner, Table } from "react-bootstrap";

import Alert from "../Alert";
import Icon from "../Icon";
import StartDeletionJob from "../StartDeletionJob";

import { formatDateTime, formatErrorMessage } from "../../utils";

export default ({ gateway, goToJobDetails }) => {
  const [errorDetails, setErrorDetails] = useState(undefined);
  const [formState, setFormState] = useState("initial");
  const [jobs, setJobs] = useState([]);
  const [renderTableCount, setRenderTableCount] = useState(0);

  const refreshJobs = () => setRenderTableCount(renderTableCount + 1);

  const successJobClass = job =>
    job.JobStatus === "COMPLETED"
      ? "success"
      : job.jobStatus === "RUNNING"
      ? "info"
      : "error";

  useEffect(() => {
    const fetchJobs = async () => {
      setFormState("initial");
      try {
        const allJobs = [];
        const pageSize = undefined;
        let watermark = undefined;
        while (true) {
          const page = await gateway.getJobs(pageSize, watermark);
          allJobs.push(...page.Jobs);
          if (page.NextStart) watermark = page.NextStart;
          else break;
        }
        setJobs(allJobs);
        setFormState("list");
      } catch (e) {
        setFormState("error");
        setErrorDetails(formatErrorMessage(e));
      }
    };
    fetchJobs();
  }, [gateway, renderTableCount]);

  return (
    <div className="page-table">
      <Row>
        <Col>
          <h2>Deletion Jobs ({jobs.length})</h2>
        </Col>
        <Col className="buttons-right" md="auto">
          <Button className="aws-button action-button" onClick={refreshJobs}>
            <Icon type="refresh" />
          </Button>
          <StartDeletionJob
            className="action-button-orange"
            gateway={gateway}
            goToJobDetails={goToJobDetails}
          />
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
      <Form>
        <Table>
          <thead>
            <tr>
              <td></td>
              <td>Job Id</td>
              <td>Status</td>
              <td>Start Time</td>
              <td>Finish Time</td>
            </tr>
          </thead>
          <tbody>
            {jobs &&
              jobs.map((job, index) => (
                <tr key={index}>
                  <td></td>
                  <td>
                    <Button
                      variant="link"
                      style={{ paddingLeft: 0 }}
                      onClick={() => goToJobDetails(job.JobId)}
                    >
                      {job.JobId}
                    </Button>
                  </td>
                  <td className={`status-label ${successJobClass(job)}`}>
                    <Icon type={`alert-${successJobClass(job)}`} />
                    <span>{job.JobStatus}</span>
                  </td>

                  <td>{formatDateTime(job.JobStartTime)}</td>
                  <td>{formatDateTime(job.JobFinishTime)}</td>
                </tr>
              ))}
          </tbody>
        </Table>
        {jobs && jobs.length === 0 && formState !== "initial" && (
          <div className="content centered">
            <b>The Deletion Jobs list is empty</b>
            <p>No items to display</p>
            <Button
              className="aws-button"
              // onClick={() => onPageChange(5)}
            >
              Start a Deletion Job
            </Button>
          </div>
        )}
      </Form>
    </div>
  );
};
