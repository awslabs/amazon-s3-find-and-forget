import React, { useState, useEffect } from "react";
import { Button, Col, Form, Row, Spinner, Table } from "react-bootstrap";

import Alert from "../Alert";
import Icon from "../Icon";
import StartDeletionJob from "../StartDeletionJob";
import TablePagination from "../TablePagination";

import {
  formatDateTime,
  formatErrorMessage,
  isEmpty,
  successJobClass,
  withDefault,
} from "../../utils";

const PAGE_SIZE = 10;

const DeletionJobs = ({ gateway, goToJobDetails }) => {
  const [currentPage, setCurrentPage] = useState(0);
  const [errorDetails, setErrorDetails] = useState(undefined);
  const [formState, setFormState] = useState("initial");
  const [jobs, setJobs] = useState([]);
  const [renderTableCount, setRenderTableCount] = useState(0);

  const refreshJobs = () => setRenderTableCount(renderTableCount + 1);
  const pages = Math.ceil(jobs.length / PAGE_SIZE);

  useEffect(() => {
    const fetchJobs = async () => {
      setFormState("initial");
      try {
        const jobs = await gateway.getJobs();
        setJobs(jobs.Jobs);
        setFormState("list");
      } catch (e) {
        setFormState("error");
        setErrorDetails(formatErrorMessage(e));
      }
    };
    fetchJobs();
  }, [gateway, renderTableCount]);

  const shouldShowItem = (index) =>
    index >= PAGE_SIZE * currentPage && index < PAGE_SIZE * (currentPage + 1);

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
      <Row className="pagination">
        <Col></Col>
        <Col className="buttons-right" md="auto">
          <TablePagination onPageChange={setCurrentPage} pages={pages} />
        </Col>
      </Row>
      {formState === "initial" && (
        <Spinner animation="border" role="status" className="spinner" />
      )}
      {formState === "error" && (
        <Alert type="error" title="An Error Occurred">
          {errorDetails}
        </Alert>
      )}
      <Form>
        <Table>
          <thead>
            <tr>
              <td></td>
              <td>Job Id</td>
              <td>Status</td>
              <td>Updates</td>
              <td>Start Time</td>
              <td>Finish Time</td>
            </tr>
          </thead>
          <tbody>
            {jobs &&
              jobs.map(
                (job, index) =>
                  shouldShowItem(index) && (
                    <tr key={index}>
                      <td></td>
                      <td>
                        <Button
                          variant="link"
                          style={{ paddingLeft: 0 }}
                          onClick={() => goToJobDetails(job.Id)}
                        >
                          {job.Id}
                        </Button>
                      </td>
                      <td
                        className={`status-label ${successJobClass(
                          job.JobStatus
                        )}`}
                      >
                        <Icon
                          type={`alert-${successJobClass(job.JobStatus)}`}
                        />
                        <span>{job.JobStatus}</span>
                      </td>
                      <td>{withDefault(job.TotalObjectUpdatedCount)}</td>
                      <td>{formatDateTime(job.JobStartTime)}</td>
                      <td>{formatDateTime(job.JobFinishTime)}</td>
                    </tr>
                  )
              )}
          </tbody>
        </Table>
        {isEmpty(jobs) && formState !== "initial" && (
          <div className="content centered">
            <b>The Deletion Jobs list is empty</b>
            <p>No items to display</p>
            <StartDeletionJob
              gateway={gateway}
              goToJobDetails={goToJobDetails}
            />
          </div>
        )}
      </Form>
    </div>
  );
};

export default DeletionJobs;
