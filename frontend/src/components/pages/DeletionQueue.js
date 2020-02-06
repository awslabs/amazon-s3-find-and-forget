import React, { useEffect, useState } from "react";
import { Button, Col, Form, Modal, Row, Spinner, Table } from "react-bootstrap";

import Alert from "../Alert";
import Icon from "../Icon";

import {
    formatErrorMessage, isEmpty, isUndefined, sortBy, formatDateTime
} from "../../utils";

export default ({ gateway, onPageChange }) => {
  const [deleting, setDeleting] = useState(false);
  const [errorDetails, setErrorDetails] = useState(undefined);
  const [formState, setFormState] = useState("initial");
  const [queue, setQueue] = useState([]);
  const [renderTableCount, setRenderTableCount] = useState(0);
  const [selectedRow, selectRow] = useState(undefined);

  const noSelected = isUndefined(selectedRow);

  const refreshQueue = () => {
    selectRow(undefined);
    let radios = document.getElementsByName("item");
    for (let i = 0; i < radios.length; i++) radios[i].checked = false;
    setRenderTableCount(renderTableCount + 1);
  };

  const deleteQueueMatch = async () => {
    setDeleting(false);
    setFormState("initial");
    try {
      await gateway.deleteQueueMatches([{
        MatchId: queue[selectedRow].MatchId,
        CreatedAt: queue[selectedRow].CreatedAt,
      }]);
      refreshQueue();
    } catch (e) {
      setFormState("error");
      setErrorDetails(formatErrorMessage(e));
    }
  };

  useEffect(() => {
    const fetchQueue = async () => {
      try {
        const result = await gateway.getQueue();
        setQueue(sortBy(result.MatchIds, "MatchId"));
        setFormState("list");
      } catch (e) {
        setFormState("error");
        setErrorDetails(formatErrorMessage(e));
      }
    };
    fetchQueue();
  }, [gateway, renderTableCount]);

  return (
    <div className="page-table">
      <Row>
        <Col>
          <h2>Deletion Queue ({queue.length})</h2>
        </Col>
        <Col className="buttons-right" md="auto">
          <Button className="aws-button action-button" onClick={refreshQueue}>
            <Icon type="refresh" />
          </Button>
          <Button
            className="aws-button action-button"
            {...(noSelected && { disabled: "disabled" })}
            onClick={() => setDeleting(true)}
          >
            Remove
          </Button>
          <Button
            className="aws-button action-button"
            onClick={() => onPageChange(5)}
          >
            Add Match to the Deletion Queue
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
      {!noSelected && (
        <Modal
          centered
          show={deleting}
          size="lg"
          onHide={() => setDeleting(false)}
        >
          <Modal.Header closeButton>
            <Modal.Title>
              Remove {queue[selectedRow].MatchId} from the Deletion Queue
            </Modal.Title>
          </Modal.Header>
          <Modal.Body>
            Are you sure you want to remove <i>{queue[selectedRow].MatchId}</i>{" "}
            from the Deletion Queue?
          </Modal.Body>
          <Modal.Footer>
            <Button
              className="aws-button cancel"
              onClick={() => setDeleting(false)}
            >
              Cancel
            </Button>
            <Button className="aws-button" onClick={deleteQueueMatch}>
              Remove Match from the Deletion Queue
            </Button>
          </Modal.Footer>
        </Modal>
      )}
      <Form>
        <Table>
          <thead>
            <tr>
              <td></td>
              <td>Match Id</td>
              <td>Date Added</td>
              <td>Data Mappers</td>
            </tr>
          </thead>
          <tbody>
            {queue &&
              queue.map((queueMatch, index) => (
                <tr
                  key={index}
                  className={selectedRow === index ? "selected" : undefined}
                >
                  <td style={{ textAlign: "center" }}>
                    <Form.Check
                      inline
                      type="radio"
                      id={`inline-${index}`}
                      name="item"
                      onClick={() => selectRow(index)}
                    />
                  </td>
                  <td>{queueMatch.MatchId}</td>
                  <td>{formatDateTime(queueMatch.CreatedAt)}</td>
                  <td>{queueMatch.DataMappers.join(", ") || "*"}</td>
                </tr>
              ))}
          </tbody>
        </Table>
        {isEmpty(queue) && formState !== "initial" && (
          <div className="content centered">
            <b>The Deletion Queue is empty</b>
            <p>No items to display</p>
            <Button className="aws-button" onClick={() => onPageChange(5)}>
              Add Match to the Deletion Queue
            </Button>
          </div>
        )}
      </Form>
    </div>
  );
};
