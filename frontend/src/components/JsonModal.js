import React from "react";
import ReactJson from "react-json-view";
import { Button, Modal } from "react-bootstrap";

const JsonModal = ({ object, onHide, show, title }) => (
  <Modal centered show={show} size="lg" onHide={onHide}>
    <Modal.Header closeButton>
      <Modal.Title>{title}</Modal.Title>
    </Modal.Header>
    <Modal.Body>
      <ReactJson
        displayDataTypes={false}
        indentWidth={2}
        name={false}
        src={object}
      />
    </Modal.Body>
    <Modal.Footer>
      <Button className="aws-button cancel" onClick={onHide}>
        Close
      </Button>
    </Modal.Footer>
  </Modal>
);

export default JsonModal;
