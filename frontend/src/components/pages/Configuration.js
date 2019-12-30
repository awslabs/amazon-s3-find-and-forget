import React, { useEffect, useState } from "react";
import { Button, Col, Form, Modal, Row, Spinner, Table } from "react-bootstrap";

import Alert from "../Alert";
import Icon from "../Icon";

import { formatErrorMessage, sortBy } from "../../utils";

export default ({ gateway, onPageChange }) => {
  const [dataMappers, setDataMappers] = useState([]);
  const [deleting, setDeleting] = useState(false);
  const [errorDetails, setErrorDetails] = useState(undefined);
  const [formState, setFormState] = useState("initial");
  const [renderTableCount, setRenderTableCount] = useState(0);
  const [selectedRow, selectRow] = useState(undefined);
  const [showingBucketPolicy, showBucketPolicy] = useState(false);

  const noSelected = typeof selectedRow === "undefined";

  const refreshDataMappers = () => {
    selectRow(undefined);
    let radios = document.getElementsByName("item");
    for (let i = 0; i < radios.length; i++) radios[i].checked = false;
    setRenderTableCount(renderTableCount + 1);
  };

  const deleteDataMapper = async () => {
    setDeleting(false);
    setFormState("initial");
    try {
      await gateway.deleteDataMapper(dataMappers[selectedRow].DataMapperId);
      refreshDataMappers();
    } catch (e) {
      setFormState("error");
      setErrorDetails(formatErrorMessage(e));
    }
  };

  useEffect(() => {
    const fetchDataMappers = async () => {
      try {
        const result = await gateway.getDataMappers();
        setDataMappers(sortBy(result.DataMappers, "DataMapperId"));
        setFormState("list");
      } catch (e) {
        setFormState("error");
        setErrorDetails(formatErrorMessage(e));
      }
    };
    fetchDataMappers();
  }, [gateway, renderTableCount]);

  return (
    <div className="page-table">
      <Row>
        <Col>
          <h2>Data Mappers ({dataMappers.length})</h2>
        </Col>
        <Col className="buttons-right" md="auto">
          <Button
            className="aws-button action-button"
            onClick={refreshDataMappers}
          >
            <Icon type="refresh" />
          </Button>
          <Button
            className="aws-button action-button"
            onClick={() => showBucketPolicy(true)}
          >
            View S3 Bucket Policy
          </Button>
          <Button
            className="aws-button action-button"
            {...(noSelected && { disabled: "disabled" })}
            onClick={() => setDeleting(true)}
          >
            Delete
          </Button>
          <Button
            className="aws-button action-button"
            onClick={() => onPageChange(4)}
          >
            Create Data Mapper
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
              Delete {dataMappers[selectedRow].DataMapperId}
            </Modal.Title>
          </Modal.Header>
          <Modal.Body>
            Are you sure you want to delete{" "}
            <i>{dataMappers[selectedRow].DataMapperId}</i>?
          </Modal.Body>
          <Modal.Footer>
            <Button
              className="aws-button cancel"
              onClick={() => setDeleting(false)}
            >
              Cancel
            </Button>
            <Button className="aws-button" onClick={deleteDataMapper}>
              Delete Data Mapper
            </Button>
          </Modal.Footer>
        </Modal>
      )}
      <Modal
        centered
        show={showingBucketPolicy}
        size="lg"
        onHide={() => showBucketPolicy(false)}
      >
        <Modal.Header closeButton>
          <Modal.Title>S3 Bucket Policy</Modal.Title>
        </Modal.Header>
        <Modal.Body>
          <p>
            After configuring the data mappers, you need to configure the
            relevant S3 bucket policies to enable the solution to write and read
            from/to them.
          </p>
          <p>
            1. Retrieve the Athena Query Executor (<b>AthenaExecutionRoleArn</b>
            ) and Delete Task (<b>DeleteTaskIAMRoleArn</b>) IAM roles from the
            Solution CloudFormation Stack Output.
          </p>
          <p>
            2. Find the relevant Bucket in the S3 AWS Web Console and{" "}
            <a href="https://s3.console.aws.amazon.com/s3/home" target="_new">
              click its name
            </a>
            , then "Permissions", and finally "Bucket Policy".
          </p>
          <p> 3. Edit the Policy and then click "Save". Here is an example:</p>
          <code>
            <pre>
              {JSON.stringify(
                {
                  Version: "2012-10-17",
                  Statement: [
                    {
                      Sid: "AllowS3F2",
                      Effect: "Allow",
                      Principal: {
                        AWS: [
                          "<AthenaExecutionRoleArn>",
                          "<DeleteTaskIAMRoleArn>"
                        ]
                      },
                      Action: "s3:*",
                      Resource: [
                        "arn:aws:s3:::<YOUR_S3_BUCKET_NAME>",
                        "arn:aws:s3:::<YOUR_S3_BUCKET_NAME>/*"
                      ]
                    }
                  ]
                },
                null,
                2
              )}
            </pre>
          </code>
        </Modal.Body>
        <Modal.Footer>
          <Button
            className="aws-button action-button"
            onClick={() => showBucketPolicy(false)}
          >
            Close
          </Button>
        </Modal.Footer>
      </Modal>

      <Form>
        <Table>
          <thead>
            <tr>
              <td></td>
              <td>Name</td>
              <td>Columns</td>
              <td>Format</td>
              <td>Query Executor</td>
              <td>Query Executor Parameters</td>
            </tr>
          </thead>
          <tbody>
            {dataMappers &&
              dataMappers.map((dataMapper, index) => (
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
                  <td>{dataMapper.DataMapperId}</td>
                  <td>{dataMapper.Columns.join(", ")}</td>
                  <td>{dataMapper.Format}</td>
                  <td>{dataMapper.QueryExecutor}</td>
                  <td>
                    {dataMapper.QueryExecutorParameters.Database}/
                    {dataMapper.QueryExecutorParameters.Table} (
                    {dataMapper.QueryExecutorParameters.DataCatalogProvider})
                  </td>
                </tr>
              ))}
          </tbody>
        </Table>
        {dataMappers && dataMappers.length === 0 && formState !== "initial" && (
          <div className="content centered">
            <b>No Data Mappers</b>
            <p>No Data Mappers to display</p>
            <Button className="aws-button" onClick={() => onPageChange(4)}>
              Create Data Mapper
            </Button>
          </div>
        )}
      </Form>
    </div>
  );
};
