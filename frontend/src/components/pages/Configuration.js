import React, { useEffect, useState } from "react";
import { Button, Col, Form, Modal, Row, Spinner, Table } from "react-bootstrap";

import Alert from "../Alert";
import BucketPolicyModal from "../BucketPolicyModal";
import Icon from "../Icon";

import { formatErrorMessage, sortBy } from "../../utils";
import { bucketMapper } from "../../utils/glueSerializer";

export default ({ gateway, onPageChange }) => {
  const [accountId, setAccountId] = useState("<AWS::ACCOUNT_ID>");
  const [bucketLocations, setBucketLocations] = useState(undefined);
  const [dataMappers, setDataMappers] = useState([]);
  const [deleting, setDeleting] = useState(false);
  const [errorDetails, setErrorDetails] = useState(undefined);
  const [formState, setFormState] = useState("initial");
  const [renderTableCount, setRenderTableCount] = useState(0);
  const [selectedRow, selectRow] = useState(undefined);
  const [showingBucketPolicy, showBucketPolicy] = useState(false);

  const noSelected = typeof selectedRow === "undefined";

  const getBucket = row => {
    const selectedDataMapper = dataMappers[row];
    const key = `${selectedDataMapper.QueryExecutorParameters.Database}/${selectedDataMapper.QueryExecutorParameters.Table}`;
    return bucketLocations[key];
  };

  const refreshDataMappers = () => {
    selectRow(undefined);
    setDataMappers([]);
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
        const [mappers, identity] = await Promise.all([
          gateway.getDataMappers(),
          gateway.getAccountId()
        ]);

        const tableDetails = await Promise.all(
          mappers.DataMappers.map(dm =>
            gateway.getGlueTable(
              dm.QueryExecutorParameters.Database,
              dm.QueryExecutorParameters.Table
            )
          )
        );

        setAccountId(
          identity.GetCallerIdentityResponse.GetCallerIdentityResult.Account
        );
        setBucketLocations(bucketMapper(tableDetails));
        setDataMappers(sortBy(mappers.DataMappers, "DataMapperId"));
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
            {...(noSelected && { disabled: "disabled" })}
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
        <>
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
          <BucketPolicyModal
            accountId={accountId}
            bucket={getBucket(selectedRow).bucket}
            close={() => showBucketPolicy(false)}
            show={showingBucketPolicy}
          />
        </>
      )}

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
              <td>Location</td>
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
                  <td>{getBucket(index).location}</td>
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
