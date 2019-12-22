import React, { useState } from "react";
import { Button, Form, Spinner } from "react-bootstrap";

import Alert from "../Alert";

import { formatErrorMessage } from "../../utils";
const region = window.s3f2Settings.region;

export default ({ gateway, goToDataMappers }) => {
  const [columns, setColumns] = useState(undefined);
  const [dataMapperId, setDataMapperId] = useState(undefined);
  const [errorDetails, setErrorDetails] = useState(undefined);
  const [formState, setFormState] = useState("initial");
  const [glueDatabase, setGlueDatabase] = useState(undefined);
  const [glueTable, setGlueTable] = useState(undefined);
  const [submitClicked, setSubmitClicked] = useState(false);

  const validationAttributes = isValid =>
    !submitClicked ? {} : isValid ? { isValid: true } : { isInvalid: true };

  const isEmpty = x =>
    !x || (Array.isArray(x) ? x.length === 0 : x.trim() === "");

  const idRegex = /^[a-zA-Z0-9-_]+$/;
  const isIdValid = x => idRegex.test(x);

  const arrayItemsNotEmptyReducer = (a, v) => a && !isEmpty(v);
  const arrayItemsNotEmpty = x => x && x.reduce(arrayItemsNotEmptyReducer);

  const isDataMapperIdValid = !isEmpty(dataMapperId) && isIdValid(dataMapperId);
  const isGlueDatabaseValid = !isEmpty(glueDatabase);
  const isGlueTableValid = !isEmpty(glueTable);
  const isColumnsValid = !isEmpty(columns) && arrayItemsNotEmpty(columns);

  const isFormValid =
    isDataMapperIdValid &&
    isGlueDatabaseValid &&
    isGlueTableValid &&
    isColumnsValid;

  const resetForm = () => {
    setColumns(undefined);
    setDataMapperId(undefined);
    setGlueDatabase(undefined);
    setGlueTable(undefined);
    setSubmitClicked(false);
    setFormState("initial");
  };

  const cancel = () => {
    resetForm();
    goToDataMappers();
  };

  const submitForm = async () => {
    setSubmitClicked(true);
    if (isFormValid) {
      setFormState("saving");
      try {
        await gateway.putDataMapper(
          dataMapperId,
          glueDatabase,
          glueTable,
          columns
        );
        setFormState("saved");
        resetForm();
        goToDataMappers();
      } catch (e) {
        setFormState("error");
        setErrorDetails(formatErrorMessage(e));
      }
    }
  };

  return (
    <Form>
      <h1>New Data Mapper</h1>
      <div className="form-container">
        <p>
          Data mappers allow you to connect data hosted in S3 to the Amazon S3
          Find and Forget solution. You are responsible for setting up Data
          Mappers correctly and make sure all the S3 buckets are mapped to the
          solution. After creating a data mapper, you must update the S3 Bucket
          Policy for the S3 Bucket referenced by the data mapper to grant read
          permission (via Athena) and read/write permission (via Fargate) to the
          Amazon S3 Find and Forget Solution.
        </p>
      </div>
      {formState === "saving" && (
        <Spinner animation="border" role="status" className="spinner" />
      )}
      {formState === "error" && (
        <>
          <div className="form-container">
            <Alert type="error" title="An error happened">
              {errorDetails}
            </Alert>
          </div>
          <div className="form-container submit-container">
            <Button className="aws-button" onClick={cancel}>
              Return to Data Mappers
            </Button>
          </div>
        </>
      )}
      {formState === "initial" && (
        <>
          <div className="page-table form-container">
            <h2>Data Mapper Settings</h2>
            <div className="content">
              <Form.Group controlId="dataMapperId">
                <Form.Label>Data Mapper Name</Form.Label>
                <Form.Text className="text-muted">
                  Input a name for your Data Mapper. Alphanumeric characters and
                  (_-) are allowed
                </Form.Text>
                <Form.Control
                  type="text"
                  onChange={e => setDataMapperId(e.target.value)}
                  {...validationAttributes(isDataMapperIdValid)}
                />
              </Form.Group>
            </div>
          </div>
          <div className="page-table form-container">
            <h2>Query Executor</h2>
            <div className="content">
              <p>Query Executor Type</p>
              <div className="selected-card">
                <Form.Check inline readOnly checked type="radio" />
                <span>Athena + Glue</span>
                <p>
                  Amazon Athena is responsible for the Find operation. AWS Glue
                  is used to catalog data in S3 and maintain metadata such as
                  data structure and partitions.
                </p>
              </div>
              <Form.Group controlId="glueDatabase">
                <Form.Label>AWS Glue Database</Form.Label>
                <Form.Text className="text-muted">
                  Input an existing Glue Database in the current account in the{" "}
                  <i>{region}</i> region
                </Form.Text>
                <Form.Control
                  type="text"
                  onChange={e => setGlueDatabase(e.target.value)}
                  {...validationAttributes(isGlueDatabaseValid)}
                />
              </Form.Group>
              <Form.Group controlId="glueTable">
                <Form.Label>AWS Glue Table</Form.Label>
                <Form.Text className="text-muted">
                  Input an existing Glue Table in the current account in the{" "}
                  <i>{region}</i> region
                </Form.Text>
                <Form.Control
                  type="text"
                  onChange={e => setGlueTable(e.target.value)}
                  {...validationAttributes(isGlueTableValid)}
                />
              </Form.Group>
              <Form.Group controlId="glueTable">
                <Form.Label>Columns used to query for matches</Form.Label>
                <Form.Text className="text-muted">
                  Input a comma separated list of columns used to query for
                  matches
                </Form.Text>
                <Form.Control
                  type="text"
                  placeholder="userid, author"
                  onChange={e =>
                    setColumns(e.target.value.split(",").map(x => x.trim()))
                  }
                  {...validationAttributes(isColumnsValid)}
                />
              </Form.Group>
            </div>
          </div>
          <div className="form-container submit-container">
            <Button className="aws-button cancel" onClick={cancel}>
              Cancel
            </Button>
            <Button className="aws-button" onClick={submitForm}>
              Create Data Mapper
            </Button>
          </div>
        </>
      )}
    </Form>
  );
};
