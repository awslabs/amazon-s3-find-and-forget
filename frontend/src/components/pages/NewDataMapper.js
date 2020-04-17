import React, { useEffect, useState } from "react";
import { Button, Form, Spinner } from "react-bootstrap";

import Alert from "../Alert";

import {
  formatErrorMessage,
  isEmpty,
  isIdValid,
  isRoleArnValid
} from "../../utils";

import { glueSerializer } from "../../utils/glueSerializer";

const region = window.s3f2Settings.region;

export default ({ gateway, goToDataMappers }) => {
  const [columns, setColumns] = useState([]);
  const [dataMapperId, setDataMapperId] = useState(undefined);
  const [errorDetails, setErrorDetails] = useState(undefined);
  const [formState, setFormState] = useState("loading");
  const [glueData, setGlueData] = useState(undefined);
  const [glueDatabase, setGlueDatabase] = useState(undefined);
  const [glueTable, setGlueTable] = useState(undefined);
  const [roleArn, setRoleArn] = useState(undefined);
  const [deletePreviousVersions, setDeletePreviousVersions] = useState(true);
  const [submitClicked, setSubmitClicked] = useState(false);

  const addColumn = c => {
    if (!columns.includes(c)) setColumns([...columns, c]);
  };

  const removeColumn = c => {
    if (columns.includes(c)) setColumns(columns.filter(x => x !== c));
  };

  const validationAttributes = isValid =>
    !submitClicked ? {} : isValid ? { isValid: true } : { isInvalid: true };

  const isDataMapperIdValid = !isEmpty(dataMapperId) && isIdValid(dataMapperId);
  const isGlueDatabaseValid = !isEmpty(glueDatabase) && glueDatabase !== "-1";
  const isGlueTableValid = !isEmpty(glueTable) && glueTable !== "-1";
  const isColumnsValid = !isEmpty(columns);
  const isRoleValid = !isEmpty(roleArn) && isRoleArnValid(roleArn);

  const isFormValid =
    isDataMapperIdValid &&
    isGlueDatabaseValid &&
    isGlueTableValid &&
    isColumnsValid &&
    isRoleValid;

  const resetGlueTable = () => {
    setGlueTable("-1");
    const tableRef = document.getElementById("glueTable");
    tableRef.selectedIndex = 0;
  };

  const resetGlueColumns = () => {
    setColumns([]);
    let checkboxes = document.getElementsByName("column");
    for (let i = 0; i < checkboxes.length; i++) checkboxes[i].checked = false;
  };

  const submitForm = async () => {
    setSubmitClicked(true);
    if (isFormValid) {
      setFormState("loading");
      try {
        await gateway.putDataMapper(
          dataMapperId,
          glueDatabase,
          glueTable,
          columns,
          roleArn,
          deletePreviousVersions
        );
        setFormState("saved");
      } catch (e) {
        setFormState("error");
        setErrorDetails(formatErrorMessage(e));
      }
    }
  };

  const selectedDatabase = glueDatabase
    ? glueData.databases.find(x => x.name === glueDatabase)
    : undefined;

  const selectedTable = selectedDatabase
    ? selectedDatabase.tables.find(t => t.name === glueTable)
    : undefined;

  const tablesForSelectedDatabase = selectedDatabase
    ? selectedDatabase.tables
    : [];

  const columnsForSelectedTable = selectedTable ? selectedTable.columns : [];
  const noTables = !glueData || isEmpty(glueData.databases);

  useEffect(() => {
    const fetchGlueTables = async () => {
      try {
        const databases = await gateway.getGlueDatabases();
        const tables = await Promise.all(
          databases.DatabaseList.map(x => gateway.getGlueTables(x.Name))
        );
        setGlueData(glueSerializer(tables));
        setFormState("initial");
      } catch (e) {
        setFormState("error");
        setErrorDetails(formatErrorMessage(e));
      }
    };
    fetchGlueTables();
  }, [gateway]);

  return (
    <Form>
      <h1>
        {formState === "saved"
          ? "Data Mapper successfully created"
          : "Create Data Mapper"}
      </h1>
      {formState === "loading" && (
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
            <Button className="aws-button" onClick={goToDataMappers}>
              Return to Data Mappers
            </Button>
          </div>
        </>
      )}
      {formState === "saved" && (
        <>
          <div className="form-container">
            <Alert type="info" title="Action needed">
              You must update the S3 Bucket Policy for the S3 Bucket referenced
              by the data mapper to grant read permission to the IAM role
              assumed by the query executor (e.g. Amazon Athena), and read/write
              permission to the IAM role used by AWS Fargate to perform any
              required deletions. On the Data Mappers page you can see the
              required policies by choosing a Data Mapper from the list and then
              choosing <strong>Generate Access Policies</strong>.
            </Alert>
          </div>
          <div className="form-container submit-container">
            <Button className="aws-button" onClick={goToDataMappers}>
              Return to Data Mappers
            </Button>
          </div>
        </>
      )}
      {formState === "initial" && noTables && (
        <div className="page-table form-container">
          <div className="content centered">
            <b>No Glue Tables found</b>
            <p>
              There are no valid Glue Tables available in the current account
              for the region <i>{region}</i>.
            </p>
            <Button
              className="aws-button"
              onClick={() =>
                window.open(
                  `https://console.aws.amazon.com/glue/home?region=${region}`,
                  "_blank"
                )
              }
            >
              Create a Glue Table in the AWS Glue Console
            </Button>
          </div>
        </div>
      )}
      {formState === "initial" && !noTables && (
        <>
          <div className="page-table form-container">
            <h2>Data Mapper Settings</h2>
            <div className="content">
              <Form.Group controlId="dataMapperId">
                <Form.Label>Data Mapper Name</Form.Label>
                <Form.Text className="text-muted">
                  Input a name for your Data Mapper. Only alphanumeric
                  characters are allowed.
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
              <div className="radio-card selected">
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
                  Glue Database in the current account in the <i>{region}</i>{" "}
                  region
                </Form.Text>
                <Form.Control
                  as="select"
                  onChange={e => {
                    setGlueDatabase(e.target.value);
                    resetGlueTable();
                    resetGlueColumns();
                  }}
                  {...validationAttributes(isGlueDatabaseValid)}
                >
                  <option value="-1">Select a Glue Database</option>
                  {glueData.databases.map((d, index) => (
                    <option key={index} value={d.name}>
                      {d.name}
                    </option>
                  ))}
                </Form.Control>
              </Form.Group>
              <Form.Group controlId="glueTable">
                <Form.Label>AWS Glue Table</Form.Label>
                <Form.Text className="text-muted">
                  Glue Table in the current account in the <i>{region}</i>{" "}
                  region
                </Form.Text>
                <Form.Control
                  as="select"
                  onChange={e => {
                    setGlueTable(e.target.value);
                    resetGlueColumns();
                  }}
                  {...validationAttributes(isGlueTableValid)}
                >
                  <option value="-1" defaultValue>
                    Select a Glue Table
                  </option>
                  {tablesForSelectedDatabase.map((t, index) => (
                    <option key={index} value={t.name}>
                      {t.name}
                    </option>
                  ))}
                </Form.Control>
              </Form.Group>
              <Form.Group>
                <Form.Label>Columns used to query for matches</Form.Label>
                <Form.Text className="text-muted">
                  Select one or more column from the table
                </Form.Text>
                {columnsForSelectedTable.map((c, index) => (
                  <Form.Check
                    type="checkbox"
                    id={`cb-${index}`}
                    key={index}
                    name="column"
                    label={c}
                    onChange={e =>
                      e.target.checked ? addColumn(c) : removeColumn(c)
                    }
                    {...validationAttributes(isColumnsValid)}
                  />
                ))}
                {isEmpty(columnsForSelectedTable) && (
                  <Form.Text className="text-muted">
                    No table selected
                  </Form.Text>
                )}
              </Form.Group>
              <Form.Group controlId="roleArn">
                <Form.Label>AWS IAM Role ARN</Form.Label>
                <Form.Text className="text-muted">
                  The ARN of the AWS IAM Role that Fargate should assume to
                  perform deletions
                </Form.Text>
                <Form.Control
                  type="text"
                  onChange={e => setRoleArn(e.target.value)}
                  {...validationAttributes(isRoleValid)}
                />
              </Form.Group>
              <Form.Group>
                <Form.Check
                  type="checkbox"
                  id="delete-old-versions"
                  name="column"
                  checked={deletePreviousVersions}
                  label="Delete previous object versions after update"
                  onChange={e => setDeletePreviousVersions(!deletePreviousVersions)}
                />
              </Form.Group>
            </div>
          </div>
          <div className="form-container submit-container">
            <Button className="aws-button cancel" onClick={goToDataMappers}>
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
