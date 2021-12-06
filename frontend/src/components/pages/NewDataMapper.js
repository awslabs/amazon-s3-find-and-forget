import React, { useEffect, useReducer, useState, Fragment } from "react";
import { Button, Form, Spinner } from "react-bootstrap";

import Alert from "../Alert";

import {
  formatErrorMessage,
  isEmpty,
  isIdValid,
  isRoleArnValid,
  isUndefined,
  multiValueArrayReducer,
} from "../../utils";

import { glueSerializer } from "../../utils/glueSerializer";

const region = window.s3f2Settings.region;

const ColumnsViewer = ({
  allColumns,
  columns,
  prefix = "",
  depth = 0,
  setColumns,
  extraAttributes,
}) =>
  allColumns.map((c, index) => {
    const column = `${prefix}${c.name}`;
    return (
      <Fragment key={`cv-${column}-${index}`}>
        <Form.Check
          type="checkbox"
          id={`cb-${column}`}
          checked={columns.includes(column)}
          name="column"
          label={`${c.name} (${c.type})`}
          onChange={(e) =>
            setColumns({
              type: e.target.checked ? "add" : "remove",
              value: column,
            })
          }
          {...extraAttributes}
          style={{ marginLeft: `${depth * 10}px` }}
          disabled={!c.canBeIdentifier}
        />
        {c.children && (
          <ColumnsViewer
            allColumns={c.children}
            columns={columns}
            prefix={`${column}.`}
            depth={depth + 1}
            key={`n-${depth}`}
            setColumns={setColumns}
            extraAttributes={extraAttributes}
          />
        )}
      </Fragment>
    );
  });

const PartitionKeysViewer = ({
  allPartitionKeys,
  partitionKeys,
  setPartitionKeys,
}) =>
  allPartitionKeys.map((pk, index) => (
    <Form.Check
      checked={partitionKeys.includes(pk)}
      type="checkbox"
      key={`pkv-${index}`}
      id={`cb-pkv-${index}`}
      name="partition-key"
      label={pk}
      onChange={(e) =>
        setPartitionKeys({
          type: e.target.checked ? "add" : "remove",
          value: pk,
        })
      }
    />
  ));

const NewDataMapper = ({ gateway, goToDataMappers }) => {
  const [columns, setColumns] = useReducer(multiValueArrayReducer, []);
  const [partitionKeys, setPartitionKeys] = useReducer(
    multiValueArrayReducer,
    []
  );

  const [dataMapperId, setDataMapperId] = useState(undefined);
  const [errorDetails, setErrorDetails] = useState(undefined);
  const [existingDataMapperIds, setExistingDataMapperIds] = useState([]);
  const [formState, setFormState] = useState("loading");
  const [glueData, setGlueData] = useState(undefined);
  const [glueDatabase, setGlueDatabase] = useState(undefined);
  const [glueTable, setGlueTable] = useState(undefined);
  const [roleArn, setRoleArn] = useState(undefined);
  const [deletePreviousVersions, setDeletePreviousVersions] = useState(true);
  const [ignoreObjectNotFoundExceptions, setIgnoreObjectNotFoundExceptions] = useState(false);
  const [submitClicked, setSubmitClicked] = useState(false);

  const validationAttributes = (isValid) =>
    !submitClicked ? {} : isValid ? { isValid: true } : { isInvalid: true };

  const overlappingDataMappers = existingDataMapperIds.includes(dataMapperId);
  const isDataMapperIdValid =
    !isEmpty(dataMapperId) &&
    !overlappingDataMappers &&
    isIdValid(dataMapperId);
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

  const selectedDatabase = glueDatabase
    ? glueData.databases.find((x) => x.name === glueDatabase)
    : undefined;

  const selectedTable = selectedDatabase
    ? selectedDatabase.tables.find((t) => t.name === glueTable)
    : undefined;

  const tablesForSelectedDatabase = selectedDatabase
    ? selectedDatabase.tables
    : [];

  const columnsForSelectedTable = selectedTable ? selectedTable.columns : [];
  const partitionKeysForSelectedTable = selectedTable
    ? selectedTable.partitionKeys
    : [];
  const noTables = !glueData || isEmpty(glueData.databases);

  const submitForm = async () => {
    setSubmitClicked(true);
    if (isFormValid) {
      setFormState("loading");
      try {
        const dataMappers = await gateway.getDataMappers();
        if (
          dataMappers.DataMappers.find((x) => x.DataMapperId === dataMapperId)
        ) {
          throw new Error("A data mapper with this name already exists");
        }
        await gateway.putDataMapper(
          dataMapperId,
          glueDatabase,
          glueTable,
          columns,
          partitionKeys,
          roleArn,
          deletePreviousVersions,
          ignoreObjectNotFoundExceptions,
          selectedTable.format
        );
        setFormState("saved");
      } catch (e) {
        setFormState("error");
        setErrorDetails(formatErrorMessage(e));
      }
    }
  };

  useEffect(() => {
    setGlueTable("-1");
    const tableRef = document.getElementById("glueTable");
    if (tableRef) tableRef.selectedIndex = 0;
  }, [glueDatabase, setGlueTable]);

  useEffect(() => {
    setPartitionKeys({ type: "reset", value: selectedTable?.partitionKeys });
    setColumns({ type: "reset" });
  }, [selectedTable, setColumns, setPartitionKeys]);

  useEffect(() => {
    const fetchInitialData = async () => {
      try {
        const dataMappers = await gateway.getDataMappers();
        const databases = await gateway.getGlueDatabases();
        const tables = await Promise.all(
          databases.DatabaseList.map((x) => gateway.getGlueTables(x.Name))
        );
        setGlueData(glueSerializer(tables));
        setExistingDataMapperIds(
          dataMappers.DataMappers.map((x) => x.DataMapperId)
        );
        setFormState("initial");
      } catch (e) {
        setFormState("error");
        setErrorDetails(formatErrorMessage(e));
      }
    };
    fetchInitialData();
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
                  onChange={(e) => setDataMapperId(e.target.value)}
                  {...validationAttributes(isDataMapperIdValid)}
                />
                {submitClicked && overlappingDataMappers && (
                  <span className="form-error">
                    A data mapper with this name already exists
                  </span>
                )}
              </Form.Group>
            </div>
          </div>
          <div className="page-table form-container">
            <h2>Query Executor</h2>
            <div className="content">
              <p>Query Executor Type</p>
              <div className="radio-card selected">
                <Form.Check inline readOnly checked type="radio" />
                <span>Athena</span>
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
                  onChange={(e) => setGlueDatabase(e.target.value)}
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
                  onChange={(e) => setGlueTable(e.target.value)}
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
                <Form.Label>Format</Form.Label>
                <Form.Text className="text-muted">
                  {isEmpty(columnsForSelectedTable)
                    ? "No table selected"
                    : selectedTable.format}
                </Form.Text>
              </Form.Group>
              <Form.Group>
                <Form.Label>Partition Keys used to generate queries</Form.Label>
                <Form.Text className="text-muted">
                  {isUndefined(selectedTable)
                    ? `No table selected`
                    : isEmpty(partitionKeysForSelectedTable)
                    ? `None - the table is not partitioned`
                    : `To control the granularity of each query, you can
                      select the partition keys to be used in the query phase. If you
                      select none, only one query will be performed for the data
                      mapper. If you select all, more queries will be run to scan each partition
                      of the data separately.`}
                </Form.Text>
                <PartitionKeysViewer
                  allPartitionKeys={partitionKeysForSelectedTable}
                  partitionKeys={partitionKeys}
                  setPartitionKeys={setPartitionKeys}
                />
              </Form.Group>
              <Form.Group>
                <Form.Label>Columns used to query for matches</Form.Label>
                <Form.Text className="text-muted">
                  {!isEmpty(columnsForSelectedTable)
                    ? `Select one or more columns from the table`
                    : `No table selected`}
                </Form.Text>
                <ColumnsViewer
                  allColumns={columnsForSelectedTable}
                  columns={columns}
                  setColumns={setColumns}
                  extraAttributes={validationAttributes(isColumnsValid)}
                />
              </Form.Group>
              <Form.Group controlId="roleArn">
                <Form.Label>AWS IAM Role ARN</Form.Label>
                <Form.Text className="text-muted">
                  The ARN of the AWS IAM Role that Fargate should assume to
                  perform deletions
                </Form.Text>
                <Form.Control
                  type="text"
                  autoComplete="off"
                  onChange={(e) => setRoleArn(e.target.value)}
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
                  onChange={(e) =>
                    setDeletePreviousVersions(!deletePreviousVersions)
                  }
                />
              </Form.Group>
              <Form.Group>
                <Form.Check
                  type="checkbox"
                  id="ignore-object-not-found-exceptions"
                  name="column"
                  checked={ignoreObjectNotFoundExceptions}
                  label="Ignore object not found exceptions during deletion"
                  onChange={(e) =>
                    setIgnoreObjectNotFoundExceptions(!ignoreObjectNotFoundExceptions)
                  }
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

export default NewDataMapper;
