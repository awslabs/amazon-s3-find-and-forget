import React, { useState, useEffect } from "react";
import { Button, Form, Spinner } from "react-bootstrap";

import Alert from "../Alert";

import { formatErrorMessage, isEmpty, sortBy } from "../../utils";

export default ({ gateway, goToDeletionQueue }) => {
  const [dataMappers, setDataMappers] = useState(undefined);
  const [dataMappersMode, setDataMappersMode] = useState("all");
  const [errorDetails, setErrorDetails] = useState(undefined);
  const [formState, setFormState] = useState("initial");
  const [matchId, setMatchId] = useState(undefined);
  const [selectedDataMappers, setSelectedDataMappers] = useState([]);
  const [submitClicked, setSubmitClicked] = useState(false);

  const addDataMapper = dm => {
    if (!selectedDataMappers.includes(dm)) {
      setSelectedDataMappers([...selectedDataMappers, dm]);
    }
  };

  const removeDataMapper = dm => {
    if (selectedDataMappers.includes(dm)) {
      setSelectedDataMappers(selectedDataMappers.filter(x => x !== dm));
    }
  };

  const validationAttributes = isValid =>
    !submitClicked ? {} : isValid ? { isValid: true } : { isInvalid: true };

  const isMatchIdValid = !isEmpty(matchId);
  const allMappers = dataMappersMode === "all";
  const isSelectedDataMappersValid = !isEmpty(selectedDataMappers);

  const isFormValid =
    isMatchIdValid && (allMappers || isSelectedDataMappersValid);

  const allMappersClasses = `radio-card ${allMappers && "selected"}`;
  const selectMappersClasses = `radio-card ${!allMappers && "selected"}`;

  const resetForm = () => {
    setSelectedDataMappers([]);
    setDataMappersMode("all");
    setMatchId(undefined);
    setSubmitClicked(false);
    setFormState("initial");
  };

  const cancel = () => {
    resetForm();
    goToDeletionQueue();
  };

  const submitForm = async () => {
    setSubmitClicked(true);
    if (isFormValid) {
      setFormState("saving");
      try {
        const dm = allMappers ? undefined : selectedDataMappers;
        await gateway.enqueue(matchId, dm);
        setFormState("saved");
        resetForm();
        goToDeletionQueue();
      } catch (e) {
        setFormState("error");
        setErrorDetails(formatErrorMessage(e));
      }
    }
  };

  useEffect(() => {
    const fetchDataMappers = async () => {
      try {
        const dataMappers = await gateway.getDataMappers();
        setDataMappers(sortBy(dataMappers.DataMappers, "DataMapperId"));
      } catch (e) {
        setFormState("error");
        setErrorDetails(formatErrorMessage(e));
      }
    };
    fetchDataMappers();
  }, [gateway]);

  return (
    <Form>
      <h1>Add a Match to the Deletion Queue</h1>
      <div className="form-container">
        <p>
          Items added to the Deletion Queue are picked for deletion on the next
          Deletion Job. Once the Deletion Job succeeds, the item is
          automatically deleted from the queue. If any issue happens during the
          Deletion Job, the item stays in the queue.
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
              Return to the Deletion Queue
            </Button>
          </div>
        </>
      )}
      {formState === "initial" && (
        <>
          <div className="page-table form-container">
            <h2>Insert Match Details</h2>
            <div className="content">
              <Form.Group controlId="dataMapperId">
                <Form.Label>Match</Form.Label>
                <Form.Text className="text-muted">
                  This is the value that will be used to identify the rows to be
                  deleted
                </Form.Text>
                <Form.Control
                  type="text"
                  onChange={e => setMatchId(e.target.value)}
                  placeholder="jane_doe"
                  {...validationAttributes(isMatchIdValid)}
                />
              </Form.Group>
              <p>Data Mappers</p>
              <div
                className={allMappersClasses}
                style={{ width: "49%", marginRight: "1%" }}
              >
                <Form.Check
                  onChange={() => setDataMappersMode("all")}
                  inline
                  name="dataMappersMode"
                  type="radio"
                  defaultChecked
                />
                <span>All Data Mappers</span>
                <p>
                  During a Deletion Job, we will search for a match in all Data
                  Mappers.
                </p>
              </div>
              {dataMappers && dataMappers.length > 0 && (
                <div
                  className={selectMappersClasses}
                  style={{ width: "49%", marginLeft: "1%" }}
                >
                  <Form.Check
                    inline
                    name="dataMappersMode"
                    type="radio"
                    onChange={() => setDataMappersMode("select")}
                  />
                  <span>Select your Data Mappers</span>
                  <p>
                    During a Deletion Job, we will search for a match in one or
                    more specific Data Mappers.
                  </p>
                </div>
              )}
              {dataMappersMode === "select" && (
                <Form.Group>
                  <Form.Label>Data Mappers</Form.Label>
                  <Form.Text className="text-muted">
                    Select all the Data Mappers that apply to the current match.
                    You must select at least one Data Mapper from this list.
                  </Form.Text>
                  {dataMappers.map((dataMapper, index) => (
                    <Form.Check
                      type="checkbox"
                      key={index}
                      id={`cb-sdm-${index}`}
                      label={dataMapper.DataMapperId}
                      onChange={e => {
                        const id = dataMapper.DataMapperId;
                        const checked = e.target.checked;
                        return checked
                          ? addDataMapper(id)
                          : removeDataMapper(id);
                      }}
                      {...validationAttributes(isSelectedDataMappersValid)}
                    ></Form.Check>
                  ))}
                </Form.Group>
              )}
            </div>
          </div>
          <div className="form-container submit-container">
            <Button className="aws-button cancel" onClick={cancel}>
              Cancel
            </Button>
            <Button className="aws-button" onClick={submitForm}>
              Add Item to the Deletion Queue
            </Button>
          </div>
        </>
      )}
    </Form>
  );
};
