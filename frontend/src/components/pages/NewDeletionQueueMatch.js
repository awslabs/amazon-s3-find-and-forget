import React, { useEffect, useReducer, useState } from "react";
import { Button, Form, Spinner } from "react-bootstrap";

import Alert from "../Alert";
import CompositeMatchIdItem from "../CompositeMatchIdItem";
import FormRadioBinaryChoice from "../FormRadioBinaryChoice";

import { formatErrorMessage, isEmpty, isUndefined, sortBy } from "../../utils";

export default ({ gateway, goToDeletionQueue }) => {
  const [compositeMatchDataMapper, setCompositeMatchDataMapper] = useState(
    undefined
  );
  const [dataMappers, setDataMappers] = useState(undefined);
  const [dataMappersMode, setDataMappersMode] = useState("all");
  const [errorDetails, setErrorDetails] = useState(undefined);
  const [formState, setFormState] = useState("initial");
  const [matchId, setMatchId] = useState(undefined);
  const [matchType, setMatchType] = useState("simple");
  const [selectedDataMappers, setSelectedDataMappers] = useState([]);
  const [submitClicked, setSubmitClicked] = useState(false);

  const [compositeMatchId, setCompositeMatchId] = useReducer(
    (state, action) => {
      if (action.type === "update") {
        const columns = Object.keys(state);
        if (!isUndefined(action.value)) {
          return Object.assign({}, state, { [action.column]: action.value });
        } else if (columns.includes(action.column)) {
          const clone = Object.assign({}, state);
          delete clone[action.column];
          return clone;
        }
      } else if (action.type === "reset") return {};
      return state;
    },
    {}
  );

  const addDataMapper = (dm) => {
    if (!selectedDataMappers.includes(dm)) {
      setSelectedDataMappers([...selectedDataMappers, dm]);
    }
  };

  const removeDataMapper = (dm) => {
    if (selectedDataMappers.includes(dm)) {
      setSelectedDataMappers(selectedDataMappers.filter((x) => x !== dm));
    }
  };

  const getDataMapperColumns = (id) =>
    dataMappers.find((x) => x.DataMapperId === id).Columns;

  const validationAttributes = (isValid) =>
    !submitClicked ? {} : isValid ? { isValid: true } : { isInvalid: true };

  const isMatchIdValid = !isEmpty(matchId);
  const allMappers = dataMappersMode === "all";
  const simpleMatchType = matchType === "simple";
  const isSelectedDataMappersValid = !isEmpty(selectedDataMappers);
  const atLeastOneCompositeMatchProvided = !isEmpty(
    Object.keys(compositeMatchId)
  );
  const isCompositeDataMapperValid =
    !isUndefined(compositeMatchDataMapper) && atLeastOneCompositeMatchProvided;

  const isCompositeFormValid =
    !simpleMatchType &&
    isCompositeDataMapperValid &&
    atLeastOneCompositeMatchProvided;

  const isSimpleFormValid =
    simpleMatchType &&
    isMatchIdValid &&
    (allMappers || isSelectedDataMappersValid);

  const isFormValid = isSimpleFormValid || isCompositeFormValid;

  const dataMappersForCompositeMatches =
    (dataMappers && dataMappers.filter((x) => x.Columns.length > 1)) || [];

  const compositeMatchesAllowed = !isEmpty(dataMappersForCompositeMatches);

  const resetSimpleFlow = () => {
    setSelectedDataMappers([]);
    setDataMappersMode("all");
    setMatchId(undefined);
    setSubmitClicked(false);
  };

  const resetCompositeFlow = () => {
    setCompositeMatchDataMapper(undefined);
    setCompositeMatchId({ type: "reset" });
  };

  const resetFormAndGoToDeletionQueue = () => {
    resetSimpleFlow();
    resetCompositeFlow();
    setMatchType("simple");
    setFormState("initial");
    goToDeletionQueue();
  };

  const submitForm = async () => {
    setSubmitClicked(true);
    if (isFormValid) {
      setFormState("saving");
      try {
        if (simpleMatchType) {
          const dm = allMappers ? undefined : selectedDataMappers;
          await gateway.enqueueSimple(matchId, dm);
        } else {
          await gateway.enqueueComposite(
            compositeMatchId,
            compositeMatchDataMapper
          );
        }
        setFormState("saved");
        resetFormAndGoToDeletionQueue();
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
            <Button
              className="aws-button"
              onClick={resetFormAndGoToDeletionQueue}
            >
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
              <p>Match Type</p>
              <FormRadioBinaryChoice
                isDefaultChoice={true}
                name="matchTypeMode"
                onlyChoice={!compositeMatchesAllowed}
                onSelect={() => {
                  setMatchType("simple");
                  resetCompositeFlow();
                }}
                selected={simpleMatchType}
                title="Simple"
              >
                A value to be matched against any column identifier of one or
                more data mappers<br /><br />
              </FormRadioBinaryChoice>
              {compositeMatchesAllowed && (
                <FormRadioBinaryChoice
                  name="matchTypeMode"
                  onSelect={() => {
                    setMatchType("composite");
                    resetSimpleFlow();
                  }}
                  selected={!simpleMatchType}
                  title="Composite"
                >
                  One or more values to be matched against specific columns
                  identifiers of a multi-column based data mapper
                </FormRadioBinaryChoice>
              )}
              {simpleMatchType && (
                <>
                  <Form.Group controlId="matchType">
                    <Form.Label>Match</Form.Label>
                    <Form.Text className="text-muted">
                      This is the value that will be used to identify the rows
                      to be deleted
                    </Form.Text>
                    <Form.Control
                      type="text"
                      onChange={(e) => setMatchId(e.target.value)}
                      autoComplete="off"
                      {...validationAttributes(isMatchIdValid)}
                    />
                  </Form.Group>
                  <p>Data Mappers</p>
                  <FormRadioBinaryChoice
                    isDefaultChoice={true}
                    name="dataMappersMode"
                    onSelect={() => setDataMappersMode("all")}
                    selected={allMappers}
                    title="All Data Mappers"
                  >
                    During a Deletion Job, we will search for a match in all
                    Data Mappers
                  </FormRadioBinaryChoice>
                  {!isEmpty(dataMappers) && (
                    <FormRadioBinaryChoice
                      name="dataMappersMode"
                      onSelect={() => setDataMappersMode("select")}
                      selected={!allMappers}
                      title="Select your Data Mappers"
                    >
                      During a Deletion Job, we will search for a match in one
                      or more specific Data Mappers
                    </FormRadioBinaryChoice>
                  )}
                  {dataMappersMode === "select" && (
                    <Form.Group>
                      <Form.Label>Data Mappers</Form.Label>
                      <Form.Text className="text-muted">
                        Select all the Data Mappers that apply to the current
                        match. You must select at least one Data Mapper from
                        this list.
                      </Form.Text>
                      {dataMappers.map((dataMapper, index) => (
                        <Form.Check
                          type="checkbox"
                          key={index}
                          id={`cb-sdm-${index}`}
                          label={dataMapper.DataMapperId}
                          onChange={(e) => {
                            const id = dataMapper.DataMapperId;
                            const checked = e.target.checked;
                            return checked
                              ? addDataMapper(id)
                              : removeDataMapper(id);
                          }}
                          {...validationAttributes(isSelectedDataMappersValid)}
                        />
                      ))}
                    </Form.Group>
                  )}
                </>
              )}
              {matchType === "composite" && (
                <>
                  <Form.Group>
                    <Form.Label>Data Mapper</Form.Label>
                    <Form.Text className="text-muted">
                      Select the Data Mapper that applies to the current
                      matches. For a composite Match Id, you can only use Data
                      Mappers with more than one column identifier.
                    </Form.Text>
                    {dataMappersForCompositeMatches.map((dataMapper, index) => (
                      <Form.Check
                        name="compositeMatchDataMapper"
                        type="radio"
                        key={index}
                        id={`cb-sdm-composite-${index}`}
                        label={dataMapper.DataMapperId}
                        onChange={() => {
                          setCompositeMatchDataMapper(dataMapper.DataMapperId);
                          setCompositeMatchId({});
                        }}
                        {...validationAttributes(isCompositeDataMapperValid)}
                      />
                    ))}
                  </Form.Group>
                  {compositeMatchDataMapper && (
                    <>
                      <Form.Group>
                        <Form.Label>Columns</Form.Label>
                        <Form.Text className="text-muted">
                          Select all the columns (at least one) that you want to
                          map to a match, and then provide a value for each of
                          them. Empty is a valid value.
                        </Form.Text>
                        {getDataMapperColumns(compositeMatchDataMapper).map(
                          (column, index) => (
                            <CompositeMatchIdItem
                              column={column}
                              index={index}
                              key={index}
                              onChange={(value) =>
                                setCompositeMatchId({
                                  type: "update",
                                  column,
                                  value,
                                })
                              }
                            />
                          )
                        )}
                      </Form.Group>
                    </>
                  )}
                </>
              )}
            </div>
          </div>
          <div className="form-container submit-container">
            <Button
              className="aws-button cancel"
              onClick={resetFormAndGoToDeletionQueue}
            >
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
