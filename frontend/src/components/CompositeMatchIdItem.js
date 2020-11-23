import React, { memo, useEffect, useState } from "react";
import { Form } from "react-bootstrap";

const CompositeMatchIdItem = ({ column, onChange }) => {
  const [columnSelected, selectColumn] = useState(false);
  const [columnValue, setColumnValue] = useState(undefined);

  useEffect(() => {
    onChange(columnValue);
  }, [columnValue, onChange]);

  return (
    <>
      <Form.Check
        type="checkbox"
        id={`cb-composite-sc-${column}`}
        label={column}
        onChange={e => {
          const checked = e.target.checked;
          selectColumn(checked);
          setColumnValue(checked ? "" : undefined);
        }}
      />
      {columnSelected && (
        <Form.Control
          type="text"
          onChange={e => setColumnValue(e.target.value)}
          autoComplete="off"
        />
      )}
    </>
  );
};

const areEqual = (prevProps, nextProps) =>
  prevProps.column === nextProps.column;

export default memo(CompositeMatchIdItem, areEqual);
