import React from "react";
import { Form } from "react-bootstrap";

const FormRadioBinaryChoice = ({
  children,
  isDefaultChoice = false,
  name,
  onlyChoice = false,
  onSelect,
  selected,
  title
}) => (
  <div
    className={`radio-card ${selected && "selected"}`}
    style={{
      width: "49%",
      marginRight: onlyChoice ? "51%" : "1%"
    }}
  >
    <Form.Check
      onChange={onSelect}
      inline
      name={name}
      type="radio"
      {...(isDefaultChoice && { defaultChecked: true })}
    />
    <span>{title}</span>
    <p>{children}</p>
  </div>
);

export default FormRadioBinaryChoice;
