// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import cn from "classnames";
import React, { useState } from "react";

import { Text, TextTypes } from "src/components";
import "./input.scss";

interface TextInputProps {
  onChange: (value: string) => void;
  value: string;
  initialValue?: string;
  placeholder?: string;
  className?: string;
  name?: string;
  label?: string;
  // validate function returns validation message
  // in case validation failed or undefined if successful.
  validate?: (value: string) => string | undefined;
}

const defaultValidate = () => undefined as string | undefined;

export const TextInput: React.FC<TextInputProps> = ({
  onChange,
  value,
  initialValue = "",
  placeholder,
  className,
  name,
  label,
  validate = defaultValidate,
}) => {
  const [isValid, setIsValid] = useState(true);
  const [validationMessage, setValidationMessage] = useState<
    string | undefined
  >(undefined);
  const [isDirty, setIsDirty] = useState(false);
  const [needValidation, setNeedValidation] = useState(false);

  const validateInput = (val: string) => {
    const msg = validate(val);
    setIsValid(!msg);
    setValidationMessage(msg);
  };

  const handleOnTextChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const val = event.target.value;
    if (needValidation && !isValid) {
      validateInput(val);
    }
    setIsDirty(true);
    onChange(val);
  };

  const handleOnBlur = (event: React.ChangeEvent<HTMLInputElement>) => {
    validateInput(event.target.value);
    setNeedValidation(true);
  };

  const textValue = isDirty ? value : initialValue;

  const classes = cn(className, "crl-input", "crl-input__text", {
    "crl-input__text--invalid": !isValid,
  });

  return (
    <div className="crl-input__wrapper">
      {label && (
        <label htmlFor={name} className="crl-input__label">
          {label}
        </label>
      )}
      <input
        name={name}
        type="text"
        value={textValue}
        placeholder={placeholder}
        className={classes}
        onChange={handleOnTextChange}
        onBlur={handleOnBlur}
        autoComplete="off"
      />
      {!isValid && (
        <div className="crl-input__text--validation-container">
          <Text
            textType={TextTypes.Caption}
            className="crl-input__text--error-message"
          >
            {validationMessage}
          </Text>
        </div>
      )}
    </div>
  );
};
