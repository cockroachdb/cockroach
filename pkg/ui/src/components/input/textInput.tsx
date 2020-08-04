// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import cn from "classnames";

import { Text, TextTypes } from "src/components";
import "./input.styl";

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

interface TextInputState {
  validationMessage: string;
  isValid: boolean;
  isDirty: boolean;
  isTouched: boolean;
  needValidation: boolean;
}

export class TextInput extends React.Component<TextInputProps, TextInputState> {
  static defaultProps = {
    initialValue: "",
    validate: () => false,
  };

  constructor(props: TextInputProps) {
    super(props);

    this.state = {
      isValid: true,
      validationMessage: undefined,
      isDirty: false,
      isTouched: false,
      needValidation: false,
    };
  }

  validateInput = (value: string) => {
    const { validate } = this.props;
    const validationMessage = validate(value);
    this.setState({
      isValid: !validationMessage,
      validationMessage,
    });
  };

  handleOnTextChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const value = event.target.value;
    const { needValidation, isValid } = this.state;
    if (needValidation && !isValid) {
      this.validateInput(value);
    }
    this.setState({
      isDirty: true,
    });
    this.props.onChange(value);
  };

  handleOnBlur = (event: React.ChangeEvent<HTMLInputElement>) => {
    const value = event.target.value;
    this.validateInput(value);
    this.setState({
      isTouched: true,
      needValidation: true,
    });
  };

  render() {
    const {
      initialValue,
      placeholder,
      className,
      name,
      value,
      label,
    } = this.props;
    const { isDirty, isValid, validationMessage } = this.state;
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
          onChange={this.handleOnTextChange}
          onBlur={this.handleOnBlur}
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
  }
}
