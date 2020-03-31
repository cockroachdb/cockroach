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
import "./textInput.styl";
import Eye from "assets/eye.svg";
import { Button } from "../button";

interface TextInputProps {
  onChange: (value: string) => void;
  value: string;
  initialValue?: string;
  placeholder?: string;
  className?: string;
  name?: string;
  type?: string;
  label?: string;
  // validate function returns validation message
  // in case validation failed or undefined if successful.
  validate?: (value: string) => string | undefined;
  autoComplete?: string;
}

interface TextInputState {
  validationMessage: string;
  isValid: boolean;
  isDirty: boolean;
  isTouched: boolean;
  needValidation: boolean;
  showPassword?: boolean;
}

export class TextInput extends React.Component<TextInputProps, TextInputState> {
  static defaultProps = {
    initialValue: "",
    validate: () => true,
    autoComplete: "on",
  };

  constructor(props: TextInputProps) {
    super(props);

    this.state = {
      isValid: true,
      validationMessage: undefined,
      isDirty: false,
      isTouched: false,
      needValidation: false,
      showPassword: false,
    };
  }

  validateInput = (value: string) => {
    const { validate } = this.props;
    const validationMessage = validate(value);
    this.setState({
      isValid: !Boolean(validationMessage),
      validationMessage,
    });
  }

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
  }

  handleOnBlur = (event: React.ChangeEvent<HTMLInputElement>) => {
    const value = event.target.value;
    this.validateInput(value);
    this.setState({
      isTouched: true,
      needValidation: true,
    });
  }

  togglePassword = () => {
    this.setState({
      showPassword: !this.state.showPassword,
    });
  }

  renderPasswordIcon = () => (
    <Button type="flat" onClick={this.togglePassword} className="crl-button__show-password">
      <img src={Eye} alt="Show password" />
    </Button>
  )

  render() {
    const { initialValue, placeholder, className, name, label, value, type, autoComplete } = this.props;
    const { isDirty, isValid, validationMessage, showPassword } = this.state;
    const textValue = isDirty ? value : initialValue;
    const inputType = type === "password" ? showPassword ? "text" : "password" : type;

    const classes = cn(
      className,
      "crl-text-input",
      {
        "crl-text-input--invalid": !isValid,
      },
    );
    return (
      <div className="crl-text-input__wrapper">
        {label && <label htmlFor={name} className="crl-text-input__label">{label}</label>}
        <input
          id="name"
          name={name}
          type={inputType}
          value={textValue}
          placeholder={placeholder}
          className={classes}
          onChange={this.handleOnTextChange}
          onBlur={this.handleOnBlur}
          autoComplete={autoComplete}
        />
        {type === "password" && this.renderPasswordIcon()}
        {
          !isValid && (
            <div className="crl-text-input__validation-container">
              <Text
                textType={TextTypes.Caption}
                className="crl-text-input__error-message"
              >
                {validationMessage}
              </Text>
            </div>
          )
        }
      </div>
    );
  }
}
