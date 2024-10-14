// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames";
import isEmpty from "lodash/isEmpty";
import React, { useRef, useEffect } from "react";

import { generateContainerClassnames } from "./util";

import "./input.scss";

export type InputProps = {
  type: "text" | "email" | "password" | "number" | undefined;
  id?: string;
  name?: string;
  className?: string;
  ariaLabel?: string;
  ariaLabelledBy?: string;
  width?: number;
  maxLength?: number;
  value?: string;
  disabled?: boolean;
  autoFocus?: boolean;
  tabIndex?: number;
  placeholder?: string;
  step?: number;
  help?: React.ReactNode;
  error?: React.ReactNode | boolean;
  inline?: boolean;
  required?: boolean;
  invalid?: boolean;
  label?: string | JSX.Element;
  prefix?: JSX.Element;
  suffix?: JSX.Element;
  onChange?: (event: React.ChangeEvent<HTMLInputElement>) => void;
  onKeyDown?: (event: React.KeyboardEvent<HTMLInputElement>) => void;
  onBlur?: (event: React.FocusEvent<HTMLInputElement> | undefined) => void;
  onFocus?: (event: React.FocusEvent<HTMLInputElement> | undefined) => void;
};

function Input(props: InputProps) {
  const {
    type,
    id,
    name,
    className,
    ariaLabel,
    ariaLabelledBy,
    width,
    maxLength,
    value,
    disabled,
    autoFocus,
    tabIndex,
    placeholder,
    step,
    help,
    error,
    inline,
    required,
    invalid,
    label,
    onChange,
    onKeyDown,
    onBlur,
    onFocus,
    prefix,
    suffix,
  } = props;
  // For all class names passed to Input, map them onto crl-input-container
  // (of the form `${className}__container`) so the container can be easily
  // targeted seperately from the input itself
  const classes = generateContainerClassnames(className);

  const helpMsg = !help ? null : (
    <div className="crl-input__message--info">{help}</div>
  );

  const errorMsg =
    !error || typeof error === "boolean" ? null : (
      <div className="crl-input__message--error">{error}</div>
    );

  /*
    For number inputs, the "wheel" event changes the value of the input, which
    we'd like to disable. However, as of Chrome 73, "wheel" is a passive event,
    and React doesn't support passive event listeners, so simply adding an
    onWheel callback that calls preventDefault doesn't work. As a workaround,
    a ref is used to directly add the passive event listener to the element.

    See this issue for more context on the Chrome change:
    https://github.com/facebook/react/issues/14856
    And this issue tracking supporting passive event listeners in React:
    https://github.com/facebook/react/issues/6436
  */

  const inputRef = useRef<HTMLInputElement>(null);
  const disableWheel = (event: Event) => event.preventDefault();

  useEffect(() => {
    if (type !== "number") return;
    inputRef.current?.addEventListener("wheel", disableWheel, {
      passive: false,
    });
  }, [inputRef, type]);

  return (
    <div
      className={classNames(`crl-input__container ${classes}`, {
        "crl-input--inline": inline,
      })}
      style={{ width }}
    >
      {!isEmpty(label) && (
        <label
          aria-label={name}
          className={classNames({
            "crl-input--required": required,
          })}
          htmlFor={id || name}
        >
          {label}
        </label>
      )}
      <div className="crl-input__affix-container">
        {prefix && <span className="crl-input__prefix">{prefix}</span>}
        <input
          ref={inputRef}
          id={id}
          name={name}
          type={type}
          maxLength={maxLength}
          placeholder={placeholder}
          onChange={evt => (onChange ? onChange(evt) : null)}
          className={classNames("crl-input", className, {
            "crl-input--prefix": props.prefix,
            "crl-input--suffix": props.suffix,
            "crl-input--invalid": error || invalid,
          })}
          aria-invalid={!!error || invalid}
          aria-required={required}
          aria-label={ariaLabel}
          aria-labelledby={ariaLabelledBy}
          value={value}
          disabled={disabled}
          autoFocus={autoFocus}
          tabIndex={tabIndex}
          onKeyDown={evt => (onKeyDown ? onKeyDown(evt) : null)}
          onBlur={onBlur}
          onFocus={onFocus}
          step={step}
        />
        {suffix && <span className="crl-input__suffix">{suffix}</span>}
      </div>
      <div className="crl-input__message">
        {errorMsg}
        {helpMsg}
      </div>
    </div>
  );
}

export default Input;
