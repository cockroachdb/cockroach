// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import cn from "classnames";
import React, { useState } from "react";

import EyeOff from "assets/eye-off.svg";
import Eye from "assets/eye.svg";

import { Button } from "../button";
import "./input.scss";

interface PasswordInputProps {
  onChange: (value: string) => void;
  value: string;
  placeholder?: string;
  className?: string;
  name?: string;
  label?: string;
}

export const PasswordInput: React.FC<PasswordInputProps> = ({
  onChange,
  value,
  placeholder,
  className,
  name,
  label,
}) => {
  const [showPassword, setShowPassword] = useState(false);

  const handleOnTextChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    onChange(event.target.value);
  };

  const togglePassword = () => {
    setShowPassword(prev => !prev);
  };

  const inputType = showPassword ? "text" : "password";
  const classes = cn(className, "crl-input", "crl-input__password");

  return (
    <div className="crl-input__wrapper">
      {label && (
        <label htmlFor={name} className="crl-input__label">
          {label}
        </label>
      )}
      <input
        name={name}
        type={inputType}
        value={value}
        placeholder={placeholder}
        className={classes}
        onChange={handleOnTextChange}
      />
      <Button
        tabIndex={-1}
        type="flat"
        onClick={togglePassword}
        className="crl-button__show-password"
      >
        <img src={showPassword ? EyeOff : Eye} alt="Toggle Password" />
      </Button>
    </div>
  );
};
