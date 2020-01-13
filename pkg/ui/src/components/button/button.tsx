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

import "./button.styl";

export interface ButtonProps {
  type?: "primary" | "secondary" | "flat";
  disabled?: boolean;
  size?: "default" | "small";
  children?: React.ReactNode;
}

export function Button(props: ButtonProps) {
  const { children, type, disabled, size } = props;

  const rootStyles = cn(
    "crl-button",
    `crl-button--type-${type}`,
    `crl-button--size-${size}`,
    {
      "crl-button--disabled": disabled,
    },
  );

  return (
    <button className={rootStyles}>{children}</button>
  );
}

Button.defaultProps = {
  type: "primary",
  disabled: false,
  size: "default",
};
