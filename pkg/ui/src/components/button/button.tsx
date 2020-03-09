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
import Back from "assets/back-arrow.svg";

import "./button.styl";

export interface ButtonProps {
  type?: "primary" | "secondary" | "flat";
  disabled?: boolean;
  size?: "default" | "small";
  children?: React.ReactNode;
  icon?: () => React.ReactNode;
  iconPosition?: "left" | "right";
  onClick?: (event: React.MouseEvent<HTMLElement>) => void;
  className?: string;
}

export function Button(props: ButtonProps) {
  const { children, type, disabled, size, icon, iconPosition, onClick, className } = props;

  const rootStyles = cn(
    "crl-button",
    `crl-button--type-${type}`,
    `crl-button--size-${size}`,
    {
      "crl-button--disabled": disabled,
    },
    className,
  );

  const renderIcon = () => {
    if (icon === undefined) {
      return null;
    }
    return (
      <div className={`crl-button__icon--push-${iconPosition}`}>
        { icon() }
      </div>
    );
  };

  return (
    <button
      onClick={onClick}
      className={rootStyles}
      disabled={disabled}
    >
      <div className="crl-button__container">
        { iconPosition === "left" && renderIcon() }
        <div className="crl-button__content">
          {children}
        </div>
        { iconPosition === "right" && renderIcon() }
      </div>
    </button>
  );
}

Button.defaultProps = {
  onClick: () => {},
  type: "primary",
  disabled: false,
  size: "default",
  className: "",
  iconPosition: "left",
};

// tslint:disable-next-line: variable-name
export const BackIcon = () => <img src={Back} alt="back" />;
