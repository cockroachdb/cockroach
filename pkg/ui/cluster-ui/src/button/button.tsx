// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { ButtonHTMLAttributes } from "react";
import classNames from "classnames/bind";
import styles from "./button.module.scss";

export interface ButtonProps {
  type?: "primary" | "secondary" | "flat" | "unstyled-link";
  disabled?: boolean;
  textAlign?: "left" | "right" | "center";
  size?: "default" | "small";
  children?: React.ReactNode;
  icon?: React.ReactNode;
  iconPosition?: "left" | "right";
  onClick?: (event: React.MouseEvent<HTMLElement>) => void;
  className?: string;
  buttonType?: ButtonHTMLAttributes<HTMLButtonElement>["type"];
  tabIndex?: ButtonHTMLAttributes<HTMLButtonElement>["tabIndex"];
}

const cx = classNames.bind(styles);

export function Button(props: ButtonProps) {
  const {
    children,
    type,
    disabled,
    size = "default",
    icon,
    iconPosition,
    onClick,
    className,
    buttonType,
    tabIndex,
    textAlign,
  } = props;

  const rootStyles = cx(
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
      <div
        className={cx("crl-button__icon", {
          [`crl-button__icon--push-${iconPosition}`]: !!children,
        })}
      >
        {icon}
      </div>
    );
  };

  return (
    <button
      onClick={onClick}
      className={rootStyles}
      disabled={disabled}
      type={buttonType}
      tabIndex={tabIndex}
    >
      <div className={cx("crl-button__container")}>
        {iconPosition === "left" && renderIcon()}
        <div className={cx("crl-button__content")} style={{ textAlign }}>
          {children}
        </div>
        {iconPosition === "right" && renderIcon()}
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
  buttonType: "button",
  textAlign: "left",
};
