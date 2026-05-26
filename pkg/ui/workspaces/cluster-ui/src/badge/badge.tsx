// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import * as React from "react";

import styles from "./badge.module.scss";

export type BadgeStatus = "success" | "danger" | "default" | "info" | "warning";

export interface BadgeProps {
  text: React.ReactNode;
  size?: "small" | "medium" | "large";
  status?: BadgeStatus;
  icon?: React.ReactNode;
  iconPosition?: "left" | "right";
  forceUpperCase: boolean;
}

const cx = classNames.bind(styles);

export function Badge(props: BadgeProps): React.ReactElement {
  const { size, status, icon, iconPosition, text, forceUpperCase } = props;
  const classes = cx(
    "badge",
    `badge--size-${size}`,
    `badge--status-${status}`,
    {
      "badge--uppercase": forceUpperCase,
    },
  );
  const iconClasses = cx(
    "badge__icon",
    `badge__icon--position-${iconPosition || "left"}`,
  );
  return (
    <div className={classes}>
      {icon && <div className={iconClasses}>{icon}</div>}
      <div className={cx("badge__text", "badge__text--no-wrap")}>{text}</div>
    </div>
  );
}

Badge.defaultProps = {
  size: "medium",
  status: "default",
  forceUpperCase: true,
};
