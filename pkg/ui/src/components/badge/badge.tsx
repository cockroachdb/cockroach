// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as React from "react";
import cn from "classnames";

import styles from "./badge.module.styl";

export type BadgeStatus = "success" | "danger" | "default" | "info" | "warning";

export interface BadgeProps {
  text: React.ReactNode;
  size?: "small" | "medium" | "large";
  status?: BadgeStatus;
  tag?: boolean; // TODO (koorosh): Tag behavior isn't implemented yet.
  icon?: React.ReactNode;
  iconPosition?: "left" | "right";
}

Badge.defaultProps = {
  size: "medium",
  status: "default",
  tag: false,
};

export function Badge(props: BadgeProps) {
  const { size, status, icon, iconPosition, text } = props;
  const classes = cn(
    styles[`badge`],
    styles[`badge--size-${size}`],
    styles[`badge--status-${status}`],
  );
  const iconClasses = cn(
    styles[`badge__icon`],
    styles[`badge__icon--position-${iconPosition || "left"}`],
  );
  return (
    <div className={classes}>
      { icon && <div className={iconClasses}>{icon}</div> }
      <div
        className={cn(
          styles[`badge__text`],
          styles[`badge__text--no-wrap`],
        )}
      >
        { text }
      </div>
    </div>
  );
}
