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

import "./badge.styl";

export type BadgeStatus = "success" | "danger" | "default" | "info" | "warning";

export interface BadgeProps {
  text: string;
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
  const classes = cn("badge", `badge--size-${size}`, `badge--status-${status}`);
  const iconClasses = cn("badge__icon", `badge__icon--position-${iconPosition || "left"}`);
  return (
    <div className={classes}>
      { icon && <div className={iconClasses}>{icon}</div> }
      <div className="badge__text badge__text--no-wrap">
        { text }
      </div>
    </div>
  );
}
