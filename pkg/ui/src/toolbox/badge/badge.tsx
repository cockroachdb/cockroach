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

export interface BadgeProps {
  size?: "small" | "medium" | "large";
  status?: "success" | "danger" | "default" | "info" | "warning";
  tag?: boolean;
  icon?: React.ReactNode;
  iconPosition?: "left" | "right";
}

Badge.defaultProps = {
  size: "medium",
  status: "default",
  tag: false,
};

export function Badge(_props: BadgeProps) {
  // const { size, status, tag } = props;
  return (
    <div className="badge">

    </div>
  );
}
