// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import classNames from "classnames/bind";
import React from "react";

import { CircleFilled } from "src/icon";

import styles from "./executionStatusIcon.module.scss";
import { ExecutionStatus } from "./types";

const cx = classNames.bind(styles);

export type StatusIconProps = {
  status: ExecutionStatus;
};

export const StatusIcon: React.FC<StatusIconProps> = ({ status }) => {
  const statusClassName =
    status === ExecutionStatus.Executing ? "executing" : "waiting";
  return <CircleFilled className={cx("status-icon", statusClassName)} />;
};
