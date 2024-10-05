// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import classNames from "classnames/bind";
import { CircleFilled } from "src/icon";
import { ExecutionStatus } from "./types";

import styles from "./executionStatusIcon.module.scss";

const cx = classNames.bind(styles);

export type StatusIconProps = {
  status: ExecutionStatus;
};

export const StatusIcon: React.FC<StatusIconProps> = ({ status }) => {
  const statusClassName =
    status === ExecutionStatus.Executing ? "executing" : "waiting";
  return <CircleFilled className={cx("status-icon", statusClassName)} />;
};
