// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import classNames from "classnames/bind";
import { Line } from "rc-progress";
import React from "react";

import { Badge } from "src/badge";

import styles from "../jobs.module.scss";

import { jobStatusToBadgeStatus } from "./jobOptions";

const cx = classNames.bind(styles);

type Job = cockroach.server.serverpb.IJobResponse;

interface JobStatusBadgeProps {
  jobStatus: string;
  advisory?: string;
}

export function JobStatusBadge({
  jobStatus,
  advisory,
}: JobStatusBadgeProps): React.ReactElement {
  const badgeStatus = jobStatusToBadgeStatus(jobStatus, advisory);
  return <Badge status={badgeStatus} text={jobStatus} />;
}

export function RetryingStatusBadge(): React.ReactElement {
  return <Badge status="warning" text="retrying" />;
}

interface ProgressBarProps {
  job: Job;
  lineWidth: number;
  showPercentage: boolean;
}

export function ProgressBar({
  job,
  lineWidth,
  showPercentage,
}: ProgressBarProps): React.ReactElement {
  const percent = job.fraction_completed * 100;
  return (
    <div className={cx("jobs-table__progress")}>
      <Line
        percent={percent}
        strokeWidth={lineWidth}
        trailWidth={lineWidth}
        strokeColor="#0055ff"
        trailColor="#d6dbe7"
        className={cx("jobs-table__progress-bar")}
      />
      {showPercentage ? (
        <div
          className={cx("jobs-table__status--percentage")}
          title={percent.toFixed(3) + "%"}
        >
          {percent.toFixed(1) + "%"}
        </div>
      ) : null}
    </div>
  );
}
