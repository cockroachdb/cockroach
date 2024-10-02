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

export class JobStatusBadge extends React.PureComponent<{ jobStatus: string }> {
  render(): React.ReactElement {
    const jobStatus = this.props.jobStatus;
    const badgeStatus = jobStatusToBadgeStatus(jobStatus);
    return <Badge status={badgeStatus} text={jobStatus} />;
  }
}

export class RetryingStatusBadge extends React.PureComponent {
  render(): React.ReactElement {
    return <Badge status="warning" text="retrying" />;
  }
}

export class ProgressBar extends React.PureComponent<{
  job: Job;
  lineWidth: number;
  showPercentage: boolean;
}> {
  render(): React.ReactElement {
    const percent = this.props.job.fraction_completed * 100;
    return (
      <div className={cx("jobs-table__progress")}>
        <Line
          percent={percent}
          strokeWidth={this.props.lineWidth}
          trailWidth={this.props.lineWidth}
          strokeColor="#0055ff"
          trailColor="#d6dbe7"
          className={cx("jobs-table__progress-bar")}
        />
        {this.props.showPercentage ? (
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
}
