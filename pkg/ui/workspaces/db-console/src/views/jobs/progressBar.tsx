// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { jobStatusToBadgeStatus } from "src/views/jobs/jobStatusOptions";
import Job = cockroach.server.serverpb.IJobResponse;
import { cockroach } from "src/js/protos";
import { Badge } from "src/components";
import { Line } from "rc-progress";

export class JobStatusBadge extends React.PureComponent<{ jobStatus: string }> {
  render() {
    const jobStatus = this.props.jobStatus;
    const badgeStatus = jobStatusToBadgeStatus(jobStatus);
    return <Badge status={badgeStatus} text={jobStatus} />;
  }
}

export class ProgressBar extends React.PureComponent<{
  job: Job;
  lineWidth: number;
  showPercentage: boolean;
}> {
  render() {
    const percent = this.props.job.fraction_completed * 100;
    return (
      <div className="jobs-table__progress">
        {this.props.job.running_status ? (
          <div className="jobs-table__running-status">
            {this.props.job.running_status}
          </div>
        ) : null}

        {this.props.showPercentage ? (
          <div
            className="jobs-table__status--percentage"
            title={percent.toFixed(3) + "%"}
          >
            {percent.toFixed(1) + "%"}
          </div>
        ) : null}
        <Line
          percent={percent}
          strokeWidth={this.props.lineWidth}
          trailWidth={this.props.lineWidth}
          strokeColor="#0788ff"
          trailColor="#d6dbe7"
          className="jobs-table__progress-bar"
        />
      </div>
    );
  }
}
