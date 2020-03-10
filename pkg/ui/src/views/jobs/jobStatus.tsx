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
import {JobStatusBadge, ProgressBar} from "src/views/jobs/progressBar";
import {Duration} from "src/views/jobs/duration";
import Job = cockroach.server.serverpb.JobsResponse.IJob;
import {cockroach} from "src/js/protos";
import {JobStatusVisual, jobToVisual} from "src/views/jobs/jobStatusOptions";

export class JobStatus extends React.PureComponent<{ job: Job, lineWidth?: number }> {
  render() {
    const visualType = jobToVisual(this.props.job);

    switch (visualType) {
      case JobStatusVisual.BadgeOnly:
        return <JobStatusBadge jobStatus={this.props.job.status} />;
      case JobStatusVisual.BadgeWithDuration:
        return (
          <div>
            <JobStatusBadge jobStatus={this.props.job.status} />
            <span className="jobs-table__duration">
              <Duration job={this.props.job}/>
            </span>
          </div>
        );
      case JobStatusVisual.ProgressBarWithDuration:
        return (
          <div>
            <ProgressBar job={this.props.job} lineWidth={this.props.lineWidth || 11} showPercentage={true}/>
            <span className="jobs-table__duration">
              <Duration job={this.props.job}/>
            </span>
          </div>
        );
      case JobStatusVisual.BadgeWithMessage:
        return (
          <div>
            <JobStatusBadge jobStatus={this.props.job.status} />
            <span className="jobs-table__duration">
              {this.props.job.running_status}
            </span>
          </div>
        );
      default:
        return <JobStatusBadge jobStatus={this.props.job.status} />;
    }
  }
}
