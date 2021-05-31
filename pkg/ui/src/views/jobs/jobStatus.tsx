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
import classNames from "classnames/bind";
import { JobStatusBadge, ProgressBar } from "src/views/jobs/progressBar";
import { Duration } from "src/views/jobs/duration";
import Job = cockroach.server.serverpb.IJobResponse;
import { cockroach } from "src/js/protos";
import { JobStatusVisual, jobToVisual } from "src/views/jobs/jobStatusOptions";
import { InlineAlert } from "src/components";
import styles from "./jobStatus.module.styl";

export interface JobStatusProps {
  job: Job;
  lineWidth?: number;
  compact?: boolean;
}

const cn = classNames.bind(styles);

export const JobStatus: React.FC<JobStatusProps> = ({
  job,
  compact,
  lineWidth,
}) => {
  const visualType = jobToVisual(job);

  switch (visualType) {
    case JobStatusVisual.BadgeOnly:
      return <JobStatusBadge jobStatus={job.status} />;
    case JobStatusVisual.BadgeWithDuration:
      return (
        <div>
          <JobStatusBadge jobStatus={job.status} />
          <span className="jobs-table__duration">
            <Duration job={job} />
          </span>
        </div>
      );
    case JobStatusVisual.ProgressBarWithDuration:
      return (
        <div>
          <ProgressBar
            job={job}
            lineWidth={lineWidth || 11}
            showPercentage={true}
          />
          <span className="jobs-table__duration">
            <Duration job={job} />
          </span>
        </div>
      );
    case JobStatusVisual.BadgeWithMessage:
      return (
        <div>
          <JobStatusBadge jobStatus={job.status} />
          <span className="jobs-table__duration">{job.running_status}</span>
        </div>
      );
    case JobStatusVisual.BadgeWithErrorMessage:
      return (
        <div>
          <JobStatusBadge jobStatus={job.status} />
          {!compact && (
            <InlineAlert
              title={job.error}
              intent="error"
              className={cn("inline-message")}
            />
          )}
        </div>
      );
    default:
      return <JobStatusBadge jobStatus={job.status} />;
  }
};
