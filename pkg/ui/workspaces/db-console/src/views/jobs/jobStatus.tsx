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
import {
  JobStatusBadge,
  RetryingStatusBadge,
  ProgressBar,
} from "src/views/jobs/progressBar";
import { Duration } from "src/views/jobs/duration";
import Job = cockroach.server.serverpb.IJobResponse;
import { cockroach } from "src/js/protos";
import {
  JobStatusVisual,
  jobToVisual,
  isRetrying,
} from "src/views/jobs/jobStatusOptions";
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
          <Duration job={job} className="jobs-table__duration" />
        </div>
      );
    case JobStatusVisual.ProgressBarWithDuration: {
      const jobIsRetrying = isRetrying(job.status);
      return (
        <div>
          <ProgressBar
            job={job}
            lineWidth={lineWidth || 11}
            showPercentage={true}
          />
          <Duration job={job} className={cn("jobs-table__duration")} />
          {jobIsRetrying && <RetryingStatusBadge />}
          {job.running_status && (
            <div className="jobs-table__running-status">
              {job.running_status}
            </div>
          )}
        </div>
      );
    }
    case JobStatusVisual.BadgeWithMessage:
      return (
        <div>
          <JobStatusBadge jobStatus={job.status} />
          <span className="jobs-table__running-status">
            {job.running_status}
          </span>
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
    case JobStatusVisual.BadgeWithRetrying:
      return (
        <div className="jobs-table__two-statuses">
          <JobStatusBadge jobStatus={job.status} />
          <RetryingStatusBadge />
        </div>
      );
    default:
      return <JobStatusBadge jobStatus={job.status} />;
  }
};
