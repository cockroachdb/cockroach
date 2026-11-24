// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { InlineAlert } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import React from "react";

import styles from "../jobs.module.scss";

import { Duration } from "./duration";
import { JOB_STATUS_FAILED, JobStatusVisual, jobToVisual } from "./jobOptions";
import {
  JobStatusBadge,
  ProgressBar,
  RetryingStatusBadge,
} from "./progressBar";

const cx = classNames.bind(styles);

type Job = cockroach.server.serverpb.IJobResponse;

export interface JobStatusProps {
  job: Job;
  lineWidth?: number;
  compact?: boolean;
  hideDuration?: boolean;
}

export const JobStatus: React.FC<JobStatusProps> = ({
  job,
  compact,
  lineWidth,
  hideDuration = false,
}) => {
  const visualType = jobToVisual(job);
  switch (visualType) {
    case JobStatusVisual.BadgeOnly:
      return (
        <JobStatusBadge
          jobStatus={job.status}
          advisory={job.error || job.running_status}
        />
      );
    case JobStatusVisual.BadgeWithDuration:
      return (
        <div>
          <JobStatusBadge jobStatus={job.status} />
          {!hideDuration && (
            <Duration job={job} className={cx("jobs-table__duration")} />
          )}
        </div>
      );
    case JobStatusVisual.ProgressBarWithDuration: {
      return (
        <div>
          <ProgressBar
            job={job}
            lineWidth={lineWidth || 11}
            showPercentage={true}
          />
          <Duration job={job} className={cx("jobs-table__duration")} />
          {job.running_status && (
            <div className={cx("jobs-table__running-status")}>
              {job.running_status}
            </div>
          )}
        </div>
      );
    }
    case JobStatusVisual.BadgeWithMessage:
      return (
        <div>
          <JobStatusBadge
            jobStatus={job.status}
            advisory={job.error || job.running_status}
          />
          <span className={cx("jobs-table__running-status")}>
            {job.running_status}
          </span>
        </div>
      );
    case JobStatusVisual.BadgeWithErrorMessage:
      return (
        <div>
          <JobStatusBadge
            jobStatus={job.status}
            advisory={job.error || job.running_status}
          />
          {!compact && (
            <InlineAlert
              title={job.error || job.running_status}
              intent={job.status === JOB_STATUS_FAILED ? "danger" : "warning"}
              className={cx("inline-message")}
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
