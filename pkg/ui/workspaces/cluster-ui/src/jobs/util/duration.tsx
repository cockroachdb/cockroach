// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import moment from "moment-timezone";
import React from "react";
import { TimestampToMoment } from "src/util";

import { JOB_STATUS_SUCCEEDED, isRunning } from "./jobOptions";

type Job = cockroach.server.serverpb.IJobResponse;

export const formatDuration = (d: moment.Duration): string =>
  [Math.floor(d.asHours()).toFixed(0), d.minutes(), d.seconds()]
    .map(c => (c < 10 ? ("0" + c).slice(-2) : c))
    .join(":");

export class Duration extends React.PureComponent<{
  job: Job;
  className?: string;
}> {
  render(): React.ReactElement {
    const { job, className } = this.props;
    // Parse timestamp to default value NULL instead of Date.now.
    // Conversion dates to Date.now causes trailing dates and constant
    // duration increase even when job is finished.
    const startedAt = TimestampToMoment(job.started, null);
    const modifiedAt = TimestampToMoment(job.modified, null);
    const finishedAt = TimestampToMoment(job.finished, null);

    if (isRunning(job.status)) {
      const fractionCompleted = job.fraction_completed;
      if (fractionCompleted > 0) {
        const duration = modifiedAt.diff(startedAt);
        const remaining = duration / fractionCompleted - duration;
        return (
          <span className={className}>
            {formatDuration(moment.duration(remaining)) + " remaining"}
          </span>
        );
      }
      return null;
    } else if (job.status == JOB_STATUS_SUCCEEDED) {
      return (
        <span className={className}>
          {"Duration: " +
            formatDuration(moment.duration(finishedAt.diff(startedAt)))}
        </span>
      );
    }
    return null;
  }
}
