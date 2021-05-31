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
import { TimestampToMoment } from "src/util/convert";
import {
  JOB_STATUS_RUNNING,
  JOB_STATUS_SUCCEEDED,
} from "src/views/jobs/jobStatusOptions";
import { formatDuration } from "src/views/jobs/index";
import moment from "moment";
import Job = cockroach.server.serverpb.IJobResponse;
import { cockroach } from "src/js/protos";

export class Duration extends React.PureComponent<{ job: Job }> {
  render() {
    const { job } = this.props;
    // Parse timestamp to default value NULL instead of Date.now.
    // Conversion dates to Date.now causes traling dates and constant
    // duration increase even when job is finished.
    const startedAt = TimestampToMoment(job.started, null);
    const modifiedAt = TimestampToMoment(job.modified, null);
    const finishedAt = TimestampToMoment(job.finished, null);

    switch (job.status) {
      case JOB_STATUS_RUNNING: {
        const fractionCompleted = job.fraction_completed;
        if (fractionCompleted > 0) {
          const duration = modifiedAt.diff(startedAt);
          const remaining = duration / fractionCompleted - duration;
          return (
            <span className="jobs-table__duration--right">
              {formatDuration(moment.duration(remaining)) + " remaining"}
            </span>
          );
        }
        return null;
      }
      case JOB_STATUS_SUCCEEDED:
        return (
          "Duration: " +
          formatDuration(moment.duration(finishedAt.diff(startedAt)))
        );
      default:
        return null;
    }
  }
}
