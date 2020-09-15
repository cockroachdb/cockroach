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
import {TimestampToMoment} from "src/util/convert";
import {
  JOB_STATUS_PENDING,
  JOB_STATUS_RUNNING, JOB_STATUS_SUCCEEDED,
  jobHasOneOfStatuses,
} from "src/views/jobs/jobStatusOptions";
import {
  formatDuration,

} from "src/views/jobs/index";
import _ from "lodash";
import moment from "moment";
import Job = cockroach.server.serverpb.JobsResponse.IJob;
import {cockroach} from "src/js/protos";

export class Duration extends React.PureComponent<{ job: Job }> {
  render() {
    if (jobHasOneOfStatuses(this.props.job, JOB_STATUS_PENDING)) {
      return "Waiting for GG TCL";
    }
    // Parse timestamp to default value NULL instead of Date.now.
    // Conversion dates to Date.now causes traling dates and constant
    // duration increase even when job is finished.
    const started = TimestampToMoment(this.props.job.started, null);
    const modified = TimestampToMoment(this.props.job.modified, null);
    const finished = TimestampToMoment(this.props.job.finished, null);

    if (!started) {
      return null;
    }
    if (jobHasOneOfStatuses(this.props.job, JOB_STATUS_RUNNING) && !!modified) {
      const fractionCompleted = this.props.job.fraction_completed;
      if (fractionCompleted > 0) {
        const duration = modified.diff(started);
        const remaining = duration / fractionCompleted - duration;
        return <span
          className="jobs-table__duration--right">{formatDuration(moment.duration(remaining)) + " remaining"}</span>;
      }
    } else if (jobHasOneOfStatuses(this.props.job, JOB_STATUS_SUCCEEDED) && !!finished) {
      return "Duration: " + formatDuration(moment.duration(finished.diff(started)));
    }
    return null;
  }
}
