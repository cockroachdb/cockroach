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
    const started = TimestampToMoment(this.props.job.started);
    const finished = TimestampToMoment(this.props.job.finished);
    const modified = TimestampToMoment(this.props.job.modified);
    if (jobHasOneOfStatuses(this.props.job, JOB_STATUS_PENDING)) {
      return "Waiting for GG TCL";
    } else if (jobHasOneOfStatuses(this.props.job, JOB_STATUS_RUNNING)) {
      const fractionCompleted = this.props.job.fraction_completed;
      if (fractionCompleted > 0) {
        const duration = modified.diff(started);
        const remaining = duration / fractionCompleted - duration;
        return <span
          className="jobs-table__duration--right">{formatDuration(moment.duration(remaining)) + " remaining"}</span>;
      }
    } else if (jobHasOneOfStatuses(this.props.job, JOB_STATUS_SUCCEEDED)) {
      return "Duration: " + formatDuration(moment.duration(finished.diff(started)));
    }
    return null;
  }
}
