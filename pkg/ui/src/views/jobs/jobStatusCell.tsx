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
import {cockroach} from "src/js/protos";
import {HighwaterTimestamp} from "src/views/jobs/highwaterTimestamp";
import {JobStatus} from "./jobStatus";
import Job = cockroach.server.serverpb.JobsResponse.IJob;

export class JobStatusCell extends React.Component<{ job: Job, lineWidth?: number }, {}> {
  render() {
    if (this.props.job.highwater_timestamp) {
      return <HighwaterTimestamp highwater={this.props.job.highwater_timestamp} tooltip={this.props.job.highwater_decimal}/>;
    }
    return <JobStatus job={this.props.job} lineWidth={this.props.lineWidth} />;
  }
}
