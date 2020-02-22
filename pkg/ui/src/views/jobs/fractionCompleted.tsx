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
import {Progress} from "src/views/jobs/progress";
import {Duration} from "src/views/jobs/duration";
import Job = cockroach.server.serverpb.JobsResponse.IJob;
import {cockroach} from "src/js/protos";

export class FractionCompleted extends React.PureComponent<{ job: Job }> {
  render() {
    return (
      <div>
        <Progress job={this.props.job} lineWidth={11} showPercentage={true}/>
        <span className="jobs-table__duration">
          <Duration job={this.props.job}/>
        </span>
      </div>
    );
  }
}
