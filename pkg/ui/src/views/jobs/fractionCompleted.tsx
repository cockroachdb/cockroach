import React from "react";
import {Progress} from "src/views/jobs/progress";
import {Duration} from "src/views/jobs/duration";
import Job = cockroach.server.serverpb.JobsResponse.IJob;
import {cockroach} from "src/js/protos";

export class FractionCompleted extends React.PureComponent<{ job: Job }> {
  render() {
    return (
      <div>
        <Progress job={this.props.job}/>
        <span className="jobs-table__duration">
          <Duration job={this.props.job}/>
        </span>
      </div>
    );
  }
}
