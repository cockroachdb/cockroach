import React from "react";
import {cockroach} from "src/js/protos";
import {HighwaterTimestamp} from "oss/src/views/jobs/highwaterTimestamp";
import {FractionCompleted} from "oss/src/views/jobs/fractionCompleted";
import Job = cockroach.server.serverpb.JobsResponse.IJob;

export class JobStatusCell extends React.Component<{ job: Job }, {}> {
  render() {
    if (this.props.job.highwater_timestamp) {
      return <HighwaterTimestamp highwater={this.props.job.highwater_timestamp} tooltip={this.props.job.highwater_decimal}/>;
    }
    return <FractionCompleted job={this.props.job}/>;
  }
}
