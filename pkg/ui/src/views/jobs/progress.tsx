import React from "react";
import {jobHasOneOfStatuses} from "src/views/jobs/jobStatusOptions";
import {JOB_STATUS_CANCELED, JOB_STATUS_FAILED, JOB_STATUS_SUCCEEDED, renamedStatuses} from "oss/src/views/jobs/index";
import classNames from "classnames";
import {Line} from "rc-progress";
import Job = cockroach.server.serverpb.JobsResponse.IJob;
import {cockroach} from "src/js/protos";

export class Progress extends React.PureComponent<{ job: Job }> {
  render() {
    if (jobHasOneOfStatuses(this.props.job, JOB_STATUS_SUCCEEDED, JOB_STATUS_FAILED, JOB_STATUS_CANCELED)) {
      const className = classNames("jobs-table__status", {
        "jobs-table__status--succeed": jobHasOneOfStatuses(this.props.job, JOB_STATUS_SUCCEEDED),
        "jobs-table__status--failed": jobHasOneOfStatuses(this.props.job, JOB_STATUS_FAILED, JOB_STATUS_CANCELED),
      });
      return (
        <div className={className}>
          {renamedStatuses(this.props.job.status)}
        </div>
      );
    }
    const percent = this.props.job.fraction_completed * 100;
    return (
      <div className="jobs-table__progress">
        {this.props.job.running_status
          ? <div className="jobs-table__running-status">{this.props.job.running_status}</div>
          : null}

        <div className="jobs-table__status--percentage"
             title={percent.toFixed(3) + "%"}>{percent.toFixed(1) + "%"}</div>
        <Line
          percent={percent}
          strokeWidth={11}
          trailWidth={11}
          strokeColor="#0788ff"
          trailColor="#d6dbe7"
          className="jobs-table__progress-bar"
        />
      </div>
    );
  }
}
