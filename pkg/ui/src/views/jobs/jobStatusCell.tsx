import React from "react";
import {cockroach} from "src/js/protos";
import Job = cockroach.server.serverpb.JobsResponse.IJob;
import {
  formatDuration,
  JOB_STATUS_CANCELED,
  JOB_STATUS_FAILED, JOB_STATUS_PAUSED,
  JOB_STATUS_PENDING, JOB_STATUS_RUNNING,
  JOB_STATUS_SUCCEEDED,
  renamedStatuses,
} from "src/views/jobs/index";
import {Line} from "rc-progress";
import {TimestampToMoment} from "oss/src/util/convert";
import moment from "moment";
import {ToolTipWrapper} from "oss/src/views/shared/components/toolTip";
import {DATE_FORMAT} from "oss/src/util/format";
import _ from "lodash";
import classNames from "classnames";

export class JobStatusCell extends React.Component<{ job: Job }, {}> {
  is(...statuses: string[]) {
    return statuses.indexOf(this.props.job.status) !== -1;
  }
  renderProgress() {
    if (this.is(JOB_STATUS_SUCCEEDED, JOB_STATUS_FAILED, JOB_STATUS_CANCELED)) {
      const className = classNames("jobs-table__status", {
        "jobs-table__status--succeed": this.is(JOB_STATUS_SUCCEEDED),
        "jobs-table__status--failed": this.is(JOB_STATUS_FAILED, JOB_STATUS_CANCELED),
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

        <div className="jobs-table__status--percentage" title={percent.toFixed(3) + "%"}>{percent.toFixed(1) + "%"}</div>
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

  renderDuration() {
    const started = TimestampToMoment(this.props.job.started);
    const finished = TimestampToMoment(this.props.job.finished);
    const modified = TimestampToMoment(this.props.job.modified);
    if (this.is(JOB_STATUS_PENDING, JOB_STATUS_PAUSED)) {
      return _.capitalize(this.props.job.status);
    } else if (this.is(JOB_STATUS_RUNNING)) {
      const fractionCompleted = this.props.job.fraction_completed;
      if (fractionCompleted > 0) {
        const duration = modified.diff(started);
        const remaining = duration / fractionCompleted - duration;
        return <span className="jobs-table__duration--right">{formatDuration(moment.duration(remaining)) + " remaining"}</span>;
      }
    } else if (this.is(JOB_STATUS_SUCCEEDED)) {
      return "Duration: " + formatDuration(moment.duration(finished.diff(started)));
    }
  }

  renderFractionCompleted() {
    return (
      <div>
        {this.renderProgress()}
        <span className="jobs-table__duration">{this.renderDuration()}</span>
      </div>
    );
  }

  renderHighwater() {
    const highwater = this.props.job.highwater_timestamp;
    const tooltip = this.props.job.highwater_decimal;
    let highwaterMoment = moment(highwater.seconds.toNumber() * 1000);
    // It's possible due to client clock skew that this timestamp could be in
    // the future. To avoid confusion, set a maximum bound of now.
    const now = moment();
    if (highwaterMoment.isAfter(now)) {
      highwaterMoment = now;
    }
    return (
      <ToolTipWrapper text={`System Time: ${tooltip}`}>
        High-water Timestamp: {highwaterMoment.format(DATE_FORMAT)}
      </ToolTipWrapper>
    );
  }

  render() {
    if (this.props.job.highwater_timestamp) {
      return this.renderHighwater();
    }
    return this.renderFractionCompleted();
  }
}
