// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Col, Divider, Icon, Row } from "antd";
import classNames from "classnames";
import _ from "lodash";
import moment from "moment";
import { TimestampToMoment } from "oss/src/util/convert";
import { Line } from "rc-progress";
import React from "react";
import Helmet from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps } from "react-router-dom";
import { cockroach } from "src/js/protos";
import { CachedDataReducerState, jobsKey, refreshJobs } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { getMatchParamByName } from "src/util/query";
import { formatDuration, showSetting, statusSetting, typeSetting } from ".";
import Loading from "../shared/components/loading";
import SqlBox from "../shared/components/sql/box";
import { SummaryCard } from "../shared/components/summaryCard";

import Job = cockroach.server.serverpb.JobsResponse.IJob;
import JobsRequest = cockroach.server.serverpb.JobsRequest;
import JobsResponse = cockroach.server.serverpb.JobsResponse;
import {
  JOB_STATUS_CANCELED, JOB_STATUS_FAILED,
  JOB_STATUS_PAUSED,
  JOB_STATUS_PENDING,
  JOB_STATUS_RUNNING, JOB_STATUS_SUCCEEDED, jobHasOneOfStatuses,
  renamedStatuses,
} from "src/views/jobs/jobStatusOptions";

interface JobsTableProps extends RouteComponentProps {
  status: string;
  show: string;
  type: number;
  refreshJobs: typeof refreshJobs;
  jobs: CachedDataReducerState<JobsResponse>;
  job: Job;
}

class JobDetails extends React.Component<JobsTableProps, {}> {
  refresh = (props = this.props) => {
    props.refreshJobs(new JobsRequest({
      status: props.status,
      type: props.type,
      limit: parseInt(props.show, 10),
    }));
  }

  componentWillMount() {
    this.refresh();
  }

  prevPage = () => this.props.history.goBack();

  renderProgress() {
    if (jobHasOneOfStatuses(this.props.job, JOB_STATUS_SUCCEEDED, JOB_STATUS_FAILED, JOB_STATUS_CANCELED)) {
      const className = classNames("jobs-table__status", {
        "jobs-table__status--succeed": jobHasOneOfStatuses(this.props.job, JOB_STATUS_SUCCEEDED),
        "jobs-table__status--failed": jobHasOneOfStatuses(this.props.job, JOB_STATUS_FAILED, JOB_STATUS_CANCELED),
      });
      return (
        <span className={className}>
          {renamedStatuses(this.props.job.status)}
        </span>
      );
    }
    const percent = this.props.job.fraction_completed * 100;
    return (
      <div className="jobs-table__progress">
        {this.props.job.running_status
          ? <div className="jobs-table__running-status">{this.props.job.running_status}</div>
          : null}

        <Line
          percent={percent}
          strokeWidth={2}
          trailWidth={2}
          strokeColor="#0788ff"
          trailColor="#d6dbe7"
          className="jobs-table__progress-bar"
        />
      </div>
    );
  }

  renderDuration() {
    const { job } = this.props;
    const started = TimestampToMoment(job.started);
    const finished = TimestampToMoment(job.finished);
    const modified = TimestampToMoment(job.modified);
    if (jobHasOneOfStatuses(this.props.job, JOB_STATUS_PENDING, JOB_STATUS_PAUSED)) {
      return _.capitalize(this.props.job.status);
    } else if (jobHasOneOfStatuses(this.props.job, JOB_STATUS_RUNNING)) {
      const fractionCompleted = this.props.job.fraction_completed;
      if (fractionCompleted > 0) {
        const duration = modified.diff(started);
        const remaining = duration / fractionCompleted - duration;
        return <span className="jobs-table__duration--right">{formatDuration(moment.duration(remaining)) + " remaining"}</span>;
      }
    } else if (jobHasOneOfStatuses(this.props.job, JOB_STATUS_SUCCEEDED)) {
      return <span>{"Duration: " + formatDuration(moment.duration(finished.diff(started)))}</span>;
    }
  }

  renderStatus = () => {
    const { job } = this.props;
    const percent = job.fraction_completed * 100;
    switch (job.status) {
      case JOB_STATUS_SUCCEEDED:
        return <div className="job-status__line">{this.renderProgress()} - {this.renderDuration()}</div>;
      case JOB_STATUS_FAILED || JOB_STATUS_CANCELED:
        return <div>{this.renderProgress()}</div>;
      default:
        return <div>
          {this.renderProgress()}
          <div className="job-status__line--percentage"><span>{percent.toFixed() + "%"} done</span><Divider type="vertical" />{this.renderDuration()}</div>
        </div>;
    }
  }

  renderContent = () => {
    const { job } = this.props;
    return (
      <Row gutter={16}>
        <Col className="gutter-row" span={16}>
          <SqlBox value={ job.description } />
          <SummaryCard>
            <h3 className="summary--card__status--title">Status</h3>
            {this.renderStatus()}
          </SummaryCard>
        </Col>
        <Col className="gutter-row" span={8}>
          <SummaryCard>
            <Row>
              <Col span={24}>
                <div className="summary--card__counting">
                  <h3 className="summary--card__counting--value">{TimestampToMoment(job.created).format("MM/DD/YYYY [at] hh:mma")}</h3>
                  <p className="summary--card__counting--label">Creation time</p>
                </div>
              </Col>
              <Col span={24}>
                <div className="summary--card__counting">
                  <h3 className="summary--card__counting--value">{job.username}</h3>
                  <p className="summary--card__counting--label">Users</p>
                </div>
              </Col>
            </Row>
          </SummaryCard>
        </Col>
      </Row>
    );
  }

  render() {
    const { job, match } = this.props;
    return (
      <div className="job-details">
        <Helmet title={ "Details | Job" } />
        <div className="section page--header">
          <div className="page--header__back-btn">
            <Icon type="arrow-left" /> <a onClick={this.prevPage}>Jobs</a>
          </div>
          <h1 className="page--header__title">{`Job ID: ${String(getMatchParamByName(match, "id"))}`}</h1>
        </div>
        <section className="section section--container">
          <Loading
            loading={_.isNil(job)}
            render={this.renderContent}
          />
        </section>
      </div>
    );
  }
}

const mapStateToProps = (state: AdminUIState, props: RouteComponentProps) => {
  const status = statusSetting.selector(state);
  const show = showSetting.selector(state);
  const type = typeSetting.selector(state);
  const key = jobsKey(status, type, parseInt(show, 10));
  const jobs = state.cachedData.jobs[key];
  // tslint:disable-next-line: no-shadowed-variable
  const job = _.filter(jobs ? jobs.data.jobs : [], job => String(job.id) === getMatchParamByName(props.match, "id"))[0];
  return {
    jobs, job, status, show, type,
  };
};

const mapDispatchToProps = {
  refreshJobs,
};

export default connect(mapStateToProps, mapDispatchToProps)(JobDetails);
