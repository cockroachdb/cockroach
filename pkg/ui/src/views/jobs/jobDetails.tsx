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
import _ from "lodash";
import { TimestampToMoment } from "oss/src/util/convert";
import React from "react";
import Helmet from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps } from "react-router-dom";
import { cockroach } from "src/js/protos";
import { CachedDataReducerState, jobsKey, refreshJobs } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { getMatchParamByName } from "src/util/query";
import { showSetting, statusSetting, typeSetting } from ".";
import Loading from "../shared/components/loading";
import SqlBox from "../shared/components/sql/box";
import { SummaryCard } from "../shared/components/summaryCard";

import Job = cockroach.server.serverpb.JobsResponse.IJob;
import JobsRequest = cockroach.server.serverpb.JobsRequest;
import JobsResponse = cockroach.server.serverpb.JobsResponse;
import {
  JOB_STATUS_CANCELED, JOB_STATUS_FAILED,
  JOB_STATUS_SUCCEEDED,
} from "src/views/jobs/jobStatusOptions";
import {Duration} from "oss/src/views/jobs/duration";
import {Progress} from "oss/src/views/jobs/progress";

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

  renderStatus = () => {
    const { job } = this.props;
    const percent = job.fraction_completed * 100;
    switch (job.status) {
      case JOB_STATUS_SUCCEEDED:
        return (
          <div className="job-status__line">
            <Progress job={this.props.job} lineWidth={2} showPercentage={false} /> - <Duration job={this.props.job} />
          </div>
        );
      case JOB_STATUS_FAILED || JOB_STATUS_CANCELED:
        return (
          <div>
            <Progress job={this.props.job} lineWidth={2} showPercentage={false} />
          </div>
        );
      default:
        return (
          <div>
            <Progress job={this.props.job} lineWidth={2} showPercentage={false} />
            <div className="job-status__line--percentage">
              <span>{percent.toFixed() + "%"} done</span>
              <Divider type="vertical" /><Duration job={this.props.job} />
            </div>
          </div>
        );
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
                  <h3 className="summary--card__counting--value">{TimestampToMoment(job.created).format("MMM DD, YYYY [at] hh:mma")}</h3>
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
