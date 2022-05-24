// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Col, Icon, Row } from "antd";
import _ from "lodash";
import Long from "long";
import React from "react";
import Helmet from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps } from "react-router-dom";
import { cockroach } from "src/js/protos";
import { jobRequestKey, refreshJob } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { getMatchParamByName } from "src/util/query";
import {
  Loading,
  SortedTable,
  util,
  SortSetting,
} from "@cockroachlabs/cluster-ui";
import SqlBox from "../shared/components/sql/box";
import { SummaryCard } from "../shared/components/summaryCard";

import Job = cockroach.server.serverpb.JobResponse;
import JobRequest = cockroach.server.serverpb.JobRequest;
import ExecutionFailure = cockroach.server.serverpb.JobResponse.IExecutionFailure;
import { Button } from "@cockroachlabs/cluster-ui";
import { ArrowLeft } from "@cockroachlabs/icons";
import { DATE_FORMAT } from "src/util/format";
import { JobStatusCell } from "./jobStatusCell";
import "src/views/shared/components/summaryCard/styles.styl";
import * as protos from "src/js/protos";
import { LocalSetting } from "src/redux/localsettings";
import styles from "./jobDetails.module.styl";
import classNames from "classnames/bind";
const cx = classNames.bind(styles);

export interface JobDetailsProps extends RouteComponentProps {
  job: Job;
  sort: SortSetting;
  refreshJob: typeof refreshJob;
  setSort: (value: SortSetting) => void;
}

export class JobDetails extends React.Component<JobDetailsProps, {}> {
  refresh = (props = this.props) => {
    props.refreshJob(
      new JobRequest({
        job_id: Long.fromString(getMatchParamByName(props.match, "id")),
      }),
    );
  };

  componentDidMount() {
    this.refresh();
  }

  prevPage = () => this.props.history.goBack();

  renderJobErrors = (job: Job) => {
    // Creating this differently named type to be clear that this table data contains more  errors than just those in
    // the execution_failures field. Ignoring "status" since the table does not need it.
    type JobError = Pick<ExecutionFailure, "start" | "end" | "error">;
    const errors: JobError[] = job.error
      ? [
          {
            start: job.started,
            end: job.finished,
            error: job.error,
          },
          ...job.execution_failures,
        ]
      : job.execution_failures;

    const columns = [
      {
        title: "Error start time (UTC)",
        name: "startTime",
        cell: (error: JobError) =>
          util.TimestampToMoment(error.start).format("MMM D, YYYY [at] h:mm A"),
        sort: (error: JobError) =>
          util.TimestampToMoment(error.start).valueOf(),
      },
      {
        title: "Error end time (UTC)",
        name: "endTime",
        cell: (error: JobError) =>
          util.TimestampToMoment(error.end).format("MMM D, YYYY [at] h:mm A"),
        sort: (error: JobError) => util.TimestampToMoment(error.end).valueOf(),
      },
      {
        title: "Error message",
        name: "message",
        cell: (error: JobError) => error.error,
        sort: (error: JobError) => error.error,
      },
    ];
    return (
      <section>
        <h3 className="summary--card__status--title">Job errors</h3>
        <SortedTable
          data={errors}
          columns={columns}
          sortSetting={this.props.sort}
          onChangeSortSetting={this.props.setSort}
          renderNoResult={
            <div>
              <Icon className={cx("no-errors__icon")} type={"check-circle"} />
              <span className={cx("no-errors__message")}>
                No job errors occured.
              </span>
            </div>
          }
        />
      </section>
    );
  };

  renderContent = () => {
    const { job } = this.props;
    return (
      <>
        <Row gutter={16}>
          <Col className="gutter-row" span={16}>
            <SqlBox value={job.description} />
            <SummaryCard>
              <h3 className="summary--card__status--title">Status</h3>
              <JobStatusCell job={job} lineWidth={1.5} />
            </SummaryCard>
          </Col>
          <Col className="gutter-row" span={8}>
            <SummaryCard>
              <Row>
                <Col span={24}>
                  <div className="summary--card__counting">
                    <h3 className="summary--card__counting--value">
                      {util.TimestampToMoment(job.created).format(DATE_FORMAT)}
                    </h3>
                    <p className="summary--card__counting--label">
                      Creation time
                    </p>
                  </div>
                </Col>
                <Col span={24}>
                  <div className="summary--card__counting">
                    <h3 className="summary--card__counting--value">
                      {job.username}
                    </h3>
                    <p className="summary--card__counting--label">Users</p>
                  </div>
                </Col>
              </Row>
            </SummaryCard>
          </Col>
        </Row>
        <Row>{this.renderJobErrors(job)}</Row>
      </>
    );
  };

  render() {
    const { job, match } = this.props;
    return (
      <div className="job-details">
        <Helmet title={"Details | Job"} />
        <div className="section page--header">
          <Button
            onClick={this.prevPage}
            type="unstyled-link"
            size="small"
            icon={<ArrowLeft fontSize={"10px"} />}
            iconPosition="left"
            className="small-margin"
          >
            Jobs
          </Button>
          <h3 className="page--header__title">{`Job ID: ${String(
            getMatchParamByName(match, "id"),
          )}`}</h3>
        </div>
        <section className="section section--container">
          <Loading
            loading={_.isNil(job)}
            page={"job details"}
            render={this.renderContent}
          />
        </section>
      </div>
    );
  }
}

export const defaultSortSetting: SortSetting = {
  columnTitle: "startTime",
  ascending: false,
};

export const sortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "sortSetting/JobDetails",
  s => s.localSettings,
  defaultSortSetting,
);

const mapStateToProps = (state: AdminUIState, props: RouteComponentProps) => {
  const sort = sortSetting.selector(state);

  const jobRequest = new protos.cockroach.server.serverpb.JobRequest({
    job_id: Long.fromString(getMatchParamByName(props.match, "id")),
  });
  const key = jobRequestKey(jobRequest);
  const jobData = state.cachedData.job[key];
  const job = jobData ? jobData.data : null;

  return {
    sort,
    job,
  };
};

const mapDispatchToProps = {
  setSort: sortSetting.set,
  refreshJob,
};

export default connect(mapStateToProps, mapDispatchToProps)(JobDetails);
