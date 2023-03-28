// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { ArrowLeft } from "@cockroachlabs/icons";
import { Col, Row } from "antd";
import "antd/lib/col/style";
import "antd/lib/row/style";
import Long from "long";
import React from "react";
import Helmet from "react-helmet";
import { RouteComponentProps } from "react-router-dom";
import { JobRequest, JobResponse } from "src/api/jobsApi";
import { Button } from "src/button";
import { Loading } from "src/loading";
import { SqlBox, SqlBoxSize } from "src/sql";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import {
  TimestampToMoment,
  idAttr,
  DATE_FORMAT_24_TZ,
  getMatchParamByName,
} from "src/util";

import { HighwaterTimestamp } from "src/jobs/util/highwaterTimestamp";
import { JobStatusCell } from "src/jobs/util/jobStatusCell";
import { isTerminalState } from "../util/jobOptions";

import { commonStyles } from "src/common";
import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import jobStyles from "src/jobs/jobs.module.scss";

import classNames from "classnames/bind";
import { Timestamp } from "../../timestamp";

const cardCx = classNames.bind(summaryCardStyles);
const jobCx = classNames.bind(jobStyles);

export interface JobDetailsStateProps {
  job: JobResponse;
  jobError: Error | null;
  jobLoading: boolean;
}

export interface JobDetailsDispatchProps {
  refreshJob: (req: JobRequest) => void;
}

export type JobDetailsProps = JobDetailsStateProps &
  JobDetailsDispatchProps &
  RouteComponentProps<unknown>;

export class JobDetails extends React.Component<JobDetailsProps> {
  refreshDataInterval: NodeJS.Timeout;

  constructor(props: JobDetailsProps) {
    super(props);
  }

  private refresh(): void {
    if (isTerminalState(this.props.job?.status)) {
      clearInterval(this.refreshDataInterval);
      return;
    }

    this.props.refreshJob(
      new cockroach.server.serverpb.JobRequest({
        job_id: Long.fromString(getMatchParamByName(this.props.match, idAttr)),
      }),
    );
  }

  componentDidMount(): void {
    if (!this.props.job) {
      this.refresh();
    }
    // Refresh every 10s.
    this.refreshDataInterval = setInterval(() => this.refresh(), 10 * 1000);
  }

  componentWillUnmount(): void {
    if (this.refreshDataInterval) {
      clearInterval(this.refreshDataInterval);
    }
  }

  prevPage = (): void => this.props.history.goBack();

  renderContent = (): React.ReactElement => {
    const job = this.props.job;
    const nextRun = TimestampToMoment(job.next_run);
    const hasNextRun = nextRun.isAfter();
    return (
      <>
        <Row gutter={24}>
          <Col className="gutter-row" span={24}>
            <SqlBox
              value={job.description}
              size={SqlBoxSize.custom}
              format={true}
            />
          </Col>
        </Row>
        <Row gutter={24}>
          <Col className="gutter-row" span={12}>
            <SummaryCard>
              <h3 className={jobCx("summary--card--title")}>Status</h3>
              <JobStatusCell job={job} lineWidth={1.5} hideDuration={true} />
              {hasNextRun && (
                <>
                  <h3 className={jobCx("summary--card--title", "secondary")}>
                    Next Planned Execution Time:
                  </h3>
                  <Timestamp time={nextRun} format={DATE_FORMAT_24_TZ} />
                </>
              )}
            </SummaryCard>
          </Col>
          <Col className="gutter-row" span={12}>
            <SummaryCard className={cardCx("summary-card")}>
              <SummaryCardItem
                label="Creation Time"
                value={
                  <Timestamp
                    time={TimestampToMoment(job.created)}
                    format={DATE_FORMAT_24_TZ}
                  />
                }
              />
              <SummaryCardItem
                label="Last Execution Time"
                value={
                  <Timestamp
                    time={TimestampToMoment(job.last_run)}
                    format={DATE_FORMAT_24_TZ}
                  />
                }
              />
              <SummaryCardItem
                label="Execution Count"
                value={String(job.num_runs)}
              />
              <SummaryCardItem label="User Name" value={job.username} />
              {job.highwater_timestamp && (
                <SummaryCardItem
                  label="High-water Timestamp"
                  value={
                    <HighwaterTimestamp
                      timestamp={job.highwater_timestamp}
                      decimalString={job.highwater_decimal}
                    />
                  }
                />
              )}
            </SummaryCard>
          </Col>
        </Row>
      </>
    );
  };

  render(): React.ReactElement {
    const isLoading = !this.props.job || this.props.jobLoading;
    const error = this.props.jobError;
    return (
      <div className={jobCx("job-details")}>
        <Helmet title={"Details | Job"} />
        <div className={jobCx("section page--header")}>
          <Button
            onClick={this.prevPage}
            type="unstyled-link"
            size="small"
            icon={<ArrowLeft fontSize={"10px"} />}
            iconPosition="left"
            className={commonStyles("small-margin")}
          >
            Jobs
          </Button>
          <h3 className={jobCx("page--header__title")}>{`Job ID: ${String(
            getMatchParamByName(this.props.match, idAttr),
          )}`}</h3>
        </div>
        <section className={jobCx("section section--container")}>
          <Loading
            loading={isLoading}
            page={"job details"}
            error={error}
            render={this.renderContent}
          />
        </section>
      </div>
    );
  }
}
