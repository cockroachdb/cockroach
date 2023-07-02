// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import React, { useContext } from "react";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { ArrowLeft } from "@cockroachlabs/icons";
import { Col, Row, Tabs } from "antd";
import "antd/lib/col/style";
import "antd/lib/row/style";
import "antd/lib/tabs/style";
import Long from "long";
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
  getMatchParamByName,
  DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_TZ,
} from "src/util";

import { HighwaterTimestamp } from "src/jobs/util/highwaterTimestamp";
import { JobStatusCell } from "src/jobs/util/jobStatusCell";
import { isTerminalState } from "../util/jobOptions";

import { commonStyles } from "src/common";
import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import jobStyles from "src/jobs/jobs.module.scss";

import classNames from "classnames/bind";
import { Timestamp } from "../../timestamp";
import { RequestState } from "../../api";
import moment from "moment-timezone";
import { CockroachCloudContext } from "src/contexts";
import { InlineAlert } from "@cockroachlabs/ui-components";

const { TabPane } = Tabs;

const cardCx = classNames.bind(summaryCardStyles);
const jobCx = classNames.bind(jobStyles);

enum TabKeysEnum {
  OVERVIEW = "Overview",
  PROFILER = "Profiler",
}

export interface JobDetailsStateProps {
  jobRequest: RequestState<JobResponse>;
}

export interface JobDetailsDispatchProps {
  refreshJob: (req: JobRequest) => void;
}

export interface JobDetailsState {
  currentTab?: string;
}

export type JobDetailsProps = JobDetailsStateProps &
  JobDetailsDispatchProps &
  RouteComponentProps<unknown>;

export class JobDetails extends React.Component<
  JobDetailsProps,
  JobDetailsState
> {
  refreshDataInterval: NodeJS.Timeout;

  constructor(props: JobDetailsProps) {
    super(props);
    const searchParams = new URLSearchParams(props.history.location.search);
    this.state = {
      currentTab: searchParams.get("tab") || "overview",
    };
  }

  private refresh(): void {
    if (isTerminalState(this.props.jobRequest.data?.status)) {
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
    if (!this.props.jobRequest.data) {
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

  renderProfilerTabContent = (
    job: cockroach.server.serverpb.JobResponse,
  ): React.ReactElement => {
    const id = job?.id;
    // This URL results in a cluster-wide CPU profile to be collected for 5
    // seconds. We set `tagfocus` (tf) to only view the samples corresponding to
    // this job's execution.
    const url = `debug/pprof/ui/cpu?node=all&seconds=5&labels=true&tf=job.*${id}`;
    return (
      <Row gutter={24}>
        <Col className="gutter-row" span={24}>
          <SummaryCard className={cardCx("summary-card")}>
            <SummaryCardItem
              label="Cluster-wide CPU Profile"
              value={<a href={url}>Profile</a>}
            />
            <InlineAlert
              intent="warning"
              title="This operation buffers profiles in memory for all the nodes in the cluster and can result in increased memory usage."
            />
          </SummaryCard>
        </Col>
      </Row>
    );
  };

  renderOverviewTabContent = (
    hasNextRun: boolean,
    nextRun: moment.Moment,
    job: JobResponse,
  ): React.ReactElement => {
    if (!job) {
      return null;
    }

    return (
      <Row gutter={24}>
        <Col className="gutter-row" span={24}>
          <SummaryCard className={cardCx("summary-card")}>
            <SummaryCardItem
              label="Status"
              value={
                <JobStatusCell job={job} lineWidth={1.5} hideDuration={true} />
              }
            />
            {hasNextRun && (
              <>
                <SummaryCardItem
                  label="Next Planned Execution Time"
                  value={
                    <Timestamp
                      time={nextRun}
                      format={DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_TZ}
                    />
                  }
                />
              </>
            )}
            <SummaryCardItem
              label="Creation Time"
              value={
                <Timestamp
                  time={TimestampToMoment(job.created)}
                  format={DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_TZ}
                />
              }
            />
            {job.modified && (
              <SummaryCardItem
                label="Last Modified Time"
                value={
                  <Timestamp
                    time={TimestampToMoment(job.modified)}
                    format={DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_TZ}
                  />
                }
              />
            )}
            {job.finished && (
              <SummaryCardItem
                label="Completed Time"
                value={
                  <Timestamp
                    time={TimestampToMoment(job.finished)}
                    format={DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_TZ}
                  />
                }
              />
            )}
            <SummaryCardItem
              label="Last Execution Time"
              value={
                <Timestamp
                  time={TimestampToMoment(job.last_run)}
                  format={DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_TZ}
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
    );
  };

  onTabChange = (tabId: string): void => {
    const { history } = this.props;
    const searchParams = new URLSearchParams(history.location.search);
    searchParams.set("tab", tabId);
    history.replace({
      ...history.location,
      search: searchParams.toString(),
    });
    this.setState({
      currentTab: tabId,
    });
  };

  render(): React.ReactElement {
    const isLoading =
      this.props.jobRequest.inFlight && !this.props.jobRequest.data;
    const error = this.props.jobRequest.error;
    const job = this.props.jobRequest.data;
    const nextRun = TimestampToMoment(job?.next_run);
    const hasNextRun = nextRun?.isAfter();
    const { currentTab } = this.state;
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
            render={() => (
              <>
                <section className={cardCx("summary-card")}>
                  <Row gutter={24}>
                    <Col className="gutter-row" span={24}>
                      <SqlBox
                        value={job?.description ?? "Job not found."}
                        size={SqlBoxSize.custom}
                        format={true}
                      />
                    </Col>
                  </Row>
                </section>
                <Tabs
                  className={commonStyles("cockroach--tabs")}
                  defaultActiveKey={TabKeysEnum.OVERVIEW}
                  onChange={this.onTabChange}
                  activeKey={currentTab}
                >
                  <TabPane tab={TabKeysEnum.OVERVIEW} key="overview">
                    {this.renderOverviewTabContent(hasNextRun, nextRun, job)}
                  </TabPane>
                  {!useContext(CockroachCloudContext) && (
                    <TabPane tab={TabKeysEnum.PROFILER} key="profiler">
                      {this.renderProfilerTabContent(job)}
                    </TabPane>
                  )}
                </Tabs>
              </>
            )}
          />
        </section>
      </div>
    );
  }
}
