// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { ArrowLeft } from "@cockroachlabs/icons";
import { Col, Row, Tabs } from "antd";
import classNames from "classnames/bind";
import Long from "long";
import moment from "moment-timezone";
import React, { useContext } from "react";
import Helmet from "react-helmet";
import { RouteComponentProps } from "react-router-dom";

import { JobRequest, JobResponse } from "src/api/jobsApi";
import { Button } from "src/button";
import { commonStyles } from "src/common";
import { CockroachCloudContext } from "src/contexts";
import { EmptyTable } from "src/empty";
import jobStyles from "src/jobs/jobs.module.scss";
import { HighwaterTimestamp } from "src/jobs/util/highwaterTimestamp";
import { JobStatusCell } from "src/jobs/util/jobStatusCell";
import { Loading } from "src/loading";
import { SortedTable } from "src/sortedtable";
import { SqlBox, SqlBoxSize } from "src/sql";
import { UIConfigState } from "src/store";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import { Text, TextTypes } from "src/text";
import {
  DATE_WITH_SECONDS_FORMAT_24_TZ,
  DATE_WITH_SECONDS_FORMAT,
  TimestampToMoment,
  getMatchParamByName,
  idAttr,
} from "src/util";

import {
  GetJobProfilerExecutionDetailRequest,
  GetJobProfilerExecutionDetailResponse,
  ListJobProfilerExecutionDetailsRequest,
  ListJobProfilerExecutionDetailsResponse,
  RequestState,
} from "../../api";
import { Timestamp } from "../../timestamp";
import { isTerminalState } from "../util/jobOptions";

import { JobProfilerView } from "./jobProfilerView";

type JobMessage = JobResponse["messages"][number];

const { TabPane } = Tabs;

const cardCx = classNames.bind(summaryCardStyles);
const jobCx = classNames.bind(jobStyles);

enum TabKeysEnum {
  OVERVIEW = "Overview",
  PROFILER = "Advanced Debugging",
}

export interface JobDetailsStateProps {
  jobRequest: RequestState<JobResponse>;
  jobProfilerExecutionDetailFilesResponse: RequestState<ListJobProfilerExecutionDetailsResponse>;
  jobProfilerLastUpdated: moment.Moment;
  jobProfilerDataIsValid: boolean;
  onDownloadExecutionFileClicked: (
    req: GetJobProfilerExecutionDetailRequest,
  ) => Promise<GetJobProfilerExecutionDetailResponse>;
  hasAdminRole?: UIConfigState["hasAdminRole"];
}

export interface JobDetailsDispatchProps {
  refreshJob: (req: JobRequest) => void;
  refreshExecutionDetailFiles: (
    req: ListJobProfilerExecutionDetailsRequest,
  ) => void;
  onRequestExecutionDetails: (jobID: Long) => void;
  refreshUserSQLRoles: () => void;
}

export interface JobDetailsState {
  currentTab?: string;
}

export type JobDetailsProps = JobDetailsStateProps &
  JobDetailsDispatchProps &
  RouteComponentProps;

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
    this.props.refreshUserSQLRoles();
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
    return (
      <JobProfilerView
        jobID={id}
        executionDetailFilesResponse={
          this.props.jobProfilerExecutionDetailFilesResponse
        }
        refreshExecutionDetailFiles={this.props.refreshExecutionDetailFiles}
        lastUpdated={this.props.jobProfilerLastUpdated}
        isDataValid={this.props.jobProfilerDataIsValid}
        onDownloadExecutionFileClicked={
          this.props.onDownloadExecutionFileClicked
        }
        onRequestExecutionDetails={this.props.onRequestExecutionDetails}
      />
    );
  };

  renderOverviewTabContent = (job: JobResponse): React.ReactElement => {
    if (!job) {
      return null;
    }

    const messageColumns = [
      {
        name: "timestamp",
        title: "When",
        hideTitleUnderline: true,
        cell: (x: JobMessage) => (
          <Timestamp
            time={TimestampToMoment(x.timestamp, null)}
            format={DATE_WITH_SECONDS_FORMAT}
          />
        ),
      },
      {
        name: "kind",
        title: "Kind",
        hideTitleUnderline: true,
        cell: (x: JobMessage) => x.kind,
      },
      {
        name: "message",
        title: "Message",
        hideTitleUnderline: true,
        cell: (x: JobMessage) => (
          <p className={jobCx("message")}>{x.message}</p>
        ),
      },
    ];

    return (
      <Row gutter={24}>
        <Col className="gutter-row" span={8}>
          <Text
            textType={TextTypes.Heading5}
            className={jobCx("details-header")}
          >
            Details
          </Text>
          <SummaryCard className={cardCx("summary-card")}>
            <SummaryCardItem
              label="Status"
              value={
                <JobStatusCell job={job} lineWidth={1.5} hideDuration={true} />
              }
            />
            <SummaryCardItem
              label="Creation Time"
              value={
                <Timestamp
                  time={TimestampToMoment(job.created, null)}
                  format={DATE_WITH_SECONDS_FORMAT_24_TZ}
                />
              }
            />
            {job.modified && (
              <SummaryCardItem
                label="Last Modified Time"
                value={
                  <Timestamp
                    time={TimestampToMoment(job.modified, null)}
                    format={DATE_WITH_SECONDS_FORMAT_24_TZ}
                  />
                }
              />
            )}
            {job.finished && (
              <SummaryCardItem
                label="Completed Time"
                value={
                  <Timestamp
                    time={TimestampToMoment(job.finished, null)}
                    format={DATE_WITH_SECONDS_FORMAT_24_TZ}
                  />
                }
              />
            )}
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
            <SummaryCardItem
              label="Coordinator Node"
              value={
                job.coordinator_id.isZero()
                  ? "-"
                  : job.coordinator_id.toString()
              }
            />
          </SummaryCard>
        </Col>
        <Col className="gutter-row" span={16}>
          <Text
            textType={TextTypes.Heading5}
            className={jobCx("details-header")}
          >
            Events
          </Text>
          <SummaryCard className={jobCx("messages-card")}>
            <SortedTable
              data={job.messages}
              columns={messageColumns}
              tableWrapperClassName={jobCx("job-messages", "sorted-table")}
              renderNoResult={<EmptyTable title="No messages recorded." />}
            />
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
                        size={SqlBoxSize.CUSTOM}
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
                    {this.renderOverviewTabContent(job)}
                  </TabPane>
                  {!useContext(CockroachCloudContext) &&
                    this.props.hasAdminRole && (
                      <TabPane
                        tab={TabKeysEnum.PROFILER}
                        key="advancedDebugging"
                      >
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
