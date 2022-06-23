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
import { SqlBox } from "src/sql";
import { SummaryCard } from "src/summaryCard";
import { TimestampToMoment } from "src/util";
import { DATE_FORMAT_24_UTC } from "src/util/format";
import { getMatchParamByName } from "src/util/query";

import { HighwaterTimestamp } from "src/jobs/util/highwaterTimestamp";
import { JobStatusCell } from "src/jobs/util/jobStatusCell";

import { commonStyles } from "src/common";
import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import jobStyles from "src/jobs/jobs.module.scss";

import classNames from "classnames/bind";

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
  constructor(props: JobDetailsProps) {
    super(props);
  }

  private refresh(): void {
    this.props.refreshJob(
      new cockroach.server.serverpb.JobRequest({
        job_id: Long.fromString(getMatchParamByName(this.props.match, "id")),
      }),
    );
  }

  componentDidMount(): void {
    this.refresh();
  }

  componentDidUpdate(): void {
    this.refresh();
  }

  prevPage = () => this.props.history.goBack();

  renderContent = () => {
    const job = this.props.job;
    return (
      <Row gutter={16}>
        <Col className={commonStyles("gutter-row")} span={16}>
          <SqlBox value={job.description} />
          <SummaryCard>
            <h3 className={cardCx("summary--card__status--title")}>Status</h3>
            <JobStatusCell job={job} lineWidth={1.5} />
          </SummaryCard>
        </Col>
        <Col className={commonStyles("gutter-row")} span={8}>
          <SummaryCard>
            <Row>
              <Col span={24}>
                <div className={cardCx("summary--card__counting")}>
                  <h3 className={cardCx("summary--card__counting--value")}>
                    {TimestampToMoment(job.created).format(DATE_FORMAT_24_UTC)}
                  </h3>
                  <p className={cardCx("summary--card__counting--label")}>
                    Creation time
                  </p>
                </div>
              </Col>
              <Col span={24}>
                <div className={cardCx("summary--card__counting")}>
                  <h3 className={cardCx("summary--card__counting--value")}>
                    {job.username}
                  </h3>
                  <p className={cardCx("summary--card__counting--label")}>
                    Users
                  </p>
                </div>
              </Col>
              {job.highwater_timestamp ? (
                <Col span={24}>
                  <div className={cardCx("summary--card__counting")}>
                    <h3 className={cardCx("summary--card__counting--value")}>
                      <HighwaterTimestamp
                        timestamp={job.highwater_timestamp}
                        decimalString={job.highwater_decimal}
                      />
                    </h3>
                    <p className={cardCx("summary--card__counting--label")}>
                      High-water Timestamp
                    </p>
                  </div>
                </Col>
              ) : null}
            </Row>
          </SummaryCard>
        </Col>
      </Row>
    );
  };

  render() {
    const isLoading = !this.props.job || this.props.jobLoading;
    const error = this.props.job && this.props.jobError;
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
            getMatchParamByName(this.props.match, "id"),
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
