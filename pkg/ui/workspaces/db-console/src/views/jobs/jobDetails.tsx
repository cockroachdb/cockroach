// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Col, Row } from "antd";
import _ from "lodash";
import { TimestampToMoment } from "src/util/convert";
import Long from "long";
import React from "react";
import Helmet from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps } from "react-router-dom";
import { cockroach } from "src/js/protos";
import { jobRequestKey, refreshJob } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { getMatchParamByName } from "src/util/query";
import { Loading } from "@cockroachlabs/cluster-ui";
import SqlBox from "../shared/components/sql/box";
import { SummaryCard } from "../shared/components/summaryCard";

import Job = cockroach.server.serverpb.JobResponse;
import JobRequest = cockroach.server.serverpb.JobRequest;
import { Button } from "@cockroachlabs/cluster-ui";
import { ArrowLeft } from "@cockroachlabs/icons";
import { DATE_FORMAT } from "src/util/format";
import { JobStatusCell } from "./jobStatusCell";
import "src/views/shared/components/summaryCard/styles.styl";
import * as protos from "src/js/protos";

interface JobsTableProps extends RouteComponentProps {
  refreshJob: typeof refreshJob;
  job: Job;
}

class JobDetails extends React.Component<JobsTableProps, {}> {
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

  renderContent = () => {
    const { job } = this.props;
    return (
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
                    {TimestampToMoment(job.created).format(DATE_FORMAT)}
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
          >
            Jobs
          </Button>
          <h1 className="page--header__title">{`Job ID: ${String(
            getMatchParamByName(match, "id"),
          )}`}</h1>
        </div>
        <section className="section section--container">
          <Loading loading={_.isNil(job)} render={this.renderContent} />
        </section>
      </div>
    );
  }
}

const mapStateToProps = (state: AdminUIState, props: RouteComponentProps) => {
  const jobRequest = new protos.cockroach.server.serverpb.JobRequest({
    job_id: Long.fromString(getMatchParamByName(props.match, "id")),
  });
  const key = jobRequestKey(jobRequest);
  const jobData = state.cachedData.job[key];
  const job = jobData ? jobData.data : null;

  return {
    job,
  };
};

const mapDispatchToProps = {
  refreshJob,
};

export default connect(mapStateToProps, mapDispatchToProps)(JobDetails);
