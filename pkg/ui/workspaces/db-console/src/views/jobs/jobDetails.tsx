// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import { JobDetails, JobDetailsStateProps } from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { createSelector } from "reselect";
import { CachedDataReducerState, refreshJob } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { JobResponseMessage } from "src/util/api";
import { getMatchParamByName } from "src/util/query";
import { Loading, util } from "@cockroachlabs/cluster-ui";
import SqlBox from "../shared/components/sql/box";
import { SummaryCard } from "../shared/components/summaryCard";

import Job = cockroach.server.serverpb.JobResponse;
import JobRequest = cockroach.server.serverpb.JobRequest;
import { Button } from "@cockroachlabs/cluster-ui";
import { ArrowLeft } from "@cockroachlabs/icons";
import { DATE_FORMAT } from "src/util/format";
import { JobStatusCell } from "./jobStatusCell";
import "../shared/components/summaryCard/styles.styl";
import * as protos from "src/js/protos";
import { HighwaterTimestamp } from "src/views/jobs/highwaterTimestamp";

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
              {job.highwater_timestamp ? (
                <Col span={24}>
                  <div className="summary--card__counting">
                    <h3 className="summary--card__counting--value">
                      <HighwaterTimestamp
                        timestamp={job.highwater_timestamp}
                        decimalString={job.highwater_decimal}
                      />
                    </h3>
                    <p className="summary--card__counting--label">
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

const mapStateToProps = (state: AdminUIState, props: RouteComponentProps) => {
  const jobRequest = new protos.cockroach.server.serverpb.JobRequest({
    job_id: Long.fromString(getMatchParamByName(props.match, "id")),
  });
  const key = jobRequestKey(jobRequest);
  const jobData = state.cachedData.job[key];
  const job = jobData ? jobData.data : null;

  const selectJobState = createSelector(
    [
      (state: AdminUIState) => state.cachedData.job,
      (_state: AdminUIState, props: RouteComponentProps) => props,
    ],
    (job, props): CachedDataReducerState<JobResponseMessage> => {
      const jobId = getMatchParamByName(props.match, "id");
      if (!job) {
        return null;
      }
      return job[jobId];
    },
  );

  const mapStateToProps = (
    state: AdminUIState,
    props: RouteComponentProps,
  ): JobDetailsStateProps => {
    const jobState = selectJobState(state, props);
    const job = jobState ? jobState.data : null;
    const jobLoading = jobState ? jobState.inFlight : false;
    const jobError = jobState ? jobState.lastError : null;
    return {
      job,
      jobLoading,
      jobError,
    };
  };

  const mapDispatchToProps = {
    refreshJob,
  };

  export default withRouter(
    connect(mapStateToProps, mapDispatchToProps)(JobDetails),
  );
