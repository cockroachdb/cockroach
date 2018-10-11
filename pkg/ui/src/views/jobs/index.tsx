// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import _ from "lodash";
import moment from "moment";
import { Line } from "rc-progress";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";

import * as protos from "src/js/protos";
import { jobsKey, refreshJobs } from "src/redux/apiReducers";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { TimestampToMoment } from "src/util/convert";
import * as docsURL from "src/util/docs";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import Loading from "src/views/shared/components/loading";
import { PageConfig, PageConfigItem } from "src/views/shared/components/pageconfig";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { ColumnDescriptor, SortedTable } from "src/views/shared/components/sortedtable";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";
import { trustIcon } from "src/util/trust";
import "./index.styl";

import succeededIcon from "!!raw-loader!assets/jobStatusIcons/checkMark.svg";
import failedIcon from "!!raw-loader!assets/jobStatusIcons/exclamationPoint.svg";

type Job = protos.cockroach.server.serverpb.JobsResponse.Job;

type JobType = protos.cockroach.sql.jobs.jobspb.Type;
const jobType = protos.cockroach.sql.jobs.jobspb.Type;

const JobsRequest = protos.cockroach.server.serverpb.JobsRequest;

const statusOptions = [
  { value: "", label: "All" },
  { value: "pending", label: "Pending" },
  { value: "running", label: "Running" },
  { value: "paused", label: "Paused" },
  { value: "canceled", label: "Canceled" },
  { value: "succeeded", label: "Succeeded" },
  { value: "failed", label: "Failed" },
];

const statusSetting = new LocalSetting<AdminUIState, string>(
  "jobs/status_setting", s => s.localSettings, statusOptions[0].value,
);

const typeOptions = [
  { value: jobType.UNSPECIFIED.toString(), label: "All" },
  { value: jobType.BACKUP.toString(), label: "Backups" },
  { value: jobType.RESTORE.toString(), label: "Restores" },
  { value: jobType.IMPORT.toString(), label: "Imports" },
  { value: jobType.SCHEMA_CHANGE.toString(), label: "Schema Changes" },
  { value: jobType.CHANGEFEED.toString(), label: "Changefeed"},
];

const typeSetting = new LocalSetting<AdminUIState, number>(
  "jobs/type_setting", s => s.localSettings, jobType.UNSPECIFIED,
);

const showOptions = [
  { value: "50", label: "Latest 50" },
  { value: "0", label: "All" },
];

const showSetting = new LocalSetting<AdminUIState, string>(
  "jobs/show_setting", s => s.localSettings, showOptions[0].value,
);

// Moment cannot render durations (moment/moment#1048). Hack it ourselves.
const formatDuration = (d: moment.Duration) =>
  [Math.floor(d.asHours()).toFixed(0), d.minutes(), d.seconds()]
    .map(c => ("0" + c).slice(-2))
    .join(":");

const JOB_STATUS_SUCCEEDED = "succeeded";
const JOB_STATUS_FAILED = "failed";
const JOB_STATUS_CANCELED = "canceled";
const JOB_STATUS_PENDING = "pending";
const JOB_STATUS_PAUSED = "paused";
const JOB_STATUS_RUNNING = "running";

const STATUS_ICONS: { [state: string]: string } = {
  [JOB_STATUS_SUCCEEDED]: succeededIcon,
  [JOB_STATUS_FAILED]: failedIcon,
};

class JobStatusCell extends React.Component<{ job: Job }, {}> {
  is(...statuses: string[]) {
    return statuses.indexOf(this.props.job.status) !== -1;
  }

  renderProgress() {
    if (this.is(JOB_STATUS_SUCCEEDED, JOB_STATUS_FAILED, JOB_STATUS_CANCELED)) {
      return (
        <div className="jobs-table__status">
          {this.props.job.status in STATUS_ICONS
            ? <div
                className="jobs-table__status-icon"
                dangerouslySetInnerHTML={trustIcon(STATUS_ICONS[this.props.job.status])}
              />
            : null}
          {this.props.job.status}
        </div>
      );
    }
    const percent = this.props.job.fraction_completed * 100;
    return (
      <div>
        {this.props.job.running_status
          ? <div className="jobs-table__running-status">{this.props.job.running_status}</div>
          : null}
        <Line
          percent={percent}
          strokeWidth={10}
          trailWidth={10}
          className="jobs-table__progress-bar"
          strokeColor={"#3A7DE1"}
        />
        <span title={percent.toFixed(3) + "%"}>{percent.toFixed(1) + "%"}</span>
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
        return formatDuration(moment.duration(remaining)) + " remaining";
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
        High-water Timestamp: {highwaterMoment.fromNow()}
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

class JobsSortedTable extends SortedTable<Job> {}

const jobsTableColumns: ColumnDescriptor<Job>[] = [
  {
    title: "ID",
    cell: job => String(job.id),
    sort: job => job.id,
  },
  {
    title: "Description",
    cell: job => <div className="jobs-table__cell--description">{job.description}</div>,
    sort: job => job.description,
  },
  {
    title: "User",
    cell: job => job.username,
    sort: job => job.username,
  },
  {
    title: "Creation Time",
    cell: job => TimestampToMoment(job.created).fromNow(),
    sort: job => TimestampToMoment(job.created).valueOf(),
  },
  {
    title: "Status",
    cell: job => <JobStatusCell job={job} />,
    sort: job => job.fraction_completed,
  },
];

const sortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "jobs/sort_setting",
  s => s.localSettings,
  { sortKey: 3 /* creation time */, ascending: false },
);

interface JobsTableProps {
  sort: SortSetting;
  status: string;
  show: string;
  type: number;
  setSort: (value: SortSetting) => void;
  setStatus: (value: string) => void;
  setShow: (value: string) => void;
  setType: (value: JobType) => void;
  refreshJobs: typeof refreshJobs;
  jobs: Job[];
  jobsValid: boolean;
}

const titleTooltip = (
  <span>
    Some jobs can be paused or canceled through SQL. For details, view the docs
    on the <a href={docsURL.pauseJob} target="_blank"><code>PAUSE JOB</code></a>
    and <a href={docsURL.cancelJob} target="_blank"><code>CANCEL JOB</code></a>
    statements.
  </span>
);

class JobsTable extends React.Component<JobsTableProps, {}> {
  refresh(props = this.props) {
    props.refreshJobs(new JobsRequest({
      status: props.status,
      type: props.type,
      limit: parseInt(props.show, 10),
    }));
  }

  componentWillMount() {
    this.refresh();
  }

  componentWillReceiveProps(props: JobsTableProps) {
    this.refresh(props);
  }

  onStatusSelected = (selected: DropdownOption) => {
    this.props.setStatus(selected.value);
  }

  onTypeSelected = (selected: DropdownOption) => {
    this.props.setType(parseInt(selected.value, 10));
  }

  onShowSelected = (selected: DropdownOption) => {
    this.props.setShow(selected.value);
  }

  renderJobExpanded = (job: Job) => {
    return (
      <div>
        <h3>Command</h3>
        <pre className="job-detail">{job.description}</pre>

        {job.status === "failed"
          ? [
              <h3>Error</h3>,
              <pre className="job-detail">{job.error}</pre>,
            ]
          : null}
      </div>
    );
  }

  renderTable = () => {
    const jobs = this.props.jobs && this.props.jobs.length > 0 && this.props.jobs;
    if (_.isEmpty(jobs)) {
      return <div className="no-results"><h2>No Results</h2></div>;
    }
    return (
      <section className="section">
        <JobsSortedTable
          data={jobs}
          sortSetting={this.props.sort}
          onChangeSortSetting={this.props.setSort}
          className="jobs-table"
          rowClass={job => "jobs-table__row--" + job.status}
          columns={jobsTableColumns}
          expandableConfig={{
            expandedContent: this.renderJobExpanded,
            expansionKey: (job) => job.id.toString(),
          }}
        />
      </section>
    );
  }

  render() {
    return (
      <div className="jobs-page">
        <Helmet>
          <title>Jobs</title>
        </Helmet>
        <section className="section">
          <h1>
            Jobs
            <div className="section-heading__tooltip">
              <ToolTipWrapper text={titleTooltip}>
                <div className="section-heading__tooltip-hover-area">
                  <div className="section-heading__info-icon">i</div>
                </div>
              </ToolTipWrapper>
            </div>
          </h1>
        </section>
        <div>
          <PageConfig>
            <PageConfigItem>
              <Dropdown
                title="Status"
                options={statusOptions}
                selected={this.props.status}
                onChange={this.onStatusSelected}
              />
            </PageConfigItem>
            <PageConfigItem>
              <Dropdown
                title="Type"
                options={typeOptions}
                selected={this.props.type.toString()}
                onChange={this.onTypeSelected}
              />
            </PageConfigItem>
            <PageConfigItem>
              <Dropdown
                title="Show"
                options={showOptions}
                selected={this.props.show}
                onChange={this.onShowSelected}
              />
            </PageConfigItem>
          </PageConfig>
        </div>
        <Loading
          loading={_.isNil(this.props.jobs)}
          render={this.renderTable}
        />
      </div>
    );
  }
}

const mapStateToProps = (state: AdminUIState) => {
  const sort = sortSetting.selector(state);
  const status = statusSetting.selector(state);
  const show = showSetting.selector(state);
  const type = typeSetting.selector(state);
  const key = jobsKey(status, type, parseInt(show, 10));
  const jobs = state.cachedData.jobs[key];
  return {
    sort, status, show, type,
    jobs: jobs && jobs.data && jobs.data.jobs,
    jobsValid: jobs && jobs.valid,
  };
};

const actions = {
  setSort: sortSetting.set,
  setStatus: statusSetting.set,
  setShow: showSetting.set,
  setType: typeSetting.set,
  refreshJobs,
};

export default connect(mapStateToProps, actions)(JobsTable);
