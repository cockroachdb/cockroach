// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Icon, Pagination, Tooltip } from "antd";
import classNames from "classnames";
import _ from "lodash";
import moment from "moment";
import { DATE_FORMAT } from "src/util/format";
import { Line } from "rc-progress";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { withRouter, Link } from "react-router-dom";

import { cockroach } from "src/js/protos";
import { jobsKey, refreshJobs } from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { TimestampToMoment } from "src/util/convert";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import Loading from "src/views/shared/components/loading";
import { PageConfig, PageConfigItem } from "src/views/shared/components/pageconfig";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { ColumnDescriptor, SortedTable } from "src/views/shared/components/sortedtable";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";
import Empty from "../app/components/empty";
import "./index.styl";

import Job = cockroach.server.serverpb.JobsResponse.IJob;
import JobType = cockroach.sql.jobs.jobspb.Type;
import JobsRequest = cockroach.server.serverpb.JobsRequest;
import JobsResponse = cockroach.server.serverpb.JobsResponse;

const statusOptions = [
  { value: "", label: "All" },
  { value: "pending", label: "Pending" },
  { value: "running", label: "Running" },
  { value: "paused", label: "Paused" },
  { value: "canceled", label: "Canceled" },
  { value: "succeeded", label: "Succeeded" },
  { value: "failed", label: "Failed" },
];

export const statusSetting = new LocalSetting<AdminUIState, string>(
  "jobs/status_setting", s => s.localSettings, statusOptions[0].value,
);

const typeOptions = [
  { value: JobType.UNSPECIFIED.toString(), label: "All" },
  { value: JobType.BACKUP.toString(), label: "Backups" },
  { value: JobType.RESTORE.toString(), label: "Restores" },
  { value: JobType.IMPORT.toString(), label: "Imports" },
  { value: JobType.SCHEMA_CHANGE.toString(), label: "Schema Changes" },
  { value: JobType.CHANGEFEED.toString(), label: "Changefeed"},
  { value: JobType.CREATE_STATS.toString(), label: "Statistics Creation"},
  { value: JobType.AUTO_CREATE_STATS.toString(), label: "Auto-Statistics Creation"},
];

export const typeSetting = new LocalSetting<AdminUIState, number>(
  "jobs/type_setting", s => s.localSettings, JobType.UNSPECIFIED,
);

const showOptions = [
  { value: "50", label: "Latest 50" },
  { value: "0", label: "All" },
];

export const showSetting = new LocalSetting<AdminUIState, string>(
  "jobs/show_setting", s => s.localSettings, showOptions[0].value,
);

// Moment cannot render durations (moment/moment#1048). Hack it ourselves.
export const formatDuration = (d: moment.Duration) =>
  [Math.floor(d.asHours()).toFixed(0), d.minutes(), d.seconds()]
    .map(c => ("0" + c).slice(-2))
    .join(":");

export const JOB_STATUS_SUCCEEDED = "succeeded";
export const JOB_STATUS_FAILED = "failed";
export const JOB_STATUS_CANCELED = "canceled";
export const JOB_STATUS_PENDING = "pending";
export const JOB_STATUS_PAUSED = "paused";
export const JOB_STATUS_RUNNING = "running";

export const renamedStatuses = (status: string) => {
  switch (status) {
    case JOB_STATUS_SUCCEEDED:
      return "Completed";
    case JOB_STATUS_FAILED:
      return "Import failed";
    case JOB_STATUS_CANCELED:
      return "Import failed";
    default:
      return status;
  }
};

class JobStatusCell extends React.Component<{ job: Job }, {}> {
  is(...statuses: string[]) {
    return statuses.indexOf(this.props.job.status) !== -1;
  }
  renderProgress() {
    if (this.is(JOB_STATUS_SUCCEEDED, JOB_STATUS_FAILED, JOB_STATUS_CANCELED)) {
      const className = classNames("jobs-table__status", {
        "jobs-table__status--succeed": this.is(JOB_STATUS_SUCCEEDED),
        "jobs-table__status--failed": this.is(JOB_STATUS_FAILED, JOB_STATUS_CANCELED),
      });
      return (
        <div className={className}>
          {renamedStatuses(this.props.job.status)}
        </div>
      );
    }
    const percent = this.props.job.fraction_completed * 100;
    return (
      <div className="jobs-table__progress">
        {this.props.job.running_status
          ? <div className="jobs-table__running-status">{this.props.job.running_status}</div>
          : null}

        <div className="jobs-table__status--percentage" title={percent.toFixed(3) + "%"}>{percent.toFixed(1) + "%"}</div>
        <Line
          percent={percent}
          strokeWidth={11}
          trailWidth={11}
          strokeColor="#0788ff"
          trailColor="#d6dbe7"
          className="jobs-table__progress-bar"
        />
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
        return <span className="jobs-table__duration--right">{formatDuration(moment.duration(remaining)) + " remaining"}</span>;
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
        High-water Timestamp: {highwaterMoment.format(DATE_FORMAT)}
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
    title: "Description",
    className: "cl-table__col-query-text",
    cell: job => {
      // If a [SQL] job.statement exists, it means that job.description
      // is a human-readable message. Otherwise job.description is a SQL
      // statement.
      const additionalStyle = (job.statement ? "" : " jobs-table__cell--sql");
      return (
        <Link className={`jobs-table__cell--description${additionalStyle}`} to={`jobs/${String(job.id)}`}>
          <div className="cl-table-link__tooltip">
            <Tooltip overlayClassName="preset-black" placement="bottom" title={
              <pre style={{ whiteSpace: "pre-wrap" }}>{ job.description }</pre>
            }>
              <div className={`jobs-table__cell--description${additionalStyle}`} >
                {job.statement || job.description}
              </div>
            </Tooltip>
          </div>
        </Link>
      );
    },
    sort: job => job.description,
  },
  {
    title: "Job ID",
    titleAlign: "right",
    cell: job => String(job.id),
    sort: job => job.id,
  },
  {
    title: "Users",
    cell: job => job.username,
    sort: job => job.username,
  },
  {
    title: "Creation Time",
    cell: job => TimestampToMoment(job.created).format(DATE_FORMAT),
    sort: job => TimestampToMoment(job.created).valueOf(),
  },
  {
    title: "Status",
    cell: job => <JobStatusCell job={job} />,
    sort: job => job.fraction_completed,
  },
];

export const sortSetting = new LocalSetting<AdminUIState, SortSetting>(
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
  jobs: CachedDataReducerState<JobsResponse>;
}

export class JobsTable extends React.Component<JobsTableProps> {
  state = {
    pagination: {
      pageSize: 20,
      current: 1,
    },
  };

  refresh(props = this.props) {
    props.refreshJobs(new JobsRequest({
      status: props.status,
      type: props.type,
      limit: parseInt(props.show, 10),
    }));
  }

  onChangePage = (current: number) => {
    const { pagination } = this.state;
    this.setState({ pagination: { ...pagination, current }});
  }

  renderPage = (_page: number, type: "page" | "prev" | "next" | "jump-prev" | "jump-next", originalElement: React.ReactNode) => {
    switch (type) {
      case "jump-prev":
        return (
          <div className="_pg-jump">
            <Icon type="left" />
            <span className="_jump-dots">•••</span>
          </div>
        );
      case "jump-next":
        return (
          <div className="_pg-jump">
            <Icon type="right" />
            <span className="_jump-dots">•••</span>
          </div>
        );
      default:
        return originalElement;
    }
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

  getData = () => {
    const { pagination: { current, pageSize } } = this.state;
    const jobs = this.props.jobs.data.jobs;
    const currentDefault = current - 1;
    const start = (currentDefault * pageSize);
    const end = (currentDefault * pageSize + pageSize);
    const data = jobs.slice(start, end);
    return data;
  }

  renderCounts = () => {
    const { pagination: { current, pageSize } } = this.state;
    const total = this.props.jobs.data.jobs.length;
    const pageCount = current * pageSize > total ? total : current * pageSize;
    const count = total > 10 ? pageCount : current * total;
    return `${count} of ${total} jobs`;
  }

  renderTable = () => {
    const jobs = this.props.jobs.data.jobs;
    const { pagination } = this.state;
    if (_.isEmpty(jobs)) {
      return (
        <Empty
          title="Jobs will show up here"
          description="Jobs can include backup, import, restore or cdc running."
          buttonHref="https://www.cockroachlabs.com/docs/stable/admin-ui-jobs-page.html"
        />
      );
    }
    return (
      <React.Fragment>
        <div className="cl-table-statistic">
          <h4 className="cl-count-title">
            {this.renderCounts()}
          </h4>
        </div>
        <section className="cl-table-wrapper">
          <JobsSortedTable
            data={this.getData()}
            sortSetting={this.props.sort}
            onChangeSortSetting={this.props.setSort}
            className="jobs-table"
            rowClass={job => "jobs-table__row--" + job.status}
            columns={jobsTableColumns}
          />
        </section>
        <Pagination
          size="small"
          itemRender={this.renderPage as (page: number, type: "page" | "prev" | "next" | "jump-prev" | "jump-next") => React.ReactNode}
          pageSize={pagination.pageSize}
          current={pagination.current}
          total={jobs.length}
          onChange={this.onChangePage}
          hideOnSinglePage
        />
      </React.Fragment>
    );
  }

  render() {
    return (
      <div className="jobs-page">
        <Helmet title="Jobs" />
        <section className="section">
          <h1 className="base-heading">
            Jobs
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
          </PageConfig>
        </div>
        <section className="section">
          <Loading
            loading={!this.props.jobs || !this.props.jobs.data}
            error={this.props.jobs && this.props.jobs.lastError}
            render={this.renderTable}
          />
        </section>
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
    sort, status, show, type, jobs,
  };
};

const mapDispatchToProps = {
  setSort: sortSetting.set,
  setStatus: statusSetting.set,
  setShow: showSetting.set,
  setType: typeSetting.set,
  refreshJobs,
};

export default withRouter(connect(mapStateToProps, mapDispatchToProps)(JobsTable));
