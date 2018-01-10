import _ from "lodash";
import moment from "moment";
import { Line } from "rc-progress";
import React from "react";
import { connect } from "react-redux";

import * as protos from "src/js/protos";
import { jobsKey, refreshJobs } from "src/redux/apiReducers";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { TimestampToMoment } from "src/util/convert";
import docsURL from "src/util/docs";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import { ExpandableString } from "src/views/shared/components/expandableString";
import Loading from "src/views/shared/components/loading";
import { PageConfig, PageConfigItem } from "src/views/shared/components/pageconfig";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { ColumnDescriptor, SortedTable } from "src/views/shared/components/sortedtable";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";

import spinner from "assets/spinner.gif";

type Job = protos.cockroach.server.serverpb.JobsResponse.Job;

type JobType = protos.cockroach.sql.jobs.Type;
const jobType = protos.cockroach.sql.jobs.Type;

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

class JobStatusCell extends React.Component<{ job: Job }, {}> {
  is(...statuses: string[]) {
    return statuses.indexOf(this.props.job.status) !== -1;
  }

  renderProgress() {
    if (this.is("succeeded", "failed", "canceled")) {
      return <div className="jobs-table__status">{this.props.job.status}</div>;
    }
    const percent = this.props.job.fraction_completed * 100;
    return <div>
      <Line percent={percent} strokeWidth={10} trailWidth={10} className="jobs-table__progress-bar" />
      <span title={percent.toFixed(3) + "%"}>{percent.toFixed(1) + "%"}</span>
    </div>;
  }

  renderDuration() {
    const started = TimestampToMoment(this.props.job.started);
    const finished = TimestampToMoment(this.props.job.finished);
    const modified = TimestampToMoment(this.props.job.modified);
    if (this.is("pending", "paused")) {
      return _.capitalize(this.props.job.status);
    } else if (this.is("running")) {
      const fractionCompleted = this.props.job.fraction_completed;
      if (fractionCompleted > 0) {
        const duration = modified.diff(started);
        const remaining = duration / fractionCompleted - duration;
        return formatDuration(moment.duration(remaining)) + " remaining";
      }
    } else if (this.is("succeeded")) {
      return "Duration: " + formatDuration(moment.duration(finished.diff(started)));
    }
  }

  render() {
    return <div>
      {this.renderProgress()}
      <span className="jobs-table__duration">{this.renderDuration()}</span>
    </div>;
  }
}

// Specialization of generic SortedTable component:
//   https://github.com/Microsoft/TypeScript/issues/3960
//
// The variable name must start with a capital letter or JSX will not recognize
// it as a component.
// tslint:disable-next-line:variable-name
const JobsSortedTable = SortedTable as new () => SortedTable<Job>;

const jobsTableColumns: ColumnDescriptor<Job>[] = [
  {
    title: "ID",
    cell: job => String(job.id),
    sort: job => job.id,
  },
  {
    title: "Description",
    cell: job => <ExpandableString long={job.description} />,
    sort: job => job.description,
    className: "jobs-table__cell--description",
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
  { sortKey: 2 /* creation time */, ascending: false },
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
    on the <a href={docsURL("pause-job.html")}><code>PAUSE JOB</code></a> and <a
    href={docsURL("cancel-job.html")}><code>CANCEL JOB</code></a> statements.
  </span>
);

class JobsTable extends React.Component<JobsTableProps, {}> {
  static title() {
    return (
      <div>
        Jobs
        <div className="section-heading__tooltip">
          <ToolTipWrapper text={titleTooltip}>
            <div className="section-heading__tooltip-hover-area">
              <div className="section-heading__info-icon">i</div>
            </div>
          </ToolTipWrapper>
        </div>
      </div>
    );
  }

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

  renderTable(jobs: Job[]) {
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
        />
      </section>
    );
  }

  render() {
    const data = this.props.jobs && this.props.jobs.length > 0 && this.props.jobs;
    return <div className="jobs-page">
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
      <Loading loading={_.isNil(this.props.jobs)} className="loading-image loading-image__spinner" image={spinner}>
          { this.renderTable(data) }
      </Loading>
    </div>;
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
