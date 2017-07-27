import moment from "moment";
import React from "react";
import { connect } from "react-redux";
import { Line } from "rc-progress";
import * as protos from "src/js/protos";

import { AdminUIState } from "src/redux/state";
import { refreshJobs, jobsKey } from "src/redux/apiReducers";
import { LocalSetting } from "src/redux/localsettings";

import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import { PageConfig, PageConfigItem } from "src/views/shared/components/pageconfig";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { SortedTable } from "src/views/shared/components/sortedtable";
import { TimestampToMoment } from "src/util/convert";

type Job = protos.cockroach.server.serverpb.JobsResponse.Job;

const JobsRequest = protos.cockroach.server.serverpb.JobsRequest;

const sortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "jobs/sort_setting", s => s.localSettings,
);

const statusOptions = [
  { value: "", label: "All" },
  { value: "pending", label: "Pending" },
  { value: "running", label: "Running" },
  { value: "succeeded", label: "Succeeded" },
  { value: "failed", label: "Failed" },
];

const statusSetting = new LocalSetting<AdminUIState, string>(
  "jobs/status_setting", s => s.localSettings, statusOptions[0].value,
);

const typeOptions = [
  { value: "", label: "All" },
  { value: "BACKUP", label: "Backups" },
  { value: "RESTORE", label: "Restores" },
  { value: "SCHEMA CHANGE", label: "Schema changes" },
];

const typeSetting = new LocalSetting<AdminUIState, string>(
  "jobs/type_setting", s => s.localSettings, typeOptions[0].value,
);

const showOptions = [
  { value: "50", label: "First 50" },
  { value: "0", label: "All" },
];

const showSetting = new LocalSetting<AdminUIState, string>(
  "jobs/show_setting", s => s.localSettings, showOptions[0].value,
);

// Moment cannot render durations (moment/moment#1048). Hack it ourselves.
const formatDuration = (d: moment.Duration) =>
  [d.asHours().toFixed(0), d.minutes(), d.seconds()]
    .map(c => ("0" + c).slice(-2))
    .join(":");

class JobProgressCell extends React.Component<{job: Job}, {}> {
  isDone() {
    return ["pending", "running"].indexOf(this.props.job.status) === -1;
  }

  renderDuration() {
    const started = TimestampToMoment(this.props.job.started);
    const finished = TimestampToMoment(this.props.job.finished);
    const modified = TimestampToMoment(this.props.job.modified);
    if (this.isDone()) {
      return "Duration: " + formatDuration(moment.duration(finished.diff(started)));
    }
    return formatDuration(moment.duration(modified.diff(started) / this.props.job.fraction_completed)) +
      " remaining";
  }

  render() {
      const { job } = this.props;
      const percent = job.fraction_completed * 100;
      return <div>
        {!this.isDone() &&
          <Line
            percent={percent}
            strokeWidth={10} trailWidth={10}
            className="jobs-table__progress-bar"
          />}
        <span title={percent.toFixed(5) + "%"}>{percent.toFixed(1) + "%"}</span>
        <br />
        <em>{this.renderDuration()}</em>
      </div>;
  }
}

// Specialization of generic SortedTable component:
//   https://github.com/Microsoft/TypeScript/issues/3960
//
// The variable name must start with a capital letter or TSX will not recognize
// it as a component.
// tslint:disable-next-line:variable-name
const JobsSortedTable = SortedTable as new () => SortedTable<Job>;

interface JobsTableProps {
  sort: SortSetting;
  status: string;
  show: string;
  type: string;
  setSort: (value: SortSetting) => void;
  setStatus: (value: string) => void;
  setShow: (value: string) => void;
  setType: (value: string) => void;
  refreshJobs: typeof refreshJobs;
  jobs: Job[];
}

class JobsTable extends React.Component<JobsTableProps, {}> {
  static title() {
    return "Jobs";
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

  render() {
    return <div>
      <PageConfig>
        <PageConfigItem>
          <Dropdown
            title="Status"
            options={statusOptions}
            selected={this.props.status}
            onChange={(selected: DropdownOption) => this.props.setStatus(selected.value)}
            />
        </PageConfigItem>
        <PageConfigItem>
          <Dropdown
            title="Type"
            options={typeOptions}
            selected={this.props.type}
            onChange={(selected: DropdownOption) => this.props.setType(selected.value)}
            />
        </PageConfigItem>
        <PageConfigItem>
          <Dropdown
            title="Show"
            options={showOptions}
            selected={this.props.show}
            onChange={(selected: DropdownOption) => this.props.setShow(selected.value)}
            />
        </PageConfigItem>
      </PageConfig>
      <section className="section">
        <div className="content">
          <JobsSortedTable
            data={this.props.jobs}
            sortSetting={this.props.sort}
            onChangeSortSetting={(setting) => this.props.setSort(setting)}
            className="jobs-table"
            rowClass={job => "jobs-table__row--" + job.status}
            columns={[
              {
                title: "User",
                cell: job => job.username,
                sort: job => job.username,
              },
              {
                title: "Description",
                cell: job => job.description,
                sort: job => job.description,
                className: "jobs-table__cell--description",
              },
              {
                title: "Start Time",
                cell: job => TimestampToMoment(job.started).fromNow(),
                sort: job => TimestampToMoment(job.started).valueOf(),
              },
              {
                title: "Progress",
                cell: job => <JobProgressCell job={job} />,
                sort: job => job.fraction_completed,
              },
              {
                title: "Status",
                cell: job => job.status,
                sort: job => job.status,
                className: "jobs-table__cell--status",
              },
            ]}
            />
        </div>
      </section>
    </div>;
  }
}

export default connect(
  (state: AdminUIState) => {
    const sort = sortSetting.selector(state);
    const status = statusSetting.selector(state);
    const show = showSetting.selector(state);
    const type = typeSetting.selector(state);
    const key = jobsKey(status, type, parseInt(show, 10));
    const jobs = state.cachedData.jobs[key];
    return {
      sort, status, show, type, jobs: jobs && jobs.data && jobs.data.jobs,
    };
  },
  {
    setSort: sortSetting.set,
    setStatus: statusSetting.set,
    setShow: showSetting.set,
    setType: typeSetting.set,
    refreshJobs,
  },
)(JobsTable);
