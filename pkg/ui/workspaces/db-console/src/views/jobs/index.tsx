// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment from "moment";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { withRouter } from "react-router-dom";

import { cockroach } from "src/js/protos";
import { jobsKey, refreshJobs } from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import { Loading } from "@cockroachlabs/cluster-ui";
import {
  PageConfig,
  PageConfigItem,
} from "src/views/shared/components/pageconfig";
import { SortSetting } from "src/views/shared/components/sortabletable";
import "./index.styl";
import { statusOptions } from "./jobStatusOptions";
import { JobTable } from "src/views/jobs/jobTable";
import { trackFilter } from "src/util/analytics";
import JobType = cockroach.sql.jobs.jobspb.Type;
import JobsRequest = cockroach.server.serverpb.JobsRequest;
import JobsResponse = cockroach.server.serverpb.JobsResponse;

export const statusSetting = new LocalSetting<AdminUIState, string>(
  "jobs/status_setting",
  (s) => s.localSettings,
  statusOptions[0].value,
);

const typeOptions = [
  { value: JobType.UNSPECIFIED.toString(), label: "All" },
  { value: JobType.BACKUP.toString(), label: "Backups" },
  { value: JobType.RESTORE.toString(), label: "Restores" },
  { value: JobType.IMPORT.toString(), label: "Imports" },
  { value: JobType.SCHEMA_CHANGE.toString(), label: "Schema Changes" },
  { value: JobType.CHANGEFEED.toString(), label: "Changefeed" },
  { value: JobType.CREATE_STATS.toString(), label: "Statistics Creation" },
  {
    value: JobType.AUTO_CREATE_STATS.toString(),
    label: "Auto-Statistics Creation",
  },
];

export const typeSetting = new LocalSetting<AdminUIState, number>(
  "jobs/type_setting",
  (s) => s.localSettings,
  JobType.UNSPECIFIED,
);

const showOptions = [
  { value: "50", label: "Latest 50" },
  { value: "0", label: "All" },
];

export const showSetting = new LocalSetting<AdminUIState, string>(
  "jobs/show_setting",
  (s) => s.localSettings,
  showOptions[0].value,
);

// Moment cannot render durations (moment/moment#1048). Hack it ourselves.
export const formatDuration = (d: moment.Duration) =>
  [Math.floor(d.asHours()).toFixed(0), d.minutes(), d.seconds()]
    .map((c) => (c < 10 ? ("0" + c).slice(-2) : c))
    .join(":");

export const sortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "jobs/sort_setting",
  (s) => s.localSettings,
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
  refresh(props = this.props) {
    props.refreshJobs(
      new JobsRequest({
        status: props.status,
        type: props.type,
        limit: parseInt(props.show, 10),
      }),
    );
  }

  componentDidMount() {
    this.refresh();
  }

  componentDidUpdate(prevProps: JobsTableProps) {
    if (
      prevProps.status !== this.props.status ||
      prevProps.type !== this.props.type ||
      prevProps.show !== this.props.show
    ) {
      this.refresh(this.props);
    }
  }

  onStatusSelected = (selected: DropdownOption) => {
    const filter = selected.value === "" ? "all" : selected.value;
    trackFilter("Status", filter);
    this.props.setStatus(selected.value);
  };

  onTypeSelected = (selected: DropdownOption) => {
    const type = parseInt(selected.value, 10);
    const typeLabel = typeOptions[type].label;
    trackFilter("Type", typeLabel);
    this.props.setType(type);
  };

  onShowSelected = (selected: DropdownOption) => {
    this.props.setShow(selected.value);
  };

  render() {
    return (
      <div className="jobs-page">
        <Helmet title="Jobs" />
        <section className="section">
          <h1 className="base-heading">Jobs</h1>
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
        <section className="section">
          <Loading
            loading={!this.props.jobs || !this.props.jobs.data}
            error={this.props.jobs && this.props.jobs.lastError}
            render={() => (
              <JobTable
                isUsedFilter={
                  this.props.status.length > 0 || this.props.type > 0
                }
                jobs={this.props.jobs}
                setSort={this.props.setSort}
                sort={this.props.sort}
              />
            )}
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
    sort,
    status,
    show,
    type,
    jobs,
  };
};

const mapDispatchToProps = {
  setSort: sortSetting.set,
  setStatus: statusSetting.set,
  setShow: showSetting.set,
  setType: typeSetting.set,
  refreshJobs,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(JobsTable),
);
