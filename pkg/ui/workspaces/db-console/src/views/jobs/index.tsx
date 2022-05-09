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
import { RouteComponentProps, withRouter } from "react-router-dom";

import { cockroach } from "src/js/protos";
import { jobsKey, refreshJobs, refreshSettings } from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import { Loading, Delayed, util, SortSetting } from "@cockroachlabs/cluster-ui";
import { InlineAlert } from "@cockroachlabs/ui-components";
import {
  PageConfig,
  PageConfigItem,
} from "src/views/shared/components/pageconfig";
import "./index.styl";
import { statusOptions } from "./jobStatusOptions";
import { JobTable } from "src/views/jobs/jobTable";
import { trackFilter } from "src/util/analytics";
import JobType = cockroach.sql.jobs.jobspb.Type;
import JobsRequest = cockroach.server.serverpb.JobsRequest;
import JobsResponse = cockroach.server.serverpb.JobsResponse;
import { createSelector } from "reselect";
import { selectClusterSettings } from "src/redux/clusterSettings";

export const statusSetting = new LocalSetting<AdminUIState, string>(
  "jobs/status_setting",
  s => s.localSettings,
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
  { value: JobType.SCHEMA_CHANGE_GC.toString(), label: "Schema Change GC" },
  {
    value: JobType.TYPEDESC_SCHEMA_CHANGE.toString(),
    label: "Type Descriptor Schema Changes",
  },
  { value: JobType.STREAM_INGESTION.toString(), label: "Stream Ingestion" },
  { value: JobType.NEW_SCHEMA_CHANGE.toString(), label: "New Schema Changes" },
  { value: JobType.MIGRATION.toString(), label: "Migrations" },
  {
    value: JobType.AUTO_SPAN_CONFIG_RECONCILIATION.toString(),
    label: "Span Config Reconciliation",
  },
  {
    value: JobType.AUTO_SQL_STATS_COMPACTION.toString(),
    label: "SQL Stats Compactions",
  },
  { value: JobType.STREAM_REPLICATION.toString(), label: "Stream Replication" },
  {
    value: JobType.ROW_LEVEL_TTL.toString(),
    label: "Time-to-live Deletions",
  },
];

export const typeSetting = new LocalSetting<AdminUIState, number>(
  "jobs/type_setting",
  s => s.localSettings,
  JobType.UNSPECIFIED,
);

const showOptions = [
  { value: "50", label: "Latest 50" },
  { value: "0", label: "All" },
];

export const showSetting = new LocalSetting<AdminUIState, string>(
  "jobs/show_setting",
  s => s.localSettings,
  showOptions[0].value,
);

// Moment cannot render durations (moment/moment#1048). Hack it ourselves.
export const formatDuration = (d: moment.Duration) =>
  [Math.floor(d.asHours()).toFixed(0), d.minutes(), d.seconds()]
    .map(c => (c < 10 ? ("0" + c).slice(-2) : c))
    .join(":");

export const sortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "sortSetting/Jobs",
  s => s.localSettings,
  { columnTitle: "creationTime", ascending: false },
);

const selectRetentionTime = createSelector(selectClusterSettings, settings => {
  if (!settings) {
    return null;
  }
  const value = settings["jobs.retention_time"]?.value;
  return util.durationFromISO8601String(value);
});

export interface JobsTableOwnProps {
  sort: SortSetting;
  status: string;
  show: string;
  type: number;
  setSort: (value: SortSetting) => void;
  setStatus: (value: string) => void;
  setShow: (value: string) => void;
  setType: (value: JobType) => void;
  refreshJobs: typeof refreshJobs;
  refreshSettings: typeof refreshSettings;
  jobs: CachedDataReducerState<JobsResponse>;
  retentionTime: moment.Duration | null;
}

export type JobsTableProps = JobsTableOwnProps & RouteComponentProps<any>;

export class JobsTable extends React.Component<JobsTableProps> {
  constructor(props: JobsTableProps) {
    super(props);

    const { history } = this.props;
    const searchParams = new URLSearchParams(history.location.search);

    // Sort Settings.
    const ascending = (searchParams.get("ascending") || undefined) === "true";
    const columnTitle = searchParams.get("columnTitle") || undefined;
    const sortSetting = this.props.sort;
    if (
      this.props.setSort &&
      columnTitle &&
      (sortSetting.columnTitle != columnTitle ||
        sortSetting.ascending != ascending)
    ) {
      this.props.setSort({ columnTitle, ascending });
    }

    // Filter Status.
    const status = searchParams.get("status") || undefined;
    if (this.props.setStatus && status && status != this.props.status) {
      this.props.setStatus(status);
    }

    // Filter Show.
    const show = searchParams.get("show") || undefined;
    if (this.props.setShow && show && show != this.props.show) {
      this.props.setShow(show);
    }

    // Filter Type.
    const type = parseInt(searchParams.get("type"), 10) || undefined;
    if (this.props.setType && type && type != this.props.type) {
      this.props.setType(type);
    }
  }

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
    this.props.refreshSettings();
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

    util.syncHistory(
      {
        status: selected.value,
      },
      this.props.history,
    );
  };

  onTypeSelected = (selected: DropdownOption) => {
    const type = parseInt(selected.value, 10);
    const typeLabel = typeOptions[type].label;
    trackFilter("Type", typeLabel);
    this.props.setType(type);

    util.syncHistory(
      {
        type: type.toString(),
      },
      this.props.history,
    );
  };

  onShowSelected = (selected: DropdownOption) => {
    this.props.setShow(selected.value);

    util.syncHistory(
      {
        show: selected.value,
      },
      this.props.history,
    );
  };

  changeSortSetting = (ss: SortSetting): void => {
    if (this.props.setSort) {
      this.props.setSort(ss);
    }

    util.syncHistory(
      {
        ascending: ss.ascending.toString(),
        columnTitle: ss.columnTitle,
      },
      this.props.history,
    );
  };

  render() {
    const isLoading = !this.props.jobs || !this.props.jobs.data;
    const error = this.props.jobs && this.props.jobs.lastError;
    return (
      <div className="jobs-page">
        <Helmet title="Jobs" />
        <h3 className="base-heading">Jobs</h3>
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
            loading={isLoading}
            page={"jobs"}
            error={error}
            render={() => (
              <JobTable
                isUsedFilter={
                  this.props.status.length > 0 || this.props.type > 0
                }
                jobs={this.props.jobs}
                setSort={this.changeSortSetting}
                sort={this.props.sort}
                retentionTime={this.props.retentionTime}
              />
            )}
          />
          {isLoading && !error && (
            <Delayed delay={moment.duration(2, "s")}>
              <InlineAlert
                intent="info"
                title="If the Jobs table contains a large amount of data, this page might take a while to load. To reduce the amount of data, try filtering the table."
              />
            </Delayed>
          )}
        </section>
      </div>
    );
  }
}

const mapStateToProps = (state: AdminUIState, _: RouteComponentProps) => {
  const sort = sortSetting.selector(state);
  const status = statusSetting.selector(state);
  const show = showSetting.selector(state);
  const type = typeSetting.selector(state);
  const key = jobsKey(status, type, parseInt(show, 10));
  const jobs = state.cachedData.jobs[key];
  const retentionTime = selectRetentionTime(state);
  return {
    sort,
    status,
    show,
    type,
    jobs,
    retentionTime,
  };
};

const mapDispatchToProps = {
  setSort: sortSetting.set,
  setStatus: statusSetting.set,
  setShow: showSetting.set,
  setType: typeSetting.set,
  refreshJobs,
  refreshSettings,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(JobsTable),
);
