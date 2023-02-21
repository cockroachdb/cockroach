// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import { cockroach, google } from "@cockroachlabs/crdb-protobuf-client";
import { InlineAlert } from "@cockroachlabs/ui-components";
import moment from "moment";
import React from "react";
import { Helmet } from "react-helmet";
import { RouteComponentProps } from "react-router-dom";
import { JobsRequest, JobsResponse } from "src/api/jobsApi";
import { Delayed } from "src/delayed";
import { Dropdown } from "src/dropdown";
import { Loading } from "src/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { ISortedTablePagination, SortSetting } from "src/sortedtable";
import ColumnsSelector, {
  SelectOption,
} from "src/columnsSelector/columnsSelector";
import { Pagination, ResultsPerPageLabel } from "src/pagination";
import { isSelectedColumn } from "src/columnsSelector/utils";
import { DATE_FORMAT_24_UTC, syncHistory, TimestampToMoment } from "src/util";
import { jobsColumnLabels, JobsTable, makeJobsColumns } from "./jobsTable";
import { showOptions, statusOptions, typeOptions } from "../util";

import { commonStyles } from "src/common";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import styles from "../jobs.module.scss";
import classNames from "classnames/bind";

const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);

type ITimestamp = google.protobuf.ITimestamp;
type JobType = cockroach.sql.jobs.jobspb.Type;

export interface JobsPageStateProps {
  sort: SortSetting;
  status: string;
  show: string;
  type: number;
  jobs: JobsResponse;
  jobsError: Error | null;
  reqInFlight: boolean;
  isDataValid: boolean;
  columns: string[];
  lastUpdated: moment.Moment | null;
}

export interface JobsPageDispatchProps {
  setSort: (value: SortSetting) => void;
  setStatus: (value: string) => void;
  setShow: (value: string) => void;
  setType: (value: JobType) => void;
  onColumnsChange: (selectedColumns: string[]) => void;
  refreshJobs: (req: JobsRequest) => void;
}

interface PageState {
  pagination: ISortedTablePagination;
}

export type JobsPageProps = JobsPageStateProps &
  JobsPageDispatchProps &
  RouteComponentProps;

const reqFromProps = (
  props: JobsPageStateProps,
): cockroach.server.serverpb.JobsRequest => {
  const showAsInt = parseInt(props.show, 10);
  return new cockroach.server.serverpb.JobsRequest({
    limit: isNaN(showAsInt) ? 0 : showAsInt,
    status: props.status,
    type: props.type,
  });
};

export class JobsPage extends React.Component<JobsPageProps, PageState> {
  refreshDataInterval: NodeJS.Timeout;

  constructor(props: JobsPageProps) {
    super(props);
    this.state = {
      pagination: {
        pageSize: 20,
        current: 1,
      },
    };
    const { history } = this.props;
    const searchParams = new URLSearchParams(history.location.search);

    // Sort Settings.
    const ascending = (searchParams.get("ascending") || undefined) === "true";
    const columnTitle = searchParams.get("columnTitle") || undefined;
    const sortSetting = this.props.sort;
    if (
      this.props.setSort &&
      columnTitle &&
      (sortSetting.columnTitle !== columnTitle ||
        sortSetting.ascending !== ascending)
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
    if (this.props.setShow && show && show !== this.props.show) {
      this.props.setShow(show);
    }

    // Filter Type.
    const type = parseInt(searchParams.get("type"), 10) || undefined;
    if (this.props.setType && type && type !== this.props.type) {
      this.props.setType(type);
    }
  }

  scheduleFetch(): void {
    clearTimeout(this.refreshDataInterval);
    const now = moment.utc();
    const nextRefresh =
      !this.props.isDataValid && !this.props.jobsError
        ? now
        : this.props.lastUpdated?.clone().add(10, "seconds") ?? now;
    const msToNextRefresh = Math.max(0, nextRefresh.diff(now, "millisecond"));
    this.refreshDataInterval = setTimeout(() => {
      const req = reqFromProps(this.props);
      this.props.refreshJobs(req);
    }, msToNextRefresh);
  }

  componentDidMount(): void {
    this.scheduleFetch();
  }

  componentDidUpdate(prevProps: JobsPageProps): void {
    if (
      prevProps.lastUpdated !== this.props.lastUpdated ||
      prevProps.show !== this.props.show ||
      prevProps.status !== this.props.status ||
      prevProps.type !== this.props.type
    ) {
      this.scheduleFetch();
    }
  }

  componentWillUnmount(): void {
    clearTimeout(this.refreshDataInterval);
  }

  onChangePage = (current: number): void => {
    const { pagination } = this.state;
    this.setState({ pagination: { ...pagination, current } });
  };

  resetPagination = (): void => {
    this.setState((prevState: PageState) => {
      return {
        pagination: {
          current: 1,
          pageSize: prevState.pagination.pageSize,
        },
      };
    });
  };

  onStatusSelected = (item: string): void => {
    this.props.setStatus(item);
    this.resetPagination();
    syncHistory(
      {
        status: item,
      },
      this.props.history,
    );
  };

  onTypeSelected = (item: string): void => {
    const type = parseInt(item, 10);
    this.props.setType(type);
    this.resetPagination();
    syncHistory(
      {
        type: type.toString(),
      },
      this.props.history,
    );
  };

  onShowSelected = (item: string): void => {
    this.props.setShow(item);
    this.resetPagination();
    syncHistory(
      {
        show: item,
      },
      this.props.history,
    );
  };

  changeSortSetting = (ss: SortSetting): void => {
    if (this.props.setSort) {
      this.props.setSort(ss);
    }

    syncHistory(
      {
        ascending: ss.ascending.toString(),
        columnTitle: ss.columnTitle,
      },
      this.props.history,
    );
  };

  formatJobsRetentionMessage = (earliestRetainedTime: ITimestamp): string => {
    return `Since ${TimestampToMoment(earliestRetainedTime).format(
      DATE_FORMAT_24_UTC,
    )}`;
  };

  render(): React.ReactElement {
    const {
      jobs,
      jobsError,
      sort,
      status,
      reqInFlight,
      isDataValid,
      type,
      show,
      columns: columnsToDisplay,
      onColumnsChange,
    } = this.props;
    const isLoading = reqInFlight && (!isDataValid || !jobs);
    const error = jobs && jobsError;
    const { pagination } = this.state;
    const filteredJobs = jobs?.jobs ?? [];
    const columns = makeJobsColumns();
    // Iterate over all available columns and create list of SelectOptions with initial selection
    // values based on stored user selections in local storage and default column configs.
    // Columns that are set to alwaysShow are filtered from the list.
    const tableColumns = columns
      .filter(c => !c.alwaysShow)
      .map(
        (c): SelectOption => ({
          label: jobsColumnLabels[c.name],
          value: c.name,
          isSelected: isSelectedColumn(columnsToDisplay, c),
        }),
      );

    // List of all columns that will be displayed based on the column selection.
    const displayColumns = columns.filter(c =>
      isSelectedColumn(this.props.columns, c),
    );

    return (
      <div>
        <Helmet title="Jobs" />
        <h3 className={commonStyles("base-heading")}>Jobs</h3>
        <div>
          <PageConfig>
            <PageConfigItem>
              <Dropdown items={statusOptions} onChange={this.onStatusSelected}>
                Status:{" "}
                {
                  statusOptions.find(option => option["value"] === status)[
                    "name"
                  ]
                }
              </Dropdown>
            </PageConfigItem>
            <PageConfigItem>
              <Dropdown items={typeOptions} onChange={this.onTypeSelected}>
                Type:{" "}
                {
                  typeOptions.find(
                    option => option["value"] === type.toString(),
                  )["name"]
                }
              </Dropdown>
            </PageConfigItem>
            <PageConfigItem>
              <Dropdown items={showOptions} onChange={this.onShowSelected}>
                Show:{" "}
                {showOptions.find(option => option["value"] === show)["name"]}
              </Dropdown>
            </PageConfigItem>
          </PageConfig>
        </div>
        <div className={cx("table-area")}>
          <Loading loading={isLoading} page={"jobs"} error={error}>
            <div>
              <section className={sortableTableCx("cl-table-container")}>
                <div className={sortableTableCx("cl-table-statistic")}>
                  <ColumnsSelector
                    options={tableColumns}
                    onSubmitColumns={onColumnsChange}
                  />
                  <div className={cx("jobs-table-summary")}>
                    <h4 className={cx("cl-count-title")}>
                      <ResultsPerPageLabel
                        pagination={{
                          ...pagination,
                          total: filteredJobs.length,
                        }}
                        pageName="jobs"
                      />
                      {jobs?.earliest_retained_time && (
                        <>
                          <span
                            className={cx(
                              "jobs-table-summary__retention-divider",
                            )}
                          >
                            |
                          </span>
                          {this.formatJobsRetentionMessage(
                            jobs?.earliest_retained_time,
                          )}
                        </>
                      )}
                    </h4>
                  </div>
                </div>
                <JobsTable
                  jobs={filteredJobs}
                  sortSetting={sort}
                  onChangeSortSetting={this.changeSortSetting}
                  visibleColumns={displayColumns}
                  pagination={pagination}
                />
              </section>
              <Pagination
                pageSize={pagination.pageSize}
                current={pagination.current}
                total={filteredJobs.length}
                onChange={this.onChangePage}
              />
            </div>
          </Loading>
          {isLoading && !error && (
            <Delayed delay={moment.duration(2, "s")}>
              <InlineAlert
                intent="info"
                title="If the Jobs table contains a large amount of data, this page might take a while to load."
              />
            </Delayed>
          )}
        </div>
      </div>
    );
  }
}
