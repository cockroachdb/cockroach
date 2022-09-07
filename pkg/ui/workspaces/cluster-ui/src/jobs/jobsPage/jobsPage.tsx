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
import { Dropdown, DropdownOption } from "src/dropdown";
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
import { statusOptions, showOptions, typeOptions } from "../util";

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
  jobsLoading: boolean;
  columns: string[];
}

export interface JobsPageDispatchProps {
  setSort: (value: SortSetting) => void;
  setStatus: (value: string) => void;
  setShow: (value: string) => void;
  setType: (value: JobType) => void;
  onColumnsChange: (selectedColumns: string[]) => void;
  refreshJobs: (req: JobsRequest) => void;
  onFilterChange?: (req: JobsRequest) => void;
}

interface PageState {
  pagination: ISortedTablePagination;
}

export type JobsPageProps = JobsPageStateProps &
  JobsPageDispatchProps &
  RouteComponentProps;

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

  refresh(): void {
    const jobsRequest = new cockroach.server.serverpb.JobsRequest({
      status: this.props.status,
      type: this.props.type,
      limit: parseInt(this.props.show, 10),
    });
    this.props.onFilterChange
      ? this.props.onFilterChange(jobsRequest)
      : this.props.refreshJobs(jobsRequest);
  }

  componentDidMount(): void {
    // Refresh every 10 seconds
    this.refresh();
    this.refreshDataInterval = setInterval(() => this.refresh(), 10 * 1000);
  }

  componentDidUpdate = (): void => {
    this.refresh();
  };

  componentWillUnmount(): void {
    if (!this.refreshDataInterval) return;
    clearInterval(this.refreshDataInterval);
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
    this.resetPagination();
    syncHistory(
      {
        status: item,
      },
      this.props.history,
    );

    this.props.setStatus(item);
  };

  statusMenuItems: DropdownOption[] = statusOptions;

  onTypeSelected = (item: string): void => {
    this.resetPagination();
    const type = parseInt(item, 10);

    syncHistory(
      {
        type: type.toString(),
      },
      this.props.history,
    );

    this.props.setType(type);
  };

  typeMenuItems: DropdownOption[] = typeOptions;

  onShowSelected = (item: string): void => {
    this.resetPagination();
    syncHistory(
      {
        show: item,
      },
      this.props.history,
    );

    this.props.setShow(item);
  };

  showMenuItems: DropdownOption[] = showOptions;

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
      jobsLoading,
      jobsError,
      sort,
      status,
      type,
      show,
      columns: columnsToDisplay,
      onColumnsChange,
    } = this.props;

    const isLoading = !jobs || jobsLoading;
    const error = jobs && jobsError;
    const { pagination } = this.state;

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
              <Dropdown
                items={this.statusMenuItems}
                onChange={this.onStatusSelected}
              >
                Status:{" "}
                {
                  statusOptions.find(option => option["value"] === status)[
                    "name"
                  ]
                }
              </Dropdown>
            </PageConfigItem>
            <PageConfigItem>
              <Dropdown
                items={this.typeMenuItems}
                onChange={this.onTypeSelected}
              >
                Type:{" "}
                {
                  typeOptions.find(
                    option => option["value"] === type.toString(),
                  )["name"]
                }
              </Dropdown>
            </PageConfigItem>
            <PageConfigItem>
              <Dropdown
                items={this.showMenuItems}
                onChange={this.onShowSelected}
              >
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
                          total: jobs?.jobs?.length,
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
                  jobs={jobs?.jobs}
                  sortSetting={sort}
                  onChangeSortSetting={this.changeSortSetting}
                  visibleColumns={displayColumns}
                  pagination={pagination}
                />
              </section>
              <Pagination
                pageSize={pagination.pageSize}
                current={pagination.current}
                total={jobs?.jobs?.length}
                onChange={this.onChangePage}
              />
            </div>
          </Loading>
          {isLoading && !error && (
            <Delayed delay={moment.duration(2, "s")}>
              <InlineAlert
                intent="info"
                title="If the Jobs table contains a large amount of data, this page might take a while to load. To reduce the amount of data, try filtering the table."
              />
            </Delayed>
          )}
        </div>
      </div>
    );
  }
}
