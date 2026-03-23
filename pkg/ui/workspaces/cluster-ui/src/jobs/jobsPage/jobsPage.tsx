// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { cockroach, google } from "@cockroachlabs/crdb-protobuf-client";
import { InlineAlert } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import moment from "moment-timezone";
import React, { useState, useEffect, useCallback } from "react";
import { Helmet } from "react-helmet";
import { useHistory } from "react-router-dom";

import { useJobs } from "src/api/jobsApi";
import ColumnsSelector, {
  SelectOption,
} from "src/columnsSelector/columnsSelector";
import { isSelectedColumn } from "src/columnsSelector/utils";
import { commonStyles } from "src/common";
import { Delayed } from "src/delayed";
import { Dropdown } from "src/dropdown";
import { Loading } from "src/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { Pagination, ResultsPerPageLabel } from "src/pagination";
import { ISortedTablePagination, SortSetting } from "src/sortedtable";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import { DATE_FORMAT_24_TZ, syncHistory, TimestampToMoment } from "src/util";

import { Timestamp } from "../../timestamp";
import styles from "../jobs.module.scss";
import {
  showOptions,
  statusOptions,
  typeOptions,
  isValidJobStatus,
  defaultRequestOptions,
  isValidJobType,
} from "../util";

import { jobsColumnLabels, JobsTable, makeJobsColumns } from "./jobsTable";

const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);

type ITimestamp = google.protobuf.ITimestamp;
type JobType = cockroach.sql.jobs.jobspb.Type;

export interface JobsPageStateProps {
  sort: SortSetting;
  status: string;
  show: string;
  type: number;
  columns: string[];
}

export interface JobsPageDispatchProps {
  setSort: (value: SortSetting) => void;
  setStatus: (value: string) => void;
  setShow: (value: string) => void;
  setType: (value: JobType) => void;
  onColumnsChange: (selectedColumns: string[]) => void;
}

export type JobsPageProps = JobsPageStateProps & JobsPageDispatchProps;

export function JobsPage(props: JobsPageProps): React.ReactElement {
  const {
    sort,
    status,
    type,
    show,
    columns: columnsToDisplay,
    onColumnsChange,
    setSort,
    setStatus,
    setShow,
    setType,
  } = props;
  const history = useHistory();

  const [pagination, setPagination] = useState<ISortedTablePagination>({
    pageSize: 20,
    current: 1,
  });

  const showAsInt = parseInt(show, 10);
  const limit = isNaN(showAsInt) ? 0 : showAsInt;

  const {
    data: jobs,
    error: jobsError,
    isLoading,
  } = useJobs(status, type as JobType, limit, {
    refreshInterval: 10 * 1000,
    keepPreviousData: true,
  });

  // Sync URL params to state on mount.
  useEffect(() => {
    const searchParams = new URLSearchParams(history.location.search);

    // Sort Settings.
    const ascending = (searchParams.get("ascending") || undefined) === "true";
    const columnTitle = searchParams.get("columnTitle") || undefined;
    if (
      setSort &&
      columnTitle &&
      (sort.columnTitle !== columnTitle || sort.ascending !== ascending)
    ) {
      setSort({ columnTitle, ascending });
    }

    // Filter Status.
    const urlStatus = searchParams.get("status");
    if (setStatus && urlStatus && urlStatus !== status) {
      setStatus(urlStatus);
    }

    // Filter Show.
    const urlShow = searchParams.get("show") || undefined;
    if (setShow && urlShow && urlShow !== show) {
      setShow(urlShow);
    }

    // Filter Type.
    const urlType = parseInt(searchParams.get("type"), 10) || undefined;
    if (setType && urlType && urlType !== type) {
      setType(urlType);
    }
    // Only run on mount.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Validate status and type on every render.
  useEffect(() => {
    if (!isValidJobStatus(status)) {
      setStatus(defaultRequestOptions.status);
      syncHistory({ status: defaultRequestOptions.status }, history);
    }
  }, [status, setStatus, history]);

  useEffect(() => {
    if (!isValidJobType(type)) {
      setType(defaultRequestOptions.type);
      syncHistory({ type: defaultRequestOptions.type.toString() }, history);
    }
  }, [type, setType, history]);

  const handleSetPagination = (current: number, pageSize: number): void => {
    setPagination(prev => ({ ...prev, current, pageSize }));
  };

  const resetPagination = useCallback((): void => {
    setPagination(prev => ({ current: 1, pageSize: prev.pageSize }));
  }, []);

  const onStatusSelected = useCallback(
    (item: string): void => {
      setStatus(item);
      resetPagination();
      syncHistory({ status: item }, history);
    },
    [setStatus, resetPagination, history],
  );

  const onTypeSelected = useCallback(
    (item: string): void => {
      const typeVal = parseInt(item, 10);
      setType(typeVal);
      resetPagination();
      syncHistory({ type: typeVal.toString() }, history);
    },
    [setType, resetPagination, history],
  );

  const onShowSelected = useCallback(
    (item: string): void => {
      setShow(item);
      resetPagination();
      syncHistory({ show: item }, history);
    },
    [setShow, resetPagination, history],
  );

  const changeSortSetting = useCallback(
    (ss: SortSetting): void => {
      if (setSort) {
        setSort(ss);
      }
      syncHistory(
        {
          ascending: ss.ascending.toString(),
          columnTitle: ss.columnTitle,
        },
        history,
      );
    },
    [setSort, history],
  );

  const formatJobsRetentionMessage = (earliestRetainedTime: ITimestamp) => {
    return (
      <>
        Since{" "}
        <Timestamp
          time={TimestampToMoment(earliestRetainedTime)}
          format={DATE_FORMAT_24_TZ}
        />
      </>
    );
  };

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
    isSelectedColumn(columnsToDisplay, c),
  );

  return (
    <div>
      <Helmet title="Jobs" />
      <h3 className={commonStyles("base-heading")}>Jobs</h3>
      <div>
        <PageConfig>
          <PageConfigItem>
            <Dropdown items={statusOptions} onChange={onStatusSelected}>
              Status:{" "}
              {statusOptions.find(option => option.value === status)?.name}
            </Dropdown>
          </PageConfigItem>
          <PageConfigItem>
            <Dropdown items={typeOptions} onChange={onTypeSelected}>
              Type:{" "}
              {
                typeOptions.find(option => option.value === type.toString())
                  ?.name
              }
            </Dropdown>
          </PageConfigItem>
          <PageConfigItem>
            <Dropdown items={showOptions} onChange={onShowSelected}>
              Show: {showOptions.find(option => option.value === show)?.name}
            </Dropdown>
          </PageConfigItem>
        </PageConfig>
      </div>
      <div className={cx("table-area")}>
        {jobsError && jobs && (
          <InlineAlert intent="danger" title={jobsError.message} />
        )}
        <Loading
          loading={isLoading}
          page={"jobs"}
          error={!jobs ? jobsError : null}
        >
          <div>
            <section className={sortableTableCx("cl-table-container")}>
              <div className={sortableTableCx("cl-table-statistic")}>
                <ColumnsSelector
                  options={tableColumns}
                  onSubmitColumns={onColumnsChange}
                  size={"small"}
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
                        {formatJobsRetentionMessage(
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
                onChangeSortSetting={changeSortSetting}
                visibleColumns={displayColumns}
                pagination={pagination}
              />
            </section>
            <Pagination
              pageSize={pagination.pageSize}
              onShowSizeChange={handleSetPagination}
              current={pagination.current}
              total={filteredJobs.length}
              onChange={handleSetPagination}
            />
          </div>
        </Loading>
        {isLoading && !jobsError && (
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
