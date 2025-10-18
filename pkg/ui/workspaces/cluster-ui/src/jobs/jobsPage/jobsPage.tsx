// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { cockroach, google } from "@cockroachlabs/crdb-protobuf-client";
import { InlineAlert } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import moment from "moment-timezone";
import React, { useEffect, useState } from "react";
import { Helmet } from "react-helmet";
import { RouteComponentProps, useHistory, useLocation } from "react-router-dom";
import useSWR from "swr";

import { JobsRequest, getJobs } from "src/api/jobsApi";
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

export type JobsPageProps = RouteComponentProps;

const reqFromProps = (
  status: string,
  show: string,
  type: number,
): JobsRequest => {
  const showAsInt = parseInt(show, 10);
  return new cockroach.server.serverpb.JobsRequest({
    limit: isNaN(showAsInt) ? 0 : showAsInt,
    status,
    type,
  });
};

export const JobsPage: React.FC<JobsPageProps> = () => {
  const history = useHistory();
  const location = useLocation();
  const searchParams = new URLSearchParams(location.search);

  // State hooks
  const [sort, setSort] = useState<SortSetting>(() => {
    const ascending = (searchParams.get("ascending") || undefined) === "true";
    const columnTitle = searchParams.get("columnTitle") || undefined;
    return { columnTitle, ascending };
  });

  const [status, setStatus] = useState<string>(
    () => searchParams.get("status") || defaultRequestOptions.status,
  );

  const [show, setShow] = useState<string>(
    () => searchParams.get("show") || defaultRequestOptions.limit.toString(),
  );

  const [type, setType] = useState<number>(() => {
    const typeParam = parseInt(searchParams.get("type"), 10);
    return typeParam || defaultRequestOptions.type;
  });

  const [columns, setColumns] = useState<string[]>([]);
  const [pagination, setPagination] = useState<ISortedTablePagination>({
    pageSize: 20,
    current: 1,
  });

  // Create request object for SWR
  const request = reqFromProps(status, show, type);

  // Use SWR for data fetching
  const {
    data: jobsResponse,
    error: jobsError,
    isValidating: isLoading,
  } = useSWR(
    // Create a stable key from the request parameters
    [`jobs`, status, show, type],
    () => getJobs(request),
    {
      refreshInterval: 10000, // Refresh every 10 seconds
      dedupingInterval: 5000, // Dedupe requests within 5 seconds
      revalidateOnFocus: false, // Don't revalidate when window regains focus
      revalidateOnReconnect: false, // Don't revalidate when reconnecting
    },
  );

  // Handle URL sync
  useEffect(() => {
    if (!isValidJobStatus(status)) {
      setStatus(defaultRequestOptions.status);
    }

    if (!isValidJobType(type)) {
      setType(defaultRequestOptions.type);
    }
  }, [status, type]);

  const setPaginationWithReset = (current: number, pageSize: number): void => {
    setPagination({ current, pageSize });
  };

  const resetPagination = (): void => {
    setPagination(prev => ({
      current: 1,
      pageSize: prev.pageSize,
    }));
  };

  const onStatusSelected = (item: string): void => {
    setStatus(item);
    resetPagination();
    syncHistory(
      {
        status: item,
      },
      history,
    );
  };

  const onTypeSelected = (item: string): void => {
    const typeValue = parseInt(item, 10);
    setType(typeValue);
    resetPagination();
    syncHistory(
      {
        type: typeValue.toString(),
      },
      history,
    );
  };

  const onShowSelected = (item: string): void => {
    setShow(item);
    resetPagination();
    syncHistory(
      {
        show: item,
      },
      history,
    );
  };

  const changeSortSetting = (ss: SortSetting): void => {
    setSort(ss);
    syncHistory(
      {
        ascending: ss.ascending.toString(),
        columnTitle: ss.columnTitle,
      },
      history,
    );
  };

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

  const jobs = jobsResponse?.jobs ?? [];
  const columnsList = makeJobsColumns();

  // Create list of SelectOptions for column selection
  const tableColumns = columnsList
    .filter(c => !c.alwaysShow)
    .map(
      (c): SelectOption => ({
        label: jobsColumnLabels[c.name],
        value: c.name,
        isSelected: isSelectedColumn(columns, c),
      }),
    );

  // List of all columns that will be displayed based on the column selection
  const displayColumns = columnsList.filter(c => isSelectedColumn(columns, c));

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
                  onSubmitColumns={setColumns}
                  size={"small"}
                />
                <div className={cx("jobs-table-summary")}>
                  <h4 className={cx("cl-count-title")}>
                    <ResultsPerPageLabel
                      pagination={{
                        ...pagination,
                        total: jobs.length,
                      }}
                      pageName="jobs"
                    />
                    {jobsResponse?.earliest_retained_time && (
                      <>
                        <span
                          className={cx(
                            "jobs-table-summary__retention-divider",
                          )}
                        >
                          |
                        </span>
                        {formatJobsRetentionMessage(
                          jobsResponse.earliest_retained_time,
                        )}
                      </>
                    )}
                  </h4>
                </div>
              </div>
              <JobsTable
                jobs={jobs}
                sortSetting={sort}
                onChangeSortSetting={changeSortSetting}
                visibleColumns={displayColumns}
                pagination={pagination}
              />
            </section>
            <Pagination
              pageSize={pagination.pageSize}
              onShowSizeChange={setPaginationWithReset}
              current={pagination.current}
              total={jobs.length}
              onChange={setPaginationWithReset}
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
};
