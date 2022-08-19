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
import { Tooltip } from "@cockroachlabs/ui-components";
import { isEqual, map } from "lodash";
import React from "react";
import { Nodes, MagnifyingGlass } from "@cockroachlabs/icons";
import { Anchor } from "src/anchor";
import { JobsResponse } from "src/api/jobsApi";
import { EmptyTable } from "src/empty";
import { Pagination, ResultsPerPageLabel } from "src/pagination";
import { ColumnDescriptor, SortSetting, SortedTable } from "src/sortedtable";
import { TimestampToMoment } from "src/util";
import {
  cancelJob,
  jobStatus,
  jobTable,
  pauseJob,
  resumeJob,
} from "src/util/docs";
import { DATE_FORMAT_24_UTC } from "src/util/format";

import { HighwaterTimestamp, JobStatusCell } from "../util";
import { JobDescriptionCell } from "./jobDescriptionCell";

import styles from "../jobs.module.scss";
import classNames from "classnames/bind";
const cx = classNames.bind(styles);

type ITimestamp = google.protobuf.ITimestamp;
type Job = cockroach.server.serverpb.IJobResponse;

class JobsSortedTable extends SortedTable<Job> {}

const jobsTableColumns: ColumnDescriptor<Job>[] = [
  {
    name: "description",
    title: (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <p>
            The description of the job, if set, or the SQL statement if there is
            no job description.
          </p>
        }
      >
        {"Description"}
      </Tooltip>
    ),
    className: cx("cl-table__col-query-text"),
    cell: job => <JobDescriptionCell job={job} />,
    sort: job => job.statement || job.description || job.type,
  },
  {
    name: "status",
    title: (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <p>
            {"Current "}
            <Anchor href={jobStatus} target="_blank">
              job status
            </Anchor>
            {
              " or completion progress, and the total time the job took to complete."
            }
          </p>
        }
      >
        {"Status"}
      </Tooltip>
    ),
    cell: job => <JobStatusCell job={job} compact />,
    sort: job => job.fraction_completed,
  },
  {
    name: "jobId",
    title: (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <p>
            {"Unique job ID. This value is used to "}
            <Anchor href={pauseJob} target="_blank">
              pause
            </Anchor>
            {", "}
            <Anchor href={resumeJob} target="_blank">
              resume
            </Anchor>
            {", or "}
            <Anchor href={cancelJob} target="_blank">
              cancel
            </Anchor>
            {" jobs."}
          </p>
        }
      >
        {"Job ID"}
      </Tooltip>
    ),
    titleAlign: "right",
    cell: job => String(job.id),
    sort: job => job.id?.toNumber(),
  },
  {
    name: "users",
    title: (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={<p>User that created the job.</p>}
      >
        {"User Name"}
      </Tooltip>
    ),
    cell: job => job.username,
    sort: job => job.username,
  },
  {
    name: "creationTime",
    title: (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={<p>Date and time the job was created.</p>}
      >
        {"Creation Time (UTC)"}
      </Tooltip>
    ),
    cell: job => TimestampToMoment(job?.created).format(DATE_FORMAT_24_UTC),
    sort: job => TimestampToMoment(job?.created).valueOf(),
  },
  {
    name: "lastModifiedTime",
    title: (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={<p>Date and time the job was last modified.</p>}
      >
        {"Modified Time (UTC)"}
      </Tooltip>
    ),
    cell: job => TimestampToMoment(job?.modified).format(DATE_FORMAT_24_UTC),
    sort: job => TimestampToMoment(job?.modified).valueOf(),
  },
  {
    name: "lastExecutionTime",
    title: (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={<p>Date and time the job was last executed.</p>}
      >
        {"Last Execution Time (UTC)"}
      </Tooltip>
    ),
    cell: job => TimestampToMoment(job?.last_run).format(DATE_FORMAT_24_UTC),
    sort: job => TimestampToMoment(job?.last_run).valueOf(),
  },
  {
    name: "highWaterTimestamp",
    title: (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <p>
            The high-water mark acts as a checkpoint for the changefeed’s job
            progress, and guarantees that all changes before (or at) the
            timestamp have been emitted.
          </p>
        }
      >
        {"High-water Timestamp"}
      </Tooltip>
    ),
    cell: job =>
      job.highwater_timestamp ? (
        <HighwaterTimestamp
          timestamp={job.highwater_timestamp}
          decimalString={job.highwater_decimal}
        />
      ) : null,
    sort: job => TimestampToMoment(job?.highwater_timestamp).valueOf(),
  },
  {
    name: "executionCount",
    title: (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={<p>Number of times the job was executed.</p>}
      >
        {"Execution Count"}
      </Tooltip>
    ),
    cell: job => String(job.num_runs),
    sort: job => job.num_runs?.toNumber(),
  },
  {
    name: "coordinatorID",
    title: (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={<p>ID of the coordinating node.</p>}
      >
        {"Coordinator Node"}
      </Tooltip>
    ),
    // If the coordinator ID is unset, we don't want to display anything, but
    // the default value of `0` is fine for sorting.
    cell: job =>
      Object.prototype.hasOwnProperty.call(job, "coordinator_id")
        ? String(job.coordinator_id)
        : "",
    sort: job => job.coordinator_id?.toNumber(),
  },
];

export interface JobTableProps {
  sort: SortSetting;
  setSort: (value: SortSetting) => void;
  jobs: JobsResponse;
  pageSize?: number;
  current?: number;
  isUsedFilter: boolean;
}

export interface JobTableState {
  pagination: {
    pageSize: number;
    current: number;
  };
}

export class JobTable extends React.Component<JobTableProps, JobTableState> {
  constructor(props: JobTableProps) {
    super(props);

    this.state = {
      pagination: {
        pageSize: props.pageSize || 20,
        current: props.current || 1,
      },
    };
  }

  componentDidUpdate(prevProps: Readonly<JobTableProps>): void {
    this.setCurrentPageToOneIfJobsChanged(prevProps);
  }

  onChangePage = (current: number): void => {
    const { pagination } = this.state;
    this.setState({ pagination: { ...pagination, current } });
  };

  renderEmptyState = (): React.ReactElement => {
    const { isUsedFilter, jobs } = this.props;
    const hasData = jobs?.jobs.length > 0;

    if (hasData) {
      return null;
    }

    if (isUsedFilter) {
      return (
        <EmptyTable
          title="No jobs match your search"
          icon={<MagnifyingGlass />}
          footer={
            <Anchor href={jobTable} target="_blank">
              Learn more about jobs
            </Anchor>
          }
        />
      );
    } else {
      return (
        <EmptyTable
          title="No jobs to show"
          icon={<Nodes />}
          message="The jobs page provides details about backup/restore jobs, schema changes, user-created table statistics, automatic table statistics jobs and changefeeds."
          footer={
            <Anchor href={jobTable} target="_blank">
              Learn more about jobs
            </Anchor>
          }
        />
      );
    }
  };

  formatJobsRetentionMessage = (earliestRetainedTime: ITimestamp): string => {
    return `Since ${TimestampToMoment(earliestRetainedTime).format(
      DATE_FORMAT_24_UTC,
    )}`;
  };

  render(): React.ReactElement {
    const jobs = this.props.jobs.jobs;
    const { pagination } = this.state;

    return (
      <React.Fragment>
        <div className={cx("cl-table-statistic jobs-table-summary")}>
          <h4 className={cx("cl-count-title")}>
            <ResultsPerPageLabel
              pagination={{ ...pagination, total: jobs.length }}
              pageName="jobs"
            />
            {this.props.jobs.earliest_retained_time && (
              <>
                <span className={cx("jobs-table-summary__retention-divider")}>
                  |
                </span>
                {this.formatJobsRetentionMessage(
                  this.props.jobs.earliest_retained_time,
                )}
              </>
            )}
          </h4>
        </div>
        <JobsSortedTable
          data={jobs}
          sortSetting={this.props.sort}
          onChangeSortSetting={this.props.setSort}
          className={cx("jobs-table")}
          rowClass={job => cx("jobs-table__row--" + job.status)}
          columns={jobsTableColumns}
          renderNoResult={this.renderEmptyState()}
          pagination={pagination}
        />
        <Pagination
          pageSize={pagination.pageSize}
          current={pagination.current}
          total={jobs.length}
          onChange={this.onChangePage}
        />
      </React.Fragment>
    );
  }

  private setCurrentPageToOneIfJobsChanged(prevProps: Readonly<JobTableProps>) {
    if (
      !isEqual(
        map(prevProps.jobs.jobs, j => {
          return j.id;
        }),
        map(this.props.jobs.jobs, j => {
          return j.id;
        }),
      )
    ) {
      this.setState((prevState: Readonly<any>) => {
        return {
          pagination: {
            ...prevState.pagination,
            current: 1,
          },
        };
      });
    }
  }
}
