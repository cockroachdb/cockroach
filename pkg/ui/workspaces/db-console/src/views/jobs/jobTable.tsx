// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { MouseEvent } from "react";
import { cockroach } from "src/js/protos";
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { DATE_FORMAT_24_UTC } from "src/util/format";
import { JobStatusCell } from "src/views/jobs/jobStatusCell";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { isEqual, map } from "lodash";
import { JobDescriptionCell } from "src/views/jobs/jobDescriptionCell";
import Job = cockroach.server.serverpb.IJobResponse;
import JobsResponse = cockroach.server.serverpb.JobsResponse;
import {
  ColumnDescriptor,
  EmptyTable,
  Pagination,
  ResultsPerPageLabel,
  SortSetting,
  SortedTable,
  util,
} from "@cockroachlabs/cluster-ui";
import {
  jobsCancel,
  jobsPause,
  jobsResume,
  jobStatus,
  jobTable,
} from "src/util/docs";
import { trackDocsLink } from "src/util/analytics";
import { Anchor } from "src/components";
import emptyTableResultsIcon from "assets/emptyState/empty-table-results.svg";
import magnifyingGlassIcon from "assets/emptyState/magnifying-glass.svg";
import { Tooltip } from "@cockroachlabs/ui-components";

class JobsSortedTable extends SortedTable<Job> {}

const jobsTableColumns: ColumnDescriptor<Job>[] = [
  {
    name: "description",
    title: (
      <Tooltip
        placement="bottom"
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
    className: "cl-table__col-query-text",
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
            <Anchor href={jobsPause} target="_blank">
              pause
            </Anchor>
            {", "}
            <Anchor href={jobsResume} target="_blank">
              resume
            </Anchor>
            {", or "}
            <Anchor href={jobsCancel} target="_blank">
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
        {"User"}
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
    cell: job =>
      util.TimestampToMoment(job?.created).format(DATE_FORMAT_24_UTC),
    sort: job => util.TimestampToMoment(job?.created).valueOf(),
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
    cell: job =>
      util.TimestampToMoment(job?.modified).format(DATE_FORMAT_24_UTC),
    sort: job => util.TimestampToMoment(job?.modified).valueOf(),
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
    cell: job =>
      util.TimestampToMoment(job?.last_run).format(DATE_FORMAT_24_UTC),
    sort: job => util.TimestampToMoment(job?.last_run).valueOf(),
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
  jobs: CachedDataReducerState<JobsResponse>;
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

  onChangePage = (current: number) => {
    const { pagination } = this.state;
    this.setState({ pagination: { ...pagination, current } });
  };

  renderEmptyState = () => {
    const { isUsedFilter, jobs } = this.props;
    const hasData = jobs?.data?.jobs?.length > 0;

    if (hasData) {
      return null;
    }

    if (isUsedFilter) {
      return (
        <EmptyTable
          title="No jobs match your search"
          icon={magnifyingGlassIcon}
          footer={
            <Anchor
              href={jobTable}
              target="_blank"
              onClick={this.redirectToLearnMore}
            >
              Learn more about jobs
            </Anchor>
          }
        />
      );
    } else {
      return (
        <EmptyTable
          title="No jobs to show"
          icon={emptyTableResultsIcon}
          message="The jobs page provides details about backup/restore jobs, schema changes, user-created table statistics, automatic table statistics jobs and changefeeds."
          footer={
            <Anchor
              href={jobTable}
              target="_blank"
              onClick={this.redirectToLearnMore}
            >
              Learn more about jobs
            </Anchor>
          }
        />
      );
    }
  };

  redirectToLearnMore = (e: MouseEvent<HTMLAnchorElement>) => {
    trackDocsLink(e.currentTarget.text);
  };

  formatJobsRetentionMessage = (
    earliestRetainedTime: protos.google.protobuf.ITimestamp,
  ): string => {
    return `Since ${util
      .TimestampToMoment(earliestRetainedTime)
      .format(DATE_FORMAT_24_UTC)}`;
  };

  render() {
    const jobs = this.props.jobs.data.jobs;
    const { pagination } = this.state;

    return (
      <React.Fragment>
        <div className="cl-table-statistic jobs-table-summary">
          <h4 className="cl-count-title">
            <ResultsPerPageLabel
              pagination={{ ...pagination, total: jobs.length }}
              pageName="jobs"
            />
            {this.props.jobs.data.earliest_retained_time && (
              <>
                <span className="jobs-table-summary__retention-divider">|</span>
                {this.formatJobsRetentionMessage(
                  this.props.jobs.data.earliest_retained_time,
                )}
              </>
            )}
          </h4>
        </div>
        <JobsSortedTable
          data={jobs}
          sortSetting={this.props.sort}
          onChangeSortSetting={this.props.setSort}
          className="jobs-table"
          rowClass={job => "jobs-table__row--" + job.status}
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
        map(prevProps.jobs.data.jobs, j => {
          return j.id;
        }),
        map(this.props.jobs.data.jobs, j => {
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
