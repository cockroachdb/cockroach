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
import _ from "lodash";
import { cockroach } from "src/js/protos";
import { TimestampToMoment } from "src/util/convert";
import { DATE_FORMAT } from "src/util/format";
import { JobStatusCell } from "src/views/jobs/jobStatusCell";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { isEqual, map } from "lodash";
import { JobDescriptionCell } from "src/views/jobs/jobDescriptionCell";
import Job = cockroach.server.serverpb.IJobResponse;
import JobsResponse = cockroach.server.serverpb.JobsResponse;
import {
  ColumnDescriptor,
  Pagination,
  ResultsPerPageLabel,
} from "@cockroachlabs/cluster-ui";
import { jobTable } from "src/util/docs";
import { trackDocsLink } from "src/util/analytics";
import { EmptyTable } from "@cockroachlabs/cluster-ui";
import { Anchor } from "src/components";
import emptyTableResultsIcon from "assets/emptyState/empty-table-results.svg";
import magnifyingGlassIcon from "assets/emptyState/magnifying-glass.svg";
import { SortedTable } from "../shared/components/sortedtable";

class JobsSortedTable extends SortedTable<Job> {}

const jobsTableColumns: ColumnDescriptor<Job>[] = [
  {
    name: "description",
    title: "Description",
    className: "cl-table__col-query-text",
    cell: (job) => <JobDescriptionCell job={job} />,
    sort: (job) => job.statement || job.description || job.type,
  },
  {
    name: "jobId",
    title: "Job ID",
    titleAlign: "right",
    cell: (job) => String(job.id),
    sort: (job) => job.id?.toNumber(),
  },
  {
    name: "users",
    title: "Users",
    cell: (job) => job.username,
    sort: (job) => job.username,
  },
  {
    name: "creationTime",
    title: "Creation Time",
    cell: (job) => TimestampToMoment(job?.created).format(DATE_FORMAT),
    sort: (job) => TimestampToMoment(job?.created).valueOf(),
  },
  {
    name: "status",
    title: "Status",
    cell: (job) => <JobStatusCell job={job} compact />,
    sort: (job) => job.fraction_completed,
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

  renderCounts = () => {
    const {
      pagination: { current, pageSize },
    } = this.state;
    const total = this.props.jobs.data.jobs.length;
    const pageCount = current * pageSize > total ? total : current * pageSize;
    const count = total > 10 ? pageCount : current * total;
    return `${count} of ${total} jobs`;
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

  render() {
    const jobs = this.props.jobs.data.jobs;
    const { pagination } = this.state;

    return (
      <React.Fragment>
        <div className="cl-table-statistic">
          <h4 className="cl-count-title">
            <ResultsPerPageLabel
              pagination={{ ...pagination, total: jobs.length }}
              pageName="jobs"
            />
          </h4>
        </div>
        <JobsSortedTable
          data={jobs}
          sortSetting={this.props.sort}
          onChangeSortSetting={this.props.setSort}
          className="jobs-table"
          rowClass={(job) => "jobs-table__row--" + job.status}
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
        map(prevProps.jobs.data.jobs, (j) => {
          return j.id;
        }),
        map(this.props.jobs.data.jobs, (j) => {
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
