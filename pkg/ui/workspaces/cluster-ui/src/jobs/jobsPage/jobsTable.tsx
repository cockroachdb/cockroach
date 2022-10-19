// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { Tooltip } from "@cockroachlabs/ui-components";
import React from "react";
import { Anchor } from "src/anchor";
import { EmptyTable } from "src/empty";
import {
  ColumnDescriptor,
  SortSetting,
  SortedTable,
  ISortedTablePagination,
} from "src/sortedtable";
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

type Job = cockroach.server.serverpb.IJobResponse;
type Jobs = Job[];

interface JobsTableProps {
  jobs: Jobs;
  sortSetting: SortSetting;
  onChangeSortSetting: (value: SortSetting) => void;
  pagination: ISortedTablePagination;
  renderNoResult?: React.ReactNode;
  visibleColumns: ColumnDescriptor<Job>[];
}

export const jobsColumnLabels: any = {
  description: "Description",
  status: "Status",
  jobId: "Job ID",
  users: "User Name",
  creationTime: "Creation Time (UTC)",
  lastModifiedTime: "Last Modified Time (UTC)",
  lastExecutionTime: "Last Execution Time (UTC)",
  executionCount: "Execution Count",
  highWaterTimestamp: "High-water Timestamp",
  coordinatorID: "Coordinator Node",
};

export function makeJobsColumns(): ColumnDescriptor<Job>[] {
  return [
    {
      name: "description",
      title: (
        <Tooltip
          placement="bottom"
          style="tableTitle"
          content={
            <p>
              The description of the job, if set, or the SQL statement if there
              is no job description.
            </p>
          }
        >
          {jobsColumnLabels.description}
        </Tooltip>
      ),
      className: cx("cl-table__col-query-text"),
      cell: job => <JobDescriptionCell job={job} />,
      sort: job => job.statement || job.description || job.type,
      alwaysShow: true,
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
          {jobsColumnLabels.status}
        </Tooltip>
      ),
      cell: job => <JobStatusCell job={job} compact />,
      sort: job => job.fraction_completed,
      alwaysShow: true,
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
          {jobsColumnLabels.jobId}
        </Tooltip>
      ),
      titleAlign: "right",
      cell: job => String(job.id),
      sort: job => job.id?.toNumber(),
      alwaysShow: true,
    },
    {
      name: "users",
      title: (
        <Tooltip
          placement="bottom"
          style="tableTitle"
          content={<p>User that created the job.</p>}
        >
          {jobsColumnLabels.users}
        </Tooltip>
      ),
      cell: job => job.username,
      sort: job => job.username,
      showByDefault: true,
    },
    {
      name: "creationTime",
      title: (
        <Tooltip
          placement="bottom"
          style="tableTitle"
          content={<p>Date and time the job was created.</p>}
        >
          {jobsColumnLabels.creationTime}
        </Tooltip>
      ),
      cell: job => TimestampToMoment(job?.created).format(DATE_FORMAT_24_UTC),
      sort: job => TimestampToMoment(job?.created).valueOf(),
      showByDefault: true,
    },
    {
      name: "lastModifiedTime",
      title: (
        <Tooltip
          placement="bottom"
          style="tableTitle"
          content={<p>Date and time the job was last modified.</p>}
        >
          {jobsColumnLabels.lastModifiedTime}
        </Tooltip>
      ),
      cell: job => TimestampToMoment(job?.modified).format(DATE_FORMAT_24_UTC),
      sort: job => TimestampToMoment(job?.modified).valueOf(),
      showByDefault: true,
    },
    {
      name: "lastExecutionTime",
      title: (
        <Tooltip
          placement="bottom"
          style="tableTitle"
          content={<p>Date and time the job was last executed.</p>}
        >
          {jobsColumnLabels.lastModifiedTime}
        </Tooltip>
      ),
      cell: job => TimestampToMoment(job?.last_run).format(DATE_FORMAT_24_UTC),
      sort: job => TimestampToMoment(job?.last_run).valueOf(),
      showByDefault: true,
    },
    {
      name: "executionCount",
      title: (
        <Tooltip
          placement="bottom"
          style="tableTitle"
          content={<p>Number of times the job was executed.</p>}
        >
          {jobsColumnLabels.executionCount}
        </Tooltip>
      ),
      cell: job => String(job.num_runs),
      sort: job => job.num_runs?.toNumber(),
      showByDefault: true,
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
          {jobsColumnLabels.highWaterTimestamp}
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
      showByDefault: false,
    },
    {
      name: "coordinatorID",
      title: (
        <Tooltip
          placement="bottom"
          style="tableTitle"
          content={<p>ID of the coordinating node.</p>}
        >
          {jobsColumnLabels.coordinatorID}
        </Tooltip>
      ),
      // If the coordinator ID is unset, we don't want to display anything, but
      // the default value of `0` is fine for sorting.
      cell: job =>
        Object.prototype.hasOwnProperty.call(job, "coordinator_id")
          ? String(job.coordinator_id)
          : "",
      sort: job => job.coordinator_id?.toNumber(),
      showByDefault: false,
    },
  ];
}

export const JobsTable: React.FC<JobsTableProps> = props => {
  return (
    <SortedTable
      data={props.jobs}
      columns={props.visibleColumns}
      className={cx("jobs-table")}
      rowClass={job => cx("jobs-table__row--" + job.status)}
      renderNoResult={
        <EmptyTable
          title="No jobs found."
          message="The jobs page provides details about backup/restore jobs, schema changes, user-created table statistics, automatic table statistics jobs and changefeeds."
          footer={
            <Anchor href={jobTable} target="_blank">
              Learn more about jobs
            </Anchor>
          }
        />
      }
      {...props}
    />
  );
};

JobsTable.defaultProps = {};
