// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { Tooltip } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
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
import { DATE_WITH_SECONDS_FORMAT } from "src/util/format";

import { Timestamp, Timezone } from "../../timestamp";
import styles from "../jobs.module.scss";
import { HighwaterTimestamp, JobStatusCell } from "../util";

import { JobDescriptionCell } from "./jobDescriptionCell";
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

export const jobsColumnLabels: { [key: string]: string } = {
  description: "Description",
  status: "Status",
  jobId: "Job ID",
  users: "User Name",
  creationTime: "Creation Time",
  lastModifiedTime: "Last Modified Time",
  finishedTime: "Completed Time",
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
          <>
            {jobsColumnLabels.creationTime} <Timezone />
          </>
        </Tooltip>
      ),
      cell: job => (
        <Timestamp
          time={TimestampToMoment(job?.created, null)}
          format={DATE_WITH_SECONDS_FORMAT}
        />
      ),
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
          <>
            {jobsColumnLabels.lastModifiedTime} <Timezone />
          </>
        </Tooltip>
      ),
      cell: job => (
        <Timestamp
          time={TimestampToMoment(job?.modified, null)}
          format={DATE_WITH_SECONDS_FORMAT}
        />
      ),
      sort: job => TimestampToMoment(job?.modified).valueOf(),
      showByDefault: true,
    },
    {
      name: "finishedTime",
      title: (
        <Tooltip
          placement="bottom"
          style="tableTitle"
          content={
            <p>
              Date and time the job was either completed, failed or canceled.
            </p>
          }
        >
          <>
            {jobsColumnLabels.finishedTime} <Timezone />
          </>
        </Tooltip>
      ),
      cell: job => (
        <Timestamp
          time={TimestampToMoment(job?.finished, null)}
          format={DATE_WITH_SECONDS_FORMAT}
        />
      ),
      sort: job => TimestampToMoment(job?.finished).valueOf(),
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
      tableWrapperClassName={cx("sorted-table")}
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
