// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {cockroach} from "src/js/protos";
import _ from "lodash";
import Job = cockroach.server.serverpb.JobsResponse.IJob;

export const statusOptions = [
  {value: "", label: "All"},
  {value: "succeeded", label: "Succeeded"},
  {value: "failed", label: "Failed"},
  {value: "pending", label: "Pending"},
  {value: "canceled", label: "Canceled"},
  {value: "paused", label: "Paused"},
];

export function jobHasOneOfStatuses(job: Job, ...statuses: string[]) {
  return statuses.indexOf(job.status) !== -1;
}

export const renamedStatuses = (status: string) => {
  switch (status) {
    case JOB_STATUS_SUCCEEDED:
      return {
        label: JOB_STATUS_SUCCEEDED,
        value: "success",
      };
    case JOB_STATUS_FAILED:
      return {
        label: JOB_STATUS_FAILED,
        value: "danger",
      };
    case JOB_STATUS_CANCELED:
      return {
        label: JOB_STATUS_CANCELED,
        value: "default",
      };
    case JOB_STATUS_PAUSED:
      return {
        label: JOB_STATUS_PAUSED,
        value: "default",
      };
    case JOB_STATUS_RUNNING:
      return {
        label: "CDC Running",
        value: "info",
      };
    case JOB_STATUS_PENDING:
      return {
        label: JOB_STATUS_PENDING,
        value: "warning",
      };
    default:
      return {
        label: _.capitalize(status),
        value: "info",
      };
  }
};

export const JOB_STATUS_SUCCEEDED = "succeeded";
export const JOB_STATUS_FAILED = "failed";
export const JOB_STATUS_CANCELED = "canceled";
export const JOB_STATUS_PAUSED = "paused";
export const JOB_STATUS_RUNNING = "running";
export const JOB_STATUS_PENDING = "pending";
