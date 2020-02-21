import {cockroach} from "src/js/protos";
import _ from "lodash";
import Job = cockroach.server.serverpb.JobsResponse.IJob;

export const statusOptions = [
  {value: "", label: "All"},
  {value: "pending", label: "Pending"},
  {value: "running", label: "Running"},
  {value: "paused", label: "Paused"},
  {value: "canceled", label: "Canceled"},
  {value: "succeeded", label: "Succeeded"},
  {value: "failed", label: "Failed"},
];

export function jobHasOneOfStatuses(job: Job, ...statuses: string[]) {
  return statuses.indexOf(job.status) !== -1;
}

export const renamedStatuses = (status: string) => {
  switch (status) {
    case JOB_STATUS_SUCCEEDED:
      return "Completed";
    case JOB_STATUS_FAILED:
      return "Import failed";
    case JOB_STATUS_CANCELED:
      return "Import failed";
    default:
      return _.capitalize(status);
  }
};

export const JOB_STATUS_SUCCEEDED = "succeeded";
export const JOB_STATUS_FAILED = "failed";
export const JOB_STATUS_CANCELED = "canceled";
export const JOB_STATUS_PAUSED = "paused";
export const JOB_STATUS_RUNNING = "running";
export const JOB_STATUS_PENDING = "pending";
