import {cockroach} from "src/js/protos";
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
