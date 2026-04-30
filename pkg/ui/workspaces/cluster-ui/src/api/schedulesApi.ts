// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import Long from "long";
import moment from "moment-timezone";

import { fetchDataJSON } from "./fetchData";

// BFF response types (private, match the Go BFF handler structs).
interface BffScheduleInfo {
  id: string;
  label: string;
  status: string;
  next_run: string;
  state: string;
  recurrence: string;
  jobs_running: number;
  owner: string;
  created: string;
  command: string;
}

interface BffSchedulesListResponse {
  schedules: BffScheduleInfo[];
}

export type Schedule = {
  id: Long;
  label: string;
  status: string;
  nextRun?: moment.Moment;
  state: string;
  recurrence: string;
  jobsRunning: number;
  owner: string;
  created: moment.Moment;
  command: string;
};

export type Schedules = Schedule[];

function prettyJson(s: string): string {
  try {
    return JSON.stringify(JSON.parse(s), null, 2);
  } catch {
    return s;
  }
}

function bffScheduleToSchedule(bff: BffScheduleInfo): Schedule {
  return {
    id: Long.fromString(bff.id),
    label: bff.label,
    status: bff.status,
    nextRun: bff.next_run ? moment.utc(bff.next_run) : null,
    state: bff.state,
    recurrence: bff.recurrence,
    jobsRunning: bff.jobs_running,
    owner: bff.owner,
    created: moment.utc(bff.created),
    command: bff.command ? prettyJson(bff.command) : "",
  };
}

export function getSchedules(req: {
  status: string;
  limit: number;
}): Promise<Schedules> {
  const params = new URLSearchParams();
  if (req.status) {
    params.set("status", req.status);
  }
  if (req.limit) {
    params.set("limit", req.limit.toString());
  }
  const queryStr = params.toString();
  const path = queryStr
    ? `api/v2/dbconsole/schedules/?${queryStr}`
    : "api/v2/dbconsole/schedules/";
  return fetchDataJSON<BffSchedulesListResponse, void>(path).then(resp => {
    if (!resp.schedules || resp.schedules.length === 0) {
      return [];
    }
    return resp.schedules.map(bffScheduleToSchedule);
  });
}

export function getSchedule(id: Long): Promise<Schedule> {
  return fetchDataJSON<BffScheduleInfo, void>(
    `api/v2/dbconsole/schedules/${id.toString()}/`,
  ).then(bffScheduleToSchedule);
}
