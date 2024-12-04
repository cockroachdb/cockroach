// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import Long from "long";
import moment from "moment-timezone";

import { RequestError } from "../util";

import {
  executeInternalSql,
  SqlExecutionRequest,
  sqlResultsAreEmpty,
} from "./sqlApi";

type ScheduleColumns = {
  id: string;
  label: string;
  schedule_status: string;
  next_run: string;
  state: string;
  recurrence: string;
  jobsrunning: number;
  owner: string;
  created: string;
  command: string;
};

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

export function getSchedules(req: {
  status: string;
  limit: number;
}): Promise<Schedules> {
  // Cast int64 to string, since otherwise it gets truncated.
  // Likewise, prettify `command` on the server since contained int64s
  // may also be truncated.
  let stmt = `
    WITH schedules AS (SHOW SCHEDULES)
    SELECT id::string, label, schedule_status, next_run,
           state, recurrence, jobsrunning, owner,
           created, jsonb_pretty(command) as command
    FROM schedules
  `;
  const args = [];
  if (req.status) {
    stmt += " WHERE schedule_status = $" + (args.length + 1);
    args.push(req.status);
  }
  stmt += " ORDER BY created DESC";
  if (req.limit) {
    stmt += " LIMIT $" + (args.length + 1);
    args.push(req.limit.toString());
  }
  const request: SqlExecutionRequest = {
    statements: [
      {
        sql: stmt,
        arguments: args,
      },
    ],
    execute: true,
  };
  return executeInternalSql<ScheduleColumns>(request).then(result => {
    const txnResults = result.execution.txn_results;
    if (sqlResultsAreEmpty(result)) {
      // No data.
      return [];
    }

    return txnResults[0].rows.map(row => {
      return {
        id: Long.fromString(row.id),
        label: row.label,
        status: row.schedule_status,
        nextRun: row.next_run ? moment.utc(row.next_run) : null,
        state: row.state,
        recurrence: row.recurrence,
        jobsRunning: row.jobsrunning,
        owner: row.owner,
        created: moment.utc(row.created),
        command: JSON.parse(row.command),
      };
    });
  });
}

export function getSchedule(id: Long): Promise<Schedule> {
  const request: SqlExecutionRequest = {
    statements: [
      {
        // Cast int64 to string, since otherwise it gets truncated.
        // Likewise, prettify `command` on the server since contained int64s
        // may also be truncated.
        sql: `
          WITH schedules AS (SHOW SCHEDULES)
          SELECT id::string, label, schedule_status, next_run,
                 state, recurrence, jobsrunning, owner,
                 created, jsonb_pretty(command) as command
          FROM schedules
          WHERE ID = $1::int64
        `,
        arguments: [id.toString()],
      },
    ],
    execute: true,
  };
  return executeInternalSql<ScheduleColumns>(request).then(result => {
    const txnResults = result.execution.txn_results;
    if (txnResults.length === 0 || !txnResults[0].rows) {
      // No data.
      throw new RequestError(400, "No schedule found with this ID.");
    }

    if (txnResults[0].rows.length > 1) {
      throw new RequestError(500, "Multiple schedules found for ID.");
    }
    const row = txnResults[0].rows[0];
    return {
      id: Long.fromString(row.id),
      label: row.label,
      status: row.schedule_status,
      nextRun: row.next_run ? moment.utc(row.next_run) : null,
      state: row.state,
      recurrence: row.recurrence,
      jobsRunning: row.jobsrunning,
      owner: row.owner,
      created: moment.utc(row.created),
      command: row.command,
    };
  });
}
