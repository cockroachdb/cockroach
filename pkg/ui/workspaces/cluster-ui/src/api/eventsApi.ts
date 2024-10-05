// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

import {
  executeInternalSql,
  LARGE_RESULT_SIZE,
  SqlExecutionRequest,
  sqlResultsAreEmpty,
  SqlStatement,
  SqlApiResponse,
  formatApiResult,
} from "./sqlApi";
import { withTimeout } from "./util";

// defaultEventsNumLimit is the default number of events to be returned.
export const defaultEventsNumLimit = 1000;

export type EventColumns = {
  timestamp: string;
  eventType: string;
  reportingID: string;
  info: string;
  uniqueID: string;
};

export type NonRedactedEventsRequest = {
  type?: string;
  limit?: number;
  offset?: number;
};

export type EventsResponse = EventColumns[];

export const baseEventsQuery = `SELECT timestamp, "eventType", "reportingID", info, "uniqueID" FROM system.eventlog`;

function buildEventStatement({
  type,
  limit,
  offset,
}: NonRedactedEventsRequest): SqlStatement {
  let placeholder = 1;
  const eventsStmt: SqlStatement = {
    sql: baseEventsQuery,
    arguments: [],
  };
  if (type) {
    eventsStmt.sql += ` WHERE "eventType" = ` + type;
    eventsStmt.arguments.push(type);
  }
  eventsStmt.sql += ` ORDER BY timestamp DESC`;
  if (!limit || limit <= 0) {
    limit = defaultEventsNumLimit;
  }
  if (limit > 0) {
    eventsStmt.sql += ` LIMIT $${placeholder}`;
    eventsStmt.arguments.push(limit);
    placeholder++;
  }
  if (offset && offset > 0) {
    eventsStmt.sql += ` OFFSET $${placeholder}`;
    eventsStmt.arguments.push(offset);
  }
  eventsStmt.sql += ";";
  return eventsStmt;
}

export function buildEventsSQLRequest(
  req: NonRedactedEventsRequest,
): SqlExecutionRequest {
  const eventsStmt: SqlStatement = buildEventStatement(req);
  return {
    statements: [eventsStmt],
    execute: true,
    max_result_size: LARGE_RESULT_SIZE,
  };
}

// getNonRedactedEvents fetches events logs from the database. Callers of
// getNonRedactedEvents from cluster-ui will need to pass a timeout argument for
// promise timeout handling (callers from db-console already have promise
// timeout handling as part of the cacheDataReducer).
// Note that this endpoint is not able to redact event log information.
export function getNonRedactedEvents(
  req: NonRedactedEventsRequest = {},
  timeout?: moment.Duration,
): Promise<SqlApiResponse<EventsResponse>> {
  const eventsRequest: SqlExecutionRequest = buildEventsSQLRequest(req);
  return withTimeout(
    executeInternalSql<EventColumns>(eventsRequest),
    timeout,
  ).then(result => {
    if (sqlResultsAreEmpty(result)) {
      return formatApiResult<EventColumns[]>(
        [],
        result.error,
        "retrieving events information",
      );
    }

    return formatApiResult<EventColumns[]>(
      result.execution.txn_results[0].rows,
      result.error,
      "retrieving events information",
    );
  });
}
