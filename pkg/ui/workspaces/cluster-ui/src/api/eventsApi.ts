// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
  executeInternalSql,
  SqlExecutionRequest,
  sqlResultsAreEmpty,
  SqlStatement,
} from "./sqlApi";
import { withTimeout } from "./util";
import moment from "moment";

// defaultEventsMaxBytesLimit is the default max size of events payload in bytes.
export const defaultEventsMaxBytesLimit = 50000; // 50KB
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
  console.log("EVENTS STMT", eventsStmt);
  return eventsStmt;
}

export function buildEventsSQLRequest(
  req: NonRedactedEventsRequest,
): SqlExecutionRequest {
  const eventsStmt: SqlStatement = buildEventStatement(req);
  return {
    statements: [eventsStmt],
    execute: true,
    max_result_size: defaultEventsMaxBytesLimit,
  };
}

// getEvents fetches events logs from the database. Callers of
// getEvents from cluster-ui will need to pass a timeout argument for
// promise timeout handling (callers from db-console already have promise
// timeout handling as part of the cacheDataReducer).
// Note that this endpoint is not able to redact event log information.
export function getNonRedactedEvents(
  req: NonRedactedEventsRequest = {},
  timeout?: moment.Duration,
): Promise<EventsResponse> {
  const eventsRequest: SqlExecutionRequest = buildEventsSQLRequest(req);
  return withTimeout(
    executeInternalSql<EventColumns>(eventsRequest),
    timeout,
  ).then(result => {
    // If request succeeded but query failed, throw error (caught by saga/cacheDataReducer).
    if (result.error) {
      throw result.error;
    }

    if (sqlResultsAreEmpty(result)) {
      return [];
    }
    return result.execution.txn_results[0].rows;
  });
}
