// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { Moment } from "moment";
import { Filters } from "src/queryFilter";

export type SessionsResponse = protos.cockroach.server.serverpb.ListSessionsResponse;
export type ActiveStatementResponse = protos.cockroach.server.serverpb.ActiveQuery;
export type ExecutionStatus = "Waiting" | "Executing" | "Preparing";
export const ActiveStatementPhase =
  protos.cockroach.server.serverpb.ActiveQuery.Phase;

export type ActiveStatement = {
  executionID: string;
  transactionID: string;
  sessionID: string;
  query: string;
  status: ExecutionStatus;
  start: Moment;
  elapsedTimeSeconds: number;
  application: string;
  user: string;
  clientAddress: string;
};

export type ActiveStatementFilters = Omit<
  Filters,
  "database" | "sqlType" | "fullScan" | "distributed" | "regions" | "nodes"
>;

export type ActiveTransactionFilters = ActiveStatementFilters;

export type ActiveTransaction = {
  executionID: string;
  sessionID: string;
  mostRecentStatement: ActiveStatement | null;
  status: ExecutionStatus;
  start: Moment;
  elapsedTimeSeconds: number;
  statementCount: number;
  retries: number;
  application: string;
};
