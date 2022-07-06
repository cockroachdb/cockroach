// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Moment } from "moment";
import { HIGH_WAIT_CONTENTION_THRESHOLD } from "../api";
import { Filters } from "../queryFilter";

export enum InsightNameEnum {
  highWaitTime = "HIGH_WAIT_TIME",
}

export enum InsightExecEnum {
  TRANSACTION = "transaction",
  STATEMENT = "statement",
}

export type InsightEvent = {
  executionID: string;
  queries: string[];
  insights: Insight[];
  startTime: Moment;
  elapsedTime: number;
  application: string;
  execType: InsightExecEnum;
};

export type InsightEventDetails = {
  executionID: string;
  queries: string[];
  insights: Insight[];
  startTime: Moment;
  elapsedTime: number;
  application: string;
  fingerprintID: string;
  waitingExecutionID: string;
  waitingFingerprintID: string;
  waitingQueries: string[];
  contendedKey: string;
  schemaName: string;
  databaseName: string;
  tableName: string;
  indexName: string;
  execType: InsightExecEnum;
};

export type Insight = {
  name: InsightNameEnum;
  label: string;
  description: string;
  tooltipDescription: string;
};

export type EventExecution = {
  executionID: string;
  fingerprintID: string;
  queries: string[];
  startTime: Moment;
  elapsedTime: number;
  execType: InsightExecEnum;
};

const highWaitTimeInsight = (
  execType: InsightExecEnum = InsightExecEnum.TRANSACTION,
): Insight => {
  const threshold = HIGH_WAIT_CONTENTION_THRESHOLD.asMilliseconds();
  const description = `This ${execType} has been waiting for more than ${threshold}ms on other ${execType}s to execute.`;
  return {
    name: InsightNameEnum.highWaitTime,
    label: "High Wait Time",
    description: description,
    tooltipDescription:
      description + ` Click the ${execType} execution ID to see more details.`,
  };
};

export const InsightTypes = [highWaitTimeInsight];

export const InsightExecOptions = [
  {
    value: InsightExecEnum.TRANSACTION.toString(),
    label: "Transaction Executions",
  },
  {
    value: InsightExecEnum.STATEMENT.toString(),
    label: "Statement Executions",
  },
];

export type InsightEventFilters = Omit<
  Filters,
  | "database"
  | "sqlType"
  | "fullScan"
  | "distributed"
  | "regions"
  | "nodes"
  | "username"
  | "sessionStatus"
  | "timeNumber"
  | "timeUnit"
>;
