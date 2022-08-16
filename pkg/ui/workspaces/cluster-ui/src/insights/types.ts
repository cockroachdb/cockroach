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
import { InsightType } from "../insightsTable/insightsTable";

export enum InsightNameEnum {
  highWaitTime = "highWaitTime",
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

export type Insight = {
  name: InsightNameEnum;
  label: string;
  description: string;
};

const highWaitTimeInsight = (
  execType: InsightExecEnum = InsightExecEnum.TRANSACTION,
): Insight => {
  const threshold = HIGH_WAIT_CONTENTION_THRESHOLD.asMilliseconds();
  return {
    name: InsightNameEnum.highWaitTime,
    label: "High Wait Time",
    description:
      `This ${execType} has been waiting for more than ${threshold}ms on other ${execType}s to execute. ` +
      `Click the ${execType} execution ID to see more details.`,
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

export type SchemaInsightEventFilters = Omit<
  Filters,
  | "app"
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

export const schemaInsightTypeToDisplayMapping = {
  CREATE_INDEX: "Create New Index",
  DROP_INDEX: "Drop Unused Index",
  REPLACE_INDEX: "Replace Index",
};

export function schemaInsightDisplayToType(display: string): string {
  return Object.keys(schemaInsightTypeToDisplayMapping).find(
    (type: InsightType) => schemaInsightTypeToDisplayMapping[type] === display,
  );
}
