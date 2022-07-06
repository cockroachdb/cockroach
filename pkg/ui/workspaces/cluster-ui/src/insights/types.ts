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
import { TransactionInsightState } from "../api";

type InsightEvent<InsightExecEnum> = {
  executionID: string;
  query: string;
  insights: Insight[];
  startTime: Moment;
  elapsedTime: number;
  application: string;
  execType: InsightExecEnum;
};

export type Insight = {
  label: string;
  description: string;
};

export enum InsightExecEnum {
  TRANSACTION = "transaction",
  STATEMENT = "statement",
}

enum InsightEnum {
  HIGH_WAIT_TIME = "High Wait Time",
}

export type TransactionInsight = InsightEvent<InsightExecEnum.TRANSACTION>;

const HIGH_WAIT_CONTENTION_THRESHOLD = 2;

const isHighWait = (
  insightState: TransactionInsightState,
  threshold: number = HIGH_WAIT_CONTENTION_THRESHOLD,
): boolean => {
  return insightState.elapsedTime > threshold;
};

const highWaitTimeInsight = (
  execType: InsightExecEnum = InsightExecEnum.TRANSACTION,
  minWait: number = HIGH_WAIT_CONTENTION_THRESHOLD,
): Insight => ({
  label: InsightEnum.HIGH_WAIT_TIME,
  description:
    `This ${execType} has been waiting for more than ${minWait}ms on other ${execType}s to execute. ` +
    `Click the ${execType} execution ID to see more details.`,
});

export const InsightTypes = [
  { name: "highWaitTime", insight: highWaitTimeInsight, check: isHighWait },
];
