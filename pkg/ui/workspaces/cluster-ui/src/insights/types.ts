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
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

export type IExtendedContentionEvent =
  cockroach.sql.contentionpb.IExtendedContentionEvent;

export type InsightTransaction = {
  executionID: string;
  query: string;
  insights: Insight[] | null;
  startTime: Moment;
  elapsedTime: number;
  application: string;
};

export interface Insight {
  label: string;
  description: string;
  execType?: InsightExecType;
}

export enum InsightExecType {
  TRANSACTION = "transaction",
  STATEMENT = "statement",
}

const highWaitTimeType = (execType: InsightExecType): Insight => ({
  label: "High Wait Time",
  description:
    `This ${execType} has been waiting for more than 200ms on other ${execType}s to execute. ` +
    `Click the ${execType} execution ID to see more details.`,
});

export const InsightTypes = (
  execType: InsightExecType = InsightExecType.TRANSACTION,
): Insight[] => [highWaitTimeType(execType)];
