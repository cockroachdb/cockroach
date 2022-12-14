// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { RequestError } from "../util";
import moment from "moment";
import { createMemoryHistory } from "history";
import Long from "long";
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { TimeScale } from "../timeScaleDropdown";
import { StatementsResponse } from "src/store/sqlStats/sqlStats.reducer";
import {ContentionEventsResponse} from "../api/txnContentionApi";
import {JobsPageProps} from "../jobs";
import {earliestRetainedTime} from "../jobs/jobsPage/jobsPage.fixture";
import {ContentionDebugPageProps} from "./contentionDebugPage";

const timestamp = new protos.google.protobuf.Timestamp({
  seconds: new Long(Date.parse("Nov 26 2021 01:00:00 GMT") * 1e-3),
});

const error = new RequestError(
  "Forbidden",
  403,
  "this operation requires admin privilege",
);

const duration = "200"


const contentionEventsResponse: ContentionEventsResponse = [
    {
      blockingTxnExecutionID: "9b7d1e63-840b-481b-a9c0-e7ef6ca7b783",
      waitingTxnExecutionID: "c66f7c81-290d-42f5-b689-db893a70379b",
      collectedAt: String(timestamp),
      waitingTxnFingerprintID: "0000000000000000",
      contentionDuration: duration,
    },
    {
      blockingTxnExecutionID: "b45364c7-8cbe-4a35-b2a1-dcbdfc2f4f5c",
      waitingTxnExecutionID: "669bf5c3-23a9-4aba-a71b-0cef75221488",
      collectedAt: String(timestamp),
      waitingTxnFingerprintID: "0000000000000000" ,
      contentionDuration: duration,
    },
    {
      blockingTxnExecutionID: "9b7d1e63-840b-481b-a9c0-e7ef6ca7b783",
      waitingTxnExecutionID: "c66f7c81-290d-42f5-b689-db893a70379b",
      collectedAt: String(timestamp),
      waitingTxnFingerprintID: "0000000000000000" ,
      contentionDuration: duration,
    },
    {
      blockingTxnExecutionID: "b45364c7-8cbe-4a35-b2a1-dcbdfc2f4f5c",
      waitingTxnExecutionID: "669bf5c3-23a9-4aba-a71b-0cef75221488",
      collectedAt: String(timestamp),
      waitingTxnFingerprintID: "0000000000000000" ,
      contentionDuration: duration,
    },
    {
      blockingTxnExecutionID: "9b7d1e63-840b-481b-a9c0-e7ef6ca7b783",
      waitingTxnExecutionID: "c66f7c81-290d-42f5-b689-db893a70379b",
      collectedAt: String(timestamp),
      waitingTxnFingerprintID: "0000000000000000" ,
      contentionDuration: duration,
    }
  ];

export const contentionDebugPageTestProps: ContentionDebugPageProps = {
  contentionEvents: contentionEventsResponse,
  contentionError: error,
  refreshTxnContentionEvents: () => null,
};

