// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

import { ExecutionStatus } from "../activeExecutions";
import { UseLiveWorkloadResult } from "../api/liveWorkloadApi";

export const getLiveWorkloadFixture = (): UseLiveWorkloadResult => ({
  data: {
    statements: [
      {
        statementID: "17ab864032f8e1c20000000000000001",
        stmtNoConstants: "SELECT count(*) FROM foo",
        transactionID: "fac8885a-f40a-4666-b746-a45061faad74",
        sessionID: "123456789",
        status: ExecutionStatus.Executing,
        start: moment.utc("2021-12-12"),
        elapsedTime: moment.duration("3s"),
        application: "my-app",
        database: "defaultdb",
        query: "SELECT count(*) FROM foo",
        timeSpentWaiting: moment.duration("1s"),
        user: "andy",
        clientAddress: "127.0.0.1",
        isFullScan: true,
        planGist: "AgICABoCBQQf0AEB",
        isolationLevel: "SERIALIZABLE",
      },
    ],
    transactions: [
      {
        transactionID: "fac8885a-f40a-4666-b746-a45061faad74",
        sessionID: "123456789",
        status: ExecutionStatus.Executing,
        start: moment.utc("2021-12-12"),
        elapsedTime: moment.duration("3s"),
        application: "my-app",
        statementCount: 1,
        retries: 0,
        priority: "normal",
        statementID: "17ab864032f8e1c20000000000000001",
        stmtNoConstants: "SELECT count(*) FROM foo",
        query: "SELECT count(*) FROM foo",
        isolationLevel: "SERIALIZABLE",
      },
    ],
    clusterLocks: null,
    internalAppNamePrefix: "$ internal",
    maxSizeApiReached: false,
  },
  isLoading: false,
  error: null,
  lastUpdated: moment.utc(),
  refresh: jest.fn(),
});
