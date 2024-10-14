// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createMemoryHistory } from "history";
import noop from "lodash/noop";
import moment from "moment-timezone";

import {
  InsightEventBase,
  InsightExecEnum,
  InsightNameEnum,
  StatementStatus,
  StmtInsightEvent,
} from "../types";

import { StatementInsightDetailsProps } from "./statementInsightDetails";

const insightEventBaseFixture: InsightEventBase = {
  application: "app",
  cpuSQLNanos: 25000000000,
  elapsedTimeMillis: 127.0,
  endTime: moment.utc("2021-12-10T05:06:07"),
  implicitTxn: true,
  insights: [
    {
      name: InsightNameEnum.SLOW_EXECUTION,
      label: "label",
      description: "slow query",
      tooltipDescription: "a really slow query",
    },
    {
      name: InsightNameEnum.SUBOPTIMAL_PLAN,
      label: "label2",
      description: "bad plan",
      tooltipDescription: "a really bad plan",
    },
  ],
  priority: "high",
  query: "SELECT count(*) FROM foo",
  retries: 0,
  rowsRead: 64500,
  rowsWritten: 0,
  sessionID: "123456789",
  startTime: moment.utc("2021-12-08T15:19:42"),
  transactionExecutionID: "87123cf6-13ea-40a8-9257-48b00b2af4cc",
  transactionFingerprintID: "8a239b0afd5ebd3a",
  username: "bob",
  errorCode: "",
  errorMsg: "",
};

const insightEventFixture: StmtInsightEvent = Object.assign(
  insightEventBaseFixture,
  {
    statementExecutionID: "17a8d80bd38900b80000000000000005",
    statementFingerprintID: "254026467b5f0ae5",
    isFullScan: true,
    indexRecommendations: [],
    planGist: "AgGA////nxkAAAYAAAADBQIGAg==",
    databaseName: "defaultdb",
    execType: InsightExecEnum.STATEMENT,
    status: StatementStatus.COMPLETED,
  },
);

export const getStatementInsightPropsFixture =
  (): StatementInsightDetailsProps => {
    const history = createMemoryHistory({ initialEntries: ["/insights"] });
    return {
      history,
      location: {
        pathname: "/insights",
        search: "",
        hash: "",
        state: null,
      },
      match: {
        path: "/insights",
        url: "/insights",
        isExact: true,
        params: {},
      },
      timeScale: {
        windowSize: moment.duration(5, "day"),
        sampleSize: moment.duration(5, "minutes"),
        fixedWindowEnd: moment.utc("2021-12-12"),
        key: "Custom",
      },
      insightEventDetails: insightEventFixture,
      insightError: null,
      hasAdminRole: true,
      setTimeScale: noop,
      refreshUserSQLRoles: noop,
    };
  };
