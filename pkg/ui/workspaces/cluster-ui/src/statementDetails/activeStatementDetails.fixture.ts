// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment from "moment-timezone";
import noop from "lodash/noop";

import { ExecutionStatus } from "../activeExecutions";

import { ActiveStatementDetailsProps } from "./activeStatementDetails";

export const getActiveStatementDetailsPropsFixture =
  (): ActiveStatementDetailsProps => {
    return {
      statement: {
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
      },
      match: {
        path: "/execution/statement/:statement",
        url: "/execution/statement/17ab864032f8e1c20000000000000001",
        isExact: true,
        params: {
          statement: "17ab864032f8e1c20000000000000001",
        },
      },
      hasAdminRole: true,
      refreshLiveWorkload: noop,
    };
  };
