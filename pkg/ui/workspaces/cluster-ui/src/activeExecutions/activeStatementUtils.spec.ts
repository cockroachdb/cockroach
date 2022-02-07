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
  ActiveStatementPhase,
  SessionsResponse,
  ActiveTransaction,
} from "./types";
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { ActiveStatement } from "src";
import moment from "moment";
import {
  activeStatementsFromSessions,
  filterActiveStatements,
} from "./activeStatementUtils";
import { TimestampToMoment } from "../util/convert";
import Long from "long";
import {
  appsFromActiveTransactions,
  activeTransactionsFromSessions,
  appsFromActiveStatements,
} from "./activeStatementUtils";

type ActiveQuery = protos.cockroach.server.serverpb.ActiveQuery;
const Timestamp = protos.google.protobuf.Timestamp;

const defaultActiveStatement: ActiveStatement = {
  executionID: "yay",
  transactionID: "transactionID",
  sessionID: "sessionID",
  query: "SELECT 1",
  status: "Executing",
  start: moment(),
  elapsedTimeSeconds: 10,
  application: "test",
  user: "user",
  clientAddress: "clientAddress",
};

// makeActiveStatement creates an ActiveStatement object with the default active statement above
// used as the base.
function makeActiveStatement(
  statementProperties: Partial<ActiveStatement> = {},
): ActiveStatement {
  return {
    ...defaultActiveStatement,
    ...statementProperties,
  };
}

// makeActiveTxn creates an ActiveTransaction object with the provided props.
function makeActiveTxn(
  props: Partial<ActiveTransaction> = {},
): ActiveTransaction {
  return {
    executionID: "txn",
    sessionID: "sessionID",
    start: moment(),
    elapsedTimeSeconds: 10,
    application: "application",
    mostRecentStatement: defaultActiveStatement,
    retries: 3,
    statementCount: 5,
    status: "Executing",
    ...props,
  };
}

const defaultActiveQuery = {
  sql: "SELECT 1",
  sql_no_constants: "SELECT _",
  id: "queryId",
  txn_id: new Uint8Array(),
  phase: ActiveStatementPhase.EXECUTING,
  start: new Timestamp({
    seconds: Long.fromNumber(
      Math.round(new Date("2022-01-01").getTime() / 1000),
    ),
  }),
};

// makeActiveQuery creates an ActiveQuery object (which is part of the ListSessionsResponse), with
// the default active query above used as the base.
function makeActiveQuery(
  props: Partial<ActiveQuery> = {},
): Partial<ActiveQuery> {
  return {
    ...defaultActiveQuery,
    ...props,
  };
}

describe("test activeStatementUtils", () => {
  describe("filterActiveStatements", () => {
    it("should filter out statements that do not match filters", () => {
      const statements: ActiveStatement[] = [
        makeActiveStatement({ executionID: "1", application: "app1" }),
        makeActiveStatement({ executionID: "2", application: "app2" }),
        makeActiveStatement({ executionID: "3", application: "app3" }),
        makeActiveStatement({ executionID: "4", application: "app1" }),
      ];

      const filters = { app: "app1" };
      const filtered = filterActiveStatements(statements, filters);

      expect(filtered.length).toBe(2);
      expect(filtered[0].executionID).toBe("1");
      expect(filtered[1].executionID).toBe("4");
    });

    it("should filter out statements that do not match search query", () => {
      const statements: ActiveStatement[] = [
        makeActiveStatement({
          executionID: "1",
          application: "app1",
          query: "SELECT 1",
        }),
        makeActiveStatement({
          executionID: "2",
          application: "app1",
          query: "SELECT 1",
        }),
        makeActiveStatement({
          executionID: "3",
          application: "app1",
          query: "SELECT 2",
        }),
        makeActiveStatement({
          executionID: "4",
          application: "app1",
          query: "SELECT 3",
        }),
      ];

      const filters = { app: "app1" };
      const search = "SELECT 1";
      const filtered = filterActiveStatements(statements, filters, search);

      expect(filtered.length).toBe(2);
      expect(filtered[0].executionID).toBe("1");
      expect(filtered[1].executionID).toBe("2");
    });

    it("should return all statements on empty filters and search", () => {
      const statements: ActiveStatement[] = [
        makeActiveStatement(),
        makeActiveStatement(),
        makeActiveStatement(),
        makeActiveStatement(),
        makeActiveStatement(),
      ];

      const filters = { app: "" };
      const filtered = filterActiveStatements(statements, filters, "");

      expect(filtered.length).toBe(statements.length);
    });
  });

  describe("activeStatementsFromSessions", () => {
    const activeQueries = [1, 2, 3, 4].map(num =>
      makeActiveQuery({ id: num.toString() }),
    );

    const sessionsResponse: SessionsResponse = {
      sessions: [
        {
          id: new Uint8Array(),
          username: "bar",
          application_name: "application",
          client_address: "clientAddress",
          active_queries: activeQueries,
        },
        {
          id: new Uint8Array(),
          username: "foo",
          application_name: "application2",
          client_address: "clientAddress2",
          active_queries: activeQueries,
        },
      ],
      errors: [],
      internal_app_name_prefix: "",
      toJSON: () => ({}),
    };

    const statements = activeStatementsFromSessions(sessionsResponse);

    expect(statements.length).toBe(activeQueries.length * 2);

    statements.forEach(stmt => {
      if (stmt.user === "bar") {
        expect(stmt.application).toBe("application");
        expect(stmt.clientAddress).toBe("clientAddress");
      } else if (stmt.user === "foo") {
        expect(stmt.application).toBe("application2");
        expect(stmt.clientAddress).toBe("clientAddress2");
      } else {
        fail(`stmt user should be foo or bar, got ${stmt.user}`);
      }
      // expect(stmt.transactionID).toBe(defaultActiveStatement.transactionID);
      expect(stmt.status).toBe("Executing");
      expect(stmt.start.unix()).toBe(
        TimestampToMoment(defaultActiveQuery.start).unix(),
      );
      // expect(stmt.sessionID).toBe(defaultActiveStatement.sessionID);
      expect(stmt.query).toBe(defaultActiveStatement.query);
    });
  });

  describe("appsFromActiveStatements", () => {
    const activeStatements = [
      makeActiveStatement({ application: "app1" }),
      makeActiveStatement({ application: "app2" }),
      makeActiveStatement({ application: "app3" }),
      makeActiveStatement({ application: "app4" }),
    ];
    const apps = appsFromActiveStatements(activeStatements);
    expect(apps).toEqual(["app1", "app2", "app3", "app4"]);
  });

  describe("activeTransactionsFromSessions", () => {
    const txns = [
      {
        id: new Uint8Array(),
        start: new Timestamp({
          seconds: Long.fromNumber(
            Math.round(new Date("2022-01-01").getTime() / 1000),
          ),
        }),
        num_auto_retries: 3,
        num_statements_executed: 4,
      },
      {
        id: new Uint8Array(),
        start: new Timestamp({
          seconds: Long.fromNumber(
            Math.round(new Date("2022-01-01").getTime() / 1000),
          ),
        }),
        num_auto_retries: 4,
        num_statements_executed: 3,
      },
    ];

    const sessionsResponse: SessionsResponse = {
      sessions: [
        {
          id: new Uint8Array(),
          username: "bar",
          application_name: "application",
          client_address: "clientAddress",
          active_queries: [makeActiveQuery()],
          active_txn: txns[0],
        },
        {
          id: new Uint8Array(),
          username: "foo",
          application_name: "application2",
          client_address: "clientAddress2",
          active_queries: [makeActiveQuery()],
          active_txn: txns[1],
        },
      ],
      errors: [],
      internal_app_name_prefix: "",
      toJSON: () => ({}),
    };

    const activeTransactions = activeTransactionsFromSessions(sessionsResponse);

    expect(activeTransactions.length).toBe(txns.length);

    activeTransactions.forEach((txn: ActiveTransaction, i) => {
      expect(txn.application).toBe(
        sessionsResponse.sessions[i].application_name,
      );
      // expect(txn.sessionID).toBe(`txn${i + 1}`);
      expect(txn.status).toBe("Executing");
      expect(txn.mostRecentStatement).toBeTruthy();
      expect(txn.start.unix()).toBe(
        TimestampToMoment(defaultActiveQuery.start).unix(),
      );
    });
  });

  describe("appsFromActiveTransactions", () => {
    const activeTxns = [
      makeActiveTxn({ application: "app1" }),
      makeActiveTxn({ application: "app2" }),
      makeActiveTxn({ application: "app3" }),
      makeActiveTxn({ application: "app4" }),
    ];

    const apps = appsFromActiveTransactions(activeTxns);
    expect(apps).toEqual(["app1", "app2", "app3", "app4"]);
  });
});
