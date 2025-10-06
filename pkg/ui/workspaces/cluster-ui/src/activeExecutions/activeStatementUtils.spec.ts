// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as protos from "@cockroachlabs/crdb-protobuf-client";
import Long from "long";
import moment from "moment-timezone";

import { TimestampToMoment } from "../util";

import {
  getActiveExecutionsFromSessions,
  getAppsFromActiveExecutions,
  filterActiveStatements,
  filterActiveTransactions,
  INTERNAL_APP_NAME_PREFIX,
} from "./activeStatementUtils";
import {
  ActiveStatementPhase,
  SessionsResponse,
  ActiveTransaction,
  ActiveStatement,
  SessionStatusType,
  ActiveStatementFilters,
  ActiveTransactionFilters,
  ExecutionStatus,
} from "./types";

type ActiveQuery = protos.cockroach.server.serverpb.ActiveQuery;
const Timestamp = protos.google.protobuf.Timestamp;

const MOCK_START_TIME = moment(new Date("2022-01-04T08:00:00"));

const defaultActiveQuery = {
  sql: "SELECT 4321",
  sql_no_constants: "SELECT _",
  id: "queryId",
  txn_id: new Uint8Array(),
  phase: ActiveStatementPhase.EXECUTING,
  start: new Timestamp({
    seconds: Long.fromNumber(MOCK_START_TIME.unix()),
  }),
};

const defaultActiveStatement: ActiveStatement = {
  statementID: defaultActiveQuery.id,
  transactionID: "transactionID",
  sessionID: "sessionID",
  query: defaultActiveQuery.sql,
  status: ExecutionStatus.Executing,
  start: MOCK_START_TIME,
  elapsedTime: moment.duration(60),
  application: "test",
  database: "db_test",
  user: "user",
  clientAddress: "clientAddress",
  isFullScan: false,
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
    transactionID: "txn",
    sessionID: "sessionID",
    start: MOCK_START_TIME,
    elapsedTime: moment.duration(60),
    application: "application",
    query: defaultActiveStatement.query,
    statementID: defaultActiveStatement.statementID,
    retries: 3,
    lastAutoRetryReason: null,
    priority: "Normal",
    statementCount: 5,
    status: ExecutionStatus.Executing,
    ...props,
  };
}

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
        makeActiveStatement({ statementID: "1", application: "app1" }),
        makeActiveStatement({ statementID: "2", application: "app2" }),
        makeActiveStatement({ statementID: "3", application: "app3" }),
        makeActiveStatement({ statementID: "4", application: "app1" }),
      ];

      const filters: ActiveStatementFilters = { app: "app1" };
      const filtered = filterActiveStatements(
        statements,
        filters,
        INTERNAL_APP_NAME_PREFIX,
      );
      expect(filtered.length).toBe(2);
      expect(filtered[0].statementID).toBe("1");
      expect(filtered[1].statementID).toBe("4");
    });

    it("should filter out statements that do not match search query", () => {
      const statements: ActiveStatement[] = [
        makeActiveStatement({
          statementID: "1",
          application: "app1",
          query: "SELECT 1",
        }),
        makeActiveStatement({
          statementID: "2",
          application: "app1",
          query: "SELECT 1",
        }),
        makeActiveStatement({
          statementID: "3",
          application: "app1",
          query: "SELECT 2",
        }),
        makeActiveStatement({
          statementID: "4",
          application: "app1",
          query: "SELECT 3",
        }),
      ];

      const filters: ActiveStatementFilters = { app: "app1" };
      const search = "SELECT 1";
      const filtered = filterActiveStatements(
        statements,
        filters,
        INTERNAL_APP_NAME_PREFIX,
        search,
      );

      expect(filtered.length).toBe(2);
      expect(filtered[0].statementID).toBe("1");
      expect(filtered[1].statementID).toBe("2");
    });

    it("should return all statements on empty filters and search", () => {
      const statements: ActiveStatement[] = [
        makeActiveStatement(),
        makeActiveStatement(),
        makeActiveStatement(),
        makeActiveStatement(),
        makeActiveStatement(),
      ];

      const filters: ActiveStatementFilters = {};
      const filtered = filterActiveStatements(
        statements,
        filters,
        INTERNAL_APP_NAME_PREFIX,
      );

      expect(filtered.length).toBe(statements.length);
    });
  });

  describe("getActiveExecutionsFromSessions", () => {
    it("should convert sessions response to active statements result", () => {
      const sessionsResponse: SessionsResponse = {
        sessions: [
          {
            id: new Uint8Array(),
            username: "bar",
            application_name: "application",
            client_address: "clientAddress",
            active_queries: [makeActiveQuery({ id: "1" })],
          },
          {
            id: new Uint8Array(),
            username: "foo",
            application_name: "application2",
            client_address: "clientAddress2",
            active_queries: [makeActiveQuery({ id: "2" })],
          },
          {
            id: new Uint8Array(),
            username: "closed",
            status: SessionStatusType.CLOSED,
            // Closed sessions should not appear in active stmts.
            application_name: "application2",
            client_address: "clientAddress2",
            active_queries: [makeActiveQuery({ id: "3" })],
          },
        ],
        errors: [],
        internal_app_name_prefix: INTERNAL_APP_NAME_PREFIX,
      };

      const statements =
        getActiveExecutionsFromSessions(sessionsResponse).statements;

      expect(statements.length).toBe(2);

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
        expect(stmt.status).toBe(ExecutionStatus.Executing);
        expect(stmt.start.unix()).toBe(
          TimestampToMoment(defaultActiveQuery.start).unix(),
        );
        expect(stmt.query).toBe(defaultActiveStatement.query);
      });
    });

    it("should convert sessions response to active transactions result", () => {
      const sessionsResponse: SessionsResponse = {
        sessions: [
          {
            id: new Uint8Array(),
            username: "bar",
            application_name: "application",
            client_address: "clientAddress",
            active_queries: [makeActiveQuery()],
            active_txn: {
              id: new Uint8Array(),
              start: new Timestamp({
                seconds: Long.fromNumber(MOCK_START_TIME.unix()),
              }),
              num_auto_retries: 3,
              num_statements_executed: 4,
            },
          },
          {
            id: new Uint8Array(),
            username: "foo",
            application_name: "application2",
            client_address: "clientAddress2",
            active_queries: [makeActiveQuery()],
            active_txn: {
              id: new Uint8Array(),
              start: new Timestamp({
                seconds: Long.fromNumber(MOCK_START_TIME.unix()),
              }),
              num_auto_retries: 4,
              num_statements_executed: 3,
            },
          },
          {
            id: new Uint8Array(),
            username: "baz",
            application_name: "application3",
            client_address: "clientAddress3",
            active_queries: [],
            active_txn: {
              id: new Uint8Array(),
              start: new Timestamp({
                seconds: Long.fromNumber(MOCK_START_TIME.unix()),
              }),
              num_auto_retries: 2,
              num_statements_executed: 3,
            },
            last_active_query: "select 1",
          },
          // The below txn should be filtered out.
          {
            id: new Uint8Array(),
            username: "foo",
            status: SessionStatusType.CLOSED,
            application_name: "closed_application",
            client_address: "clientAddress2",
            active_queries: [makeActiveQuery()],
            active_txn: {
              id: new Uint8Array(),
              start: new Timestamp({
                seconds: Long.fromNumber(MOCK_START_TIME.unix()),
              }),
              num_auto_retries: 2,
              num_statements_executed: 3,
            },
          },
        ],
        errors: [],
        internal_app_name_prefix: INTERNAL_APP_NAME_PREFIX,
      };

      const activeTransactions =
        getActiveExecutionsFromSessions(sessionsResponse).transactions;

      // Should filter out the txn from closed  session.
      expect(activeTransactions.length).toBe(3);

      let executingCnt = 0;
      activeTransactions.forEach((txn: ActiveTransaction, i) => {
        expect(txn.application).toBe(
          sessionsResponse.sessions[i].application_name,
        );
        expect(txn.query).toBeTruthy();
        if (sessionsResponse.sessions[i].active_queries.length > 0) {
          expect(txn.status).toEqual(ExecutionStatus.Executing);
          executingCnt++;
        } else {
          expect(txn.status).toEqual(ExecutionStatus.Idle);
        }
        expect(txn.start.unix()).toBe(
          TimestampToMoment(defaultActiveQuery.start).unix(),
        );
      });

      expect(executingCnt).toEqual(2);
    });

    it("should populate txn latest query when there is no active stmt for txns with at least 1 stmt", () => {
      const lastActiveQueryText = "SELECT 1";
      const sessionsResponse: SessionsResponse = {
        sessions: [
          {
            id: new Uint8Array(),
            last_active_query: lastActiveQueryText,
            active_queries: [],
            active_txn: {
              id: new Uint8Array(),
              start: new Timestamp({
                seconds: Long.fromNumber(MOCK_START_TIME.unix()),
              }),
              num_auto_retries: 0,
              num_statements_executed: 1,
            },
          },
          {
            id: new Uint8Array(),
            last_active_query: lastActiveQueryText,
            active_queries: [],
            active_txn: {
              id: new Uint8Array(),
              start: new Timestamp({
                seconds: Long.fromNumber(MOCK_START_TIME.unix()),
              }),
              num_auto_retries: 0,
              num_statements_executed: 0,
            },
          },
        ],
        errors: [],
        internal_app_name_prefix: INTERNAL_APP_NAME_PREFIX,
      };

      const activeExecs = getActiveExecutionsFromSessions(sessionsResponse);

      expect(activeExecs.transactions[0].query).toBe(lastActiveQueryText);
      expect(activeExecs.transactions[1].query).toBeFalsy();
    });
  });

  describe("getAppsFromActiveExecutions", () => {
    const activeStatements = [
      makeActiveStatement({ application: "app1" }),
      makeActiveStatement({ application: "app2" }),
      makeActiveStatement({ application: "app3" }),
      makeActiveStatement({ application: "app4" }),
    ];
    const apps = getAppsFromActiveExecutions(
      activeStatements,
      INTERNAL_APP_NAME_PREFIX,
    );
    expect(apps).toEqual(["app1", "app2", "app3", "app4"]);
  });

  describe("filterActiveTransactions", () => {
    it("should filter out txns that do not match filters", () => {
      const txns: ActiveTransaction[] = [
        makeActiveTxn({ transactionID: "1", application: "app1" }),
        makeActiveTxn({ transactionID: "2", application: "app2" }),
        makeActiveTxn({ transactionID: "3", application: "app3" }),
        makeActiveTxn({ transactionID: "4", application: "app1" }),
      ];

      const filters: ActiveTransactionFilters = { app: "app1" };
      const filtered = filterActiveTransactions(
        txns,
        filters,
        INTERNAL_APP_NAME_PREFIX,
      );

      expect(filtered.length).toBe(2);
      expect(filtered[0].transactionID).toBe("1");
      expect(filtered[1].transactionID).toBe("4");
    });

    it("should filter out txns that do not match search query", () => {
      const txns: ActiveTransaction[] = [
        makeActiveTxn({
          transactionID: "1",
          application: "app1",
          query: "SELECT 1",
        }),
        makeActiveTxn({
          transactionID: "2",
          application: "app1",
          query: "SELECT 1",
        }),
        makeActiveTxn({
          transactionID: "3",
          application: "app1",
          query: "SELECT 2",
        }),
        makeActiveTxn({
          transactionID: "4",
          application: "app1",
          query: "SELECT 2",
        }),
      ];

      const filters: ActiveTransactionFilters = { app: "app1" };
      const search = "SELECT 1";
      const filtered = filterActiveTransactions(
        txns,
        filters,
        INTERNAL_APP_NAME_PREFIX,
        search,
      );

      expect(filtered.length).toBe(2);
      expect(filtered[0].transactionID).toBe("1");
      expect(filtered[1].transactionID).toBe("2");
    });

    it("should return all statements on empty filters and search", () => {
      const txns: ActiveTransaction[] = [
        makeActiveTxn(),
        makeActiveTxn(),
        makeActiveTxn(),
        makeActiveTxn(),
        makeActiveTxn(),
      ];

      const filters: ActiveTransactionFilters = {};
      const filtered = filterActiveTransactions(
        txns,
        filters,
        INTERNAL_APP_NAME_PREFIX,
      );

      expect(filtered.length).toBe(txns.length);
    });
  });
});
