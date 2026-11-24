// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as protos from "@cockroachlabs/crdb-protobuf-client";
import Long from "long";

import { Filters } from "../queryFilter";

import { data, nodeRegions } from "./transactions.fixture";
import {
  filterTransactions,
  generateRegion,
  getStatementsByFingerprintId,
  statementFingerprintIdsToText,
} from "./utils";

type Statement =
  protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
type Transaction =
  protos.cockroach.server.serverpb.StatementsResponse.IExtendedCollectedTransactionStatistics;

describe("getStatementsByFingerprintId", () => {
  it("filters statements by fingerprint id", () => {
    const selectedStatements = getStatementsByFingerprintId(
      [
        Long.fromString("4104049045071304794"),
        Long.fromString("3334049045071304794"),
      ],
      [
        { id: Long.fromString("4104049045071304794") },
        { id: Long.fromString("5554049045071304794") },
      ],
    );
    expect(selectedStatements).toHaveLength(1);
    expect(
      selectedStatements[0].id.eq(Long.fromString("4104049045071304794")),
    ).toBe(true);
  });
});

const txData = data.transactions as Transaction[];

describe("Filter transactions", () => {
  it("show internal if no filters applied", () => {
    const filter: Filters = {
      app: "",
      timeNumber: "0",
      timeUnit: "seconds",
      nodes: "",
      regions: "",
    };
    expect(
      filterTransactions(
        txData,
        filter,
        "$ internal",
        data.statements,
        nodeRegions,
        false,
      ).transactions.length,
    ).toBe(11);
  });

  it("filters by app", () => {
    const filter: Filters = {
      app: "$ TEST",
      timeNumber: "0",
      timeUnit: "seconds",
      nodes: "",
      regions: "",
    };
    expect(
      filterTransactions(
        txData,
        filter,
        "$ internal",
        data.statements,
        nodeRegions,
        false,
      ).transactions.length,
    ).toBe(3);
  });

  it("filters by app exactly", () => {
    const filter: Filters = {
      app: "$ TEST EXACT",
      timeNumber: "0",
      timeUnit: "seconds",
      nodes: "",
      regions: "",
    };
    expect(
      filterTransactions(
        txData,
        filter,
        "$ internal",
        data.statements,
        nodeRegions,
        false,
      ).transactions.length,
    ).toBe(1);
  });

  it("filters by 2 apps", () => {
    const filter: Filters = {
      app: "$ TEST EXACT,$ TEST",
      timeNumber: "0",
      timeUnit: "seconds",
      nodes: "",
      regions: "",
    };
    expect(
      filterTransactions(
        txData,
        filter,
        "$ internal",
        data.statements,
        nodeRegions,
        false,
      ).transactions.length,
    ).toBe(4);
  });

  it("filters by internal prefix", () => {
    const filter: Filters = {
      app: data.internal_app_name_prefix,
      timeNumber: "0",
      timeUnit: "seconds",
      nodes: "",
      regions: "",
    };
    expect(
      filterTransactions(
        txData,
        filter,
        "$ internal",
        data.statements,
        nodeRegions,
        false,
      ).transactions.length,
    ).toBe(7);
  });

  it("filters by time", () => {
    const filter: Filters = {
      app: "$ internal,$ TEST",
      timeNumber: "40",
      timeUnit: "miliseconds",
      nodes: "",
      regions: "",
    };
    expect(
      filterTransactions(
        txData,
        filter,
        "$ internal",
        data.statements,
        nodeRegions,
        false,
      ).transactions.length,
    ).toBe(8);
  });

  it("filters by one node", () => {
    const filter: Filters = {
      app: "$ internal,$ TEST",
      timeNumber: "0",
      timeUnit: "seconds",
      nodes: "n1",
      regions: "",
    };
    expect(
      filterTransactions(
        txData,
        filter,
        "$ internal",
        data.statements,
        nodeRegions,
        false,
      ).transactions.length,
    ).toBe(6);
  });

  it("filters by multiple nodes", () => {
    const filter: Filters = {
      app: "$ internal,$ TEST,$ TEST EXACT",
      timeNumber: "0",
      timeUnit: "seconds",
      nodes: "n2,n4",
      regions: "",
    };
    expect(
      filterTransactions(
        txData,
        filter,
        "$ internal",
        data.statements,
        nodeRegions,
        false,
      ).transactions.length,
    ).toBe(8);
  });

  it("filters by one region", () => {
    const filter: Filters = {
      app: "$ internal,$ TEST",
      timeNumber: "0",
      timeUnit: "seconds",
      nodes: "",
      regions: "gcp-europe-west1",
    };
    expect(
      filterTransactions(
        txData,
        filter,
        "$ internal",
        data.statements,
        nodeRegions,
        false,
      ).transactions.length,
    ).toBe(4);
  });

  it("filters by multiple regions", () => {
    const filter: Filters = {
      app: "$ internal,$ TEST,$ TEST EXACT",
      timeNumber: "0",
      timeUnit: "seconds",
      nodes: "",
      regions: "gcp-us-west1,gcp-europe-west1",
    };
    expect(
      filterTransactions(
        txData,
        filter,
        "$ internal",
        data.statements,
        nodeRegions,
        false,
      ).transactions.length,
    ).toBe(9);
  });
});

describe("statementFingerprintIdsToText", () => {
  it("translate statement fingerprint IDs into queries", () => {
    const statements = [
      {
        id: Long.fromString("4104049045071304794"),
        key: {
          key_data: {
            query: "SELECT _",
          },
        },
      },
      {
        id: Long.fromString("5104049045071304794"),
        key: {
          key_data: {
            query: "SELECT _, _",
          },
        },
      },
    ];
    const statementFingerprintIds = [
      Long.fromString("4104049045071304794"),
      Long.fromString("5104049045071304794"),
      Long.fromString("4104049045071304794"),
      Long.fromString("4104049045071304794"),
    ];

    expect(statementFingerprintIdsToText(statementFingerprintIds, statements))
      .toBe(`SELECT _
SELECT _, _
SELECT _
SELECT _`);
  });
});

describe("generateRegion", () => {
  function transaction(...ids: number[]): Transaction {
    return {
      stats_data: {
        statement_fingerprint_ids: ids.map(id => Long.fromInt(id)),
      },
    };
  }

  function statement(id: number, ...regions: string[]): Statement {
    return { id: Long.fromInt(id), stats: { regions } };
  }

  it("gathers up the list of regions for the transaction, sorted", () => {
    expect(
      generateRegion(transaction(42, 43, 44), [
        statement(42, "gcp-us-west1", "gcp-us-east1"),
        statement(43, "gcp-us-west1"),
        statement(44, "gcp-us-central1"),
      ]),
    ).toEqual(["gcp-us-central1", "gcp-us-east1", "gcp-us-west1"]);
  });
});
