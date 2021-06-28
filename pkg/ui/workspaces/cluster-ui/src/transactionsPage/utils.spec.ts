// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { assert } from "chai";
import { filterTransactions, getStatementsByFingerprintId } from "./utils";
import { Filters } from "../queryFilter/filter";
import { data, nodeRegions } from "./transactions.fixture";
import Long from "long";
import * as protos from "@cockroachlabs/crdb-protobuf-client";

type Transaction = protos.cockroach.server.serverpb.StatementsResponse.IExtendedCollectedTransactionStatistics;

describe("getStatementsByFingerprintId", () => {
  it("filters statements by fingerprint id", () => {
    const selectedStatements = getStatementsByFingerprintId(
      [Long.fromInt(4104049045071304794), Long.fromInt(3334049045071304794)],
      [
        { id: Long.fromInt(4104049045071304794) },
        { id: Long.fromInt(5554049045071304794) },
      ],
    );
    assert.lengthOf(selectedStatements, 1);
    assert.isTrue(
      selectedStatements[0].id.eq(Long.fromInt(4104049045071304794)),
    );
  });
});

const txData = (data.transactions as any) as Transaction[];

describe("Filter transactions", () => {
  it("show all if no filters applied", () => {
    const filter: Filters = {
      app: "All",
      timeNumber: "0",
      timeUnit: "seconds",
      nodes: "",
      regions: "",
    };
    assert.equal(
      filterTransactions(
        txData,
        filter,
        "$ internal",
        data.statements,
        nodeRegions,
      ).transactions.length,
      11,
    );
  });

  it("filters by app", () => {
    const filter: Filters = {
      app: "$ TEST",
      timeNumber: "0",
      timeUnit: "seconds",
      nodes: "",
      regions: "",
    };
    assert.equal(
      filterTransactions(
        txData,
        filter,
        "$ internal",
        data.statements,
        nodeRegions,
      ).transactions.length,
      3,
    );
  });

  it("filters by app exactly", () => {
    const filter: Filters = {
      app: "$ TEST EXACT",
      timeNumber: "0",
      timeUnit: "seconds",
      nodes: "",
      regions: "",
    };
    assert.equal(
      filterTransactions(
        txData,
        filter,
        "$ internal",
        data.statements,
        nodeRegions,
      ).transactions.length,
      1,
    );
  });

  it("filters by internal prefix", () => {
    const filter: Filters = {
      app: data.internal_app_name_prefix,
      timeNumber: "0",
      timeUnit: "seconds",
      nodes: "",
      regions: "",
    };
    assert.equal(
      filterTransactions(
        txData,
        filter,
        "$ internal",
        data.statements,
        nodeRegions,
      ).transactions.length,
      7,
    );
  });

  it("filters by time", () => {
    const filter: Filters = {
      app: "All",
      timeNumber: "40",
      timeUnit: "miliseconds",
      nodes: "",
      regions: "",
    };
    assert.equal(
      filterTransactions(
        txData,
        filter,
        "$ internal",
        data.statements,
        nodeRegions,
      ).transactions.length,
      8,
    );
  });

  it("filters by one node", () => {
    const filter: Filters = {
      app: "All",
      timeNumber: "0",
      timeUnit: "seconds",
      nodes: "n1",
      regions: "",
    };
    assert.equal(
      filterTransactions(
        txData,
        filter,
        "$ internal",
        data.statements,
        nodeRegions,
      ).transactions.length,
      6,
    );
  });

  it("filters by multiple nodes", () => {
    const filter: Filters = {
      app: "All",
      timeNumber: "0",
      timeUnit: "seconds",
      nodes: "n2,n4",
      regions: "",
    };
    assert.equal(
      filterTransactions(
        txData,
        filter,
        "$ internal",
        data.statements,
        nodeRegions,
      ).transactions.length,
      8,
    );
  });

  it("filters by one region", () => {
    const filter: Filters = {
      app: "All",
      timeNumber: "0",
      timeUnit: "seconds",
      nodes: "",
      regions: "gcp-europe-west1",
    };
    assert.equal(
      filterTransactions(
        txData,
        filter,
        "$ internal",
        data.statements,
        nodeRegions,
      ).transactions.length,
      4,
    );
  });

  it("filters by multiple regions", () => {
    const filter: Filters = {
      app: "All",
      timeNumber: "0",
      timeUnit: "seconds",
      nodes: "",
      regions: "gcp-us-west1,gcp-europe-west1",
    };
    assert.equal(
      filterTransactions(
        txData,
        filter,
        "$ internal",
        data.statements,
        nodeRegions,
      ).transactions.length,
      9,
    );
  });
});
