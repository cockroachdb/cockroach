import { assert } from "chai";
import { filterTransactions, getStatementsById } from "./utils";
import { Filters } from "../queryFilter/filter";
import { data } from "./transactions.fixture";
import Long from "long";
import * as protos from "@cockroachlabs/crdb-protobuf-client";

type Transaction = protos.cockroach.server.serverpb.StatementsResponse.IExtendedCollectedTransactionStatistics;

describe("getStatementsById", () => {
  it("filters statements by id", () => {
    const selectedStatements = getStatementsById(
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
    };
    assert.equal(
      filterTransactions(txData, filter, "$ internal").transactions.length,
      11,
    );
  });

  it("filters by app", () => {
    const filter: Filters = {
      app: "$ TEST",
      timeNumber: "0",
      timeUnit: "seconds",
    };
    assert.equal(
      filterTransactions(txData, filter, "$ internal").transactions.length,
      3,
    );
  });

  it("filters by app exactly", () => {
    const filter: Filters = {
      app: "$ TEST EXACT",
      timeNumber: "0",
      timeUnit: "seconds",
    };
    assert.equal(
      filterTransactions(txData, filter, "$ internal").transactions.length,
      1,
    );
  });

  it("filters by internal prefix", () => {
    const filter: Filters = {
      app: data.internal_app_name_prefix,
      timeNumber: "0",
      timeUnit: "seconds",
    };
    assert.equal(
      filterTransactions(txData, filter, "$ internal").transactions.length,
      7,
    );
  });

  it("filters by time", () => {
    const filter: Filters = {
      app: "All",
      timeNumber: "40",
      timeUnit: "miliseconds",
    };
    assert.equal(
      filterTransactions(txData, filter, "$ internal").transactions.length,
      8,
    );
  });
});
