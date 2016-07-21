import { assert } from "chai";
import { generateTableID, databaseRequestToID, tableRequestToID } from "./apiReducers";
import * as protos from "../js/protos";

describe("table id generator", function () {
  it("generates encoded db/table id", function () {
    assert.equal(generateTableID("a.a.a.a", "a.a.a"), encodeURIComponent("a.a.a.a") + "/" + encodeURIComponent("a.a.a"));
  });
});

describe("request to string functions", function () {
  it("correctly generates a string from a database details request", function () {
    let database = "testDatabase";
    let databaseRequest = new protos.cockroach.server.serverpb.DatabaseDetailsRequest({ database });
    assert.equal(databaseRequestToID(databaseRequest), database);
  });
  it("correctly generates a string from a table details request", function () {
    let database = "testDatabase";
    let table = "testTable";
    let tableRequest = new protos.cockroach.server.serverpb.TableDetailsRequest({ database, table });
    assert.equal(tableRequestToID(tableRequest), generateTableID(database, table));
  });
});
