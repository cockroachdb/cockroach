// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { assert } from "chai";
import {
  generateTableID,
  databaseRequestToID,
  tableRequestToID,
} from "./apiReducers";
import * as protos from "src/js/protos";

describe("table id generator", function () {
  it("generates encoded db/table id", function () {
    const db = "&a.a.a/a.a/";
    const table = "/a.a/a.a.a&";
    assert.equal(
      generateTableID(db, table),
      encodeURIComponent(db) + "/" + encodeURIComponent(table),
    );
    assert.equal(
      decodeURIComponent(generateTableID(db, table).split("/")[0]),
      db,
    );
    assert.equal(
      decodeURIComponent(generateTableID(db, table).split("/")[1]),
      table,
    );
  });
});

describe("request to string functions", function () {
  it("correctly generates a string from a database details request", function () {
    const database = "testDatabase";
    const databaseRequest = new protos.cockroach.server.serverpb.DatabaseDetailsRequest(
      { database },
    );
    assert.equal(databaseRequestToID(databaseRequest), database);
  });
  it("correctly generates a string from a table details request", function () {
    const database = "testDatabase";
    const table = "testTable";
    const tableRequest = new protos.cockroach.server.serverpb.TableDetailsRequest(
      { database, table },
    );
    assert.equal(
      tableRequestToID(tableRequest),
      generateTableID(database, table),
    );
  });
});
