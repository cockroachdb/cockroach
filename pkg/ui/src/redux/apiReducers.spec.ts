// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import { assert } from "chai";
import { generateTableID, databaseRequestToID, tableRequestToID } from "./apiReducers";
import * as protos from "src/js/protos";

describe("table id generator", function () {
  it("generates encoded db/table id", function () {
    const db = "&a.a.a/a.a/";
    const table = "/a.a/a.a.a&";
    assert.equal(generateTableID(db, table), encodeURIComponent(db) + "/" + encodeURIComponent(table));
    assert.equal(decodeURIComponent(generateTableID(db, table).split("/")[0]), db);
    assert.equal(decodeURIComponent(generateTableID(db, table).split("/")[1]), table);
  });
});

describe("request to string functions", function () {
  it("correctly generates a string from a database details request", function () {
    const database = "testDatabase";
    const databaseRequest = new protos.cockroach.server.serverpb.DatabaseDetailsRequest({ database });
    assert.equal(databaseRequestToID(databaseRequest), database);
  });
  it("correctly generates a string from a table details request", function () {
    const database = "testDatabase";
    const table = "testTable";
    const tableRequest = new protos.cockroach.server.serverpb.TableDetailsRequest({ database, table });
    assert.equal(tableRequestToID(tableRequest), generateTableID(database, table));
  });
});
