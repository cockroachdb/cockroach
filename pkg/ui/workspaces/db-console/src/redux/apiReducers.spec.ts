// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { databaseRequestPayloadToID, tableRequestToID } from "./apiReducers";
import { api as clusterUiApi, util } from "@cockroachlabs/cluster-ui";
import { indexUnusedDuration } from "../util/constants";

describe("table id generator", function () {
  it("generates encoded db/table id", function () {
    const db = "&a.a.a/a.a/";
    const table = "/a.a/a.a.a&";
    expect(util.generateTableID(db, table)).toEqual(
      encodeURIComponent(db) + "/" + encodeURIComponent(table),
    );
    expect(
      decodeURIComponent(util.generateTableID(db, table).split("/")[0]),
    ).toEqual(db);
    expect(
      decodeURIComponent(util.generateTableID(db, table).split("/")[1]),
    ).toEqual(table);
  });
});

describe("request to string functions", function () {
  it("correctly generates a string from a database details request", function () {
    const database = "testDatabase";
    expect(
      databaseRequestPayloadToID({
        database,
        csIndexUnusedDuration: indexUnusedDuration,
      }),
    ).toEqual(database);
  });
  it("correctly generates a string from a table details request", function () {
    const database = "testDatabase";
    const table = "testTable";
    const tableRequest: clusterUiApi.TableDetailsReqParams = {
      database,
      table,
      csIndexUnusedDuration: indexUnusedDuration,
    };
    expect(tableRequestToID(tableRequest)).toEqual(
      util.generateTableID(database, table),
    );
  });
});
