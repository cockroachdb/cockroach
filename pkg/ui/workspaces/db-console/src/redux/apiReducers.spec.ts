// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
  generateTableID,
  databaseRequestPayloadToID,
  tableRequestToID,
  createSelectorForCachedDataField,
} from "./apiReducers";
import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";
import { AdminUIState, createAdminUIStore } from "src/redux/state";
import { createMemoryHistory } from "history";
import { merge } from "lodash";
import moment from "moment-timezone";

describe("table id generator", function () {
  it("generates encoded db/table id", function () {
    const db = "&a.a.a/a.a/";
    const table = "/a.a/a.a.a&";
    expect(generateTableID(db, table)).toEqual(
      encodeURIComponent(db) + "/" + encodeURIComponent(table),
    );
    expect(
      decodeURIComponent(generateTableID(db, table).split("/")[0]),
    ).toEqual(db);
    expect(
      decodeURIComponent(generateTableID(db, table).split("/")[1]),
    ).toEqual(table);
  });
});

describe("request to string functions", function () {
  it("correctly generates a string from a database details request", function () {
    const database = "testDatabase";
    expect(databaseRequestPayloadToID(database)).toEqual(database);
  });
  it("correctly generates a string from a table details request", function () {
    const database = "testDatabase";
    const table = "testTable";
    const tableRequest: clusterUiApi.TableDetailsReqParams = {
      database,
      table,
    };
    expect(tableRequestToID(tableRequest)).toEqual(
      generateTableID(database, table),
    );
  });
});

describe("createSelectorForCachedDataField", () => {
  const setAt = moment.utc();

  function mockStoreState(): AdminUIState {
    const store = createAdminUIStore(createMemoryHistory());
    return merge(store.getState(), {
      cachedData: {
        statements: {
          inFlight: true,
          setAt,
          reqAt: null,
          data: null,
          lastError: new Error("hello world"),
          valid: true,
        },
        version: {
          inFlight: false,
          setAt: null,
          reqAt: moment.utc(),
          data: {
            details: [
              {
                version: "hello",
                detail: "world",
              },
            ],
          },
          lastError: null,
          valid: false,
        },
      },
    });
  }
  it("converts the fields of the cached data to RequestState", () => {
    const state = mockStoreState();

    const selectStmts = createSelectorForCachedDataField("statements");
    const selectVersion = createSelectorForCachedDataField("version");

    const stmtsState = selectStmts(state);
    expect(stmtsState).toEqual({
      inFlight: true,
      lastUpdated: setAt,
      data: null,
      error: new Error("hello world"),
      valid: true,
    });

    const versionState = selectVersion(state);
    expect(versionState).toEqual({
      inFlight: false,
      lastUpdated: null,
      data: {
        details: [
          {
            version: "hello",
            detail: "world",
          },
        ],
      },
      error: null,
      valid: false,
    });
  });
});
