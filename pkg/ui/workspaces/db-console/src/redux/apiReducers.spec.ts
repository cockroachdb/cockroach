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
  databaseRequestPayloadToID,
  tableRequestToID,
  createSelectorForCachedDataField,
  createSelectorForKeyedCachedDataField,
} from "./apiReducers";
import { api as clusterUiApi, util } from "@cockroachlabs/cluster-ui";
import { AdminUIState, createAdminUIStore } from "src/redux/state";
import { createMemoryHistory } from "history";
import { merge } from "lodash";
import moment from "moment-timezone";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { RouteComponentProps } from "react-router";
import { queryByName } from "src/util/query";

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
      util.generateTableID(database, table),
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

describe("createSelectorForKeyedCachedDataField", () => {
  const setAt = moment.utc();

  const mockCertData: cockroach.server.serverpb.ICertificateDetails[] = [
    {
      type: cockroach.server.serverpb.CertificateDetails.CertificateType.CA,
      error_message: null,
      fields: [],
    },
    {
      type: cockroach.server.serverpb.CertificateDetails.CertificateType.UI,
      error_message: null,
      fields: [],
    },
  ];

  function mockStoreState(): AdminUIState {
    const store = createAdminUIStore(createMemoryHistory());

    return merge(store.getState(), {
      cachedData: {
        certificates: {
          0: {
            inFlight: false,
            setAt,
            reqAt: moment.utc(),
            data: { certificates: mockCertData },
            lastError: null,
            valid: true,
          },
          1: {
            inFlight: true,
            setAt: null,
            reqAt: null,
            data: null,
            lastError: new Error("error"),
            valid: false,
          },
        },
      },
    });
  }

  it("converts the fields of the keyed cached data to RequestState", () => {
    const state = mockStoreState();

    // This is the key selector for the keyed cache reducer below.
    const selectNode = (_state: AdminUIState, props: RouteComponentProps) =>
      queryByName(props.location, "nodeID");
    const selectCerts = createSelectorForKeyedCachedDataField(
      "certificates",
      selectNode,
    );

    const node0State = selectCerts(state, {
      history: null,
      match: null,
      location: {
        search: "?nodeID=0",
        pathname: null,
        state: null,
        hash: null,
      },
    });

    expect(node0State).toEqual({
      inFlight: false,
      lastUpdated: setAt,
      data: { certificates: mockCertData },
      error: null,
      valid: true,
    });

    const node1State = selectCerts(state, {
      history: null,
      match: null,
      location: {
        search: "?nodeID=1",
        pathname: null,
        state: null,
        hash: null,
      },
    });

    expect(node1State).toEqual({
      inFlight: true,
      lastUpdated: null,
      data: null,
      error: new Error("error"),
      valid: false,
    });

    const noEntry = selectCerts(state, {
      history: null,
      match: null,
      location: {
        search: "?nodeID=2",
        pathname: null,
        state: null,
        hash: null,
      },
    });

    expect(noEntry).toEqual({
      inFlight: false,
      lastUpdated: undefined,
      data: undefined,
      error: undefined,
      valid: true,
    });
  });
});
