// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import assert from "assert";
import { createMemoryHistory } from "history";
import _ from "lodash";
import Long from "long";
import { RouteComponentProps } from "react-router-dom";
import { bindActionCreators, Store } from "redux";
import {
  DatabaseTablePageActions,
  DatabaseTablePageData,
  DatabaseTablePageDataDetails,
  DatabaseTablePageDataStats,
} from "@cockroachlabs/cluster-ui";

import { AdminUIState, createAdminUIStore } from "src/redux/state";
import { databaseNameAttr, tableNameAttr } from "src/util/constants";
import * as fakeApi from "src/util/fakeApi";
import { mapStateToProps, mapDispatchToProps } from "./redux";

function fakeRouteComponentProps(
  k1: string,
  v1: string,
  k2: string,
  v2: string,
): RouteComponentProps {
  return {
    history: createMemoryHistory(),
    location: {
      pathname: "",
      search: "",
      state: {},
      hash: "",
    },
    match: {
      params: {
        [k1]: v1,
        [k2]: v2,
      },
      isExact: true,
      path: "",
      url: "",
    },
  };
}

class TestDriver {
  private readonly actions: DatabaseTablePageActions;
  private readonly properties: () => DatabaseTablePageData;

  constructor(
    store: Store<AdminUIState>,
    private readonly database: string,
    private readonly table: string,
  ) {
    this.actions = bindActionCreators(
      mapDispatchToProps,
      store.dispatch.bind(store),
    );
    this.properties = () =>
      mapStateToProps(
        store.getState(),
        fakeRouteComponentProps(
          databaseNameAttr,
          database,
          tableNameAttr,
          table,
        ),
      );
  }

  assertProperties(expected: DatabaseTablePageData) {
    assert.deepEqual(this.properties(), expected);
  }

  assertTableDetails(expected: DatabaseTablePageDataDetails) {
    assert.deepEqual(this.properties().details, expected);
  }

  assertTableStats(expected: DatabaseTablePageDataStats) {
    assert.deepEqual(this.properties().stats, expected);
  }

  async refreshTableDetails() {
    return this.actions.refreshTableDetails(this.database, this.table);
  }

  async refreshTableStats() {
    return this.actions.refreshTableStats(this.database, this.table);
  }
}

describe("Database Table Page", function() {
  let driver: TestDriver;

  beforeEach(function() {
    driver = new TestDriver(
      createAdminUIStore(createMemoryHistory()),
      "DATABASE",
      "TABLE",
    );
  });

  afterEach(function() {
    fakeApi.restore();
  });

  it("starts in a pre-loading state", function() {
    driver.assertProperties({
      databaseName: "DATABASE",
      name: "TABLE",
      details: {
        loading: false,
        loaded: false,
        createStatement: "",
        replicaCount: 0,
        indexNames: [],
        grants: [],
      },
      stats: {
        loading: false,
        loaded: false,
        sizeInBytes: 0,
        rangeCount: 0,
      },
    });
  });

  it("loads table details", async function() {
    fakeApi.stubTableDetails("DATABASE", "TABLE", {
      grants: [
        { user: "admin", privileges: ["CREATE", "DROP"] },
        { user: "public", privileges: ["SELECT"] },
      ],
      indexes: [
        { name: "primary" },
        { name: "another_index", seq: new Long(1) },
        { name: "another_index", seq: new Long(2) },
      ],
      create_table_statement: "CREATE TABLE foo",
      zone_config: {
        num_replicas: 5,
      },
    });

    await driver.refreshTableDetails();

    driver.assertTableDetails({
      loading: false,
      loaded: true,
      createStatement: "CREATE TABLE foo",
      replicaCount: 5,
      indexNames: ["primary", "another_index"],
      grants: [
        { user: "admin", privilege: "CREATE" },
        { user: "admin", privilege: "DROP" },
        { user: "public", privilege: "SELECT" },
      ],
    });
  });

  it("loads table stats", async function() {
    fakeApi.stubTableStats("DATABASE", "TABLE", {
      range_count: new Long(4200),
      approximate_disk_bytes: new Long(44040192),
    });

    await driver.refreshTableStats();

    driver.assertTableStats({
      loading: false,
      loaded: true,
      sizeInBytes: 44040192,
      rangeCount: 4200,
    });
  });
});
