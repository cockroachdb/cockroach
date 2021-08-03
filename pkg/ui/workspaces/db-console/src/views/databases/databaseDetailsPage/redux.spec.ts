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
  DatabaseDetailsPageActions,
  DatabaseDetailsPageData,
  DatabaseDetailsPageDataTableDetails,
  DatabaseDetailsPageDataTableStats,
} from "@cockroachlabs/cluster-ui";

import { AdminUIState, createAdminUIStore } from "src/redux/state";
import { databaseNameAttr } from "src/util/constants";
import * as fakeApi from "src/util/fakeApi";
import { mapStateToProps, mapDispatchToProps } from "./redux";

function fakeRouteComponentProps(
  key: string,
  value: string,
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
        [key]: value,
      },
      isExact: true,
      path: "",
      url: "",
    },
  };
}

class TestDriver {
  private readonly actions: DatabaseDetailsPageActions;
  private readonly properties: () => DatabaseDetailsPageData;

  constructor(store: Store<AdminUIState>, private readonly database: string) {
    this.actions = bindActionCreators(
      mapDispatchToProps,
      store.dispatch.bind(store),
    );
    this.properties = () =>
      mapStateToProps(
        store.getState(),
        fakeRouteComponentProps(databaseNameAttr, database),
      );
  }

  assertProperties(expected: DatabaseDetailsPageData) {
    assert.deepEqual(this.properties(), expected);
  }

  assertTableDetails(
    name: string,
    expected: DatabaseDetailsPageDataTableDetails,
  ) {
    assert.deepEqual(this.findTable(name).details, expected);
  }

  assertTableRoles(name: string, expected: string[]) {
    assert.deepEqual(this.findTable(name).details.roles, expected);
  }

  assertTableGrants(name: string, expected: string[]) {
    assert.deepEqual(this.findTable(name).details.grants, expected);
  }

  assertTableStats(name: string, expected: DatabaseDetailsPageDataTableStats) {
    assert.deepEqual(this.findTable(name).stats, expected);
  }

  async refreshDatabaseDetails() {
    return this.actions.refreshDatabaseDetails(this.database);
  }

  async refreshTableDetails(table: string) {
    return this.actions.refreshTableDetails(this.database, table);
  }

  async refreshTableStats(table: string) {
    return this.actions.refreshTableStats(this.database, table);
  }

  private findTable(name: string) {
    return _.find(this.properties().tables, { name });
  }
}

describe("Database Details Page", function() {
  let driver: TestDriver;

  beforeEach(function() {
    driver = new TestDriver(
      createAdminUIStore(createMemoryHistory()),
      "things",
    );
  });

  afterEach(function() {
    fakeApi.restore();
  });

  it("starts in a pre-loading state", function() {
    driver.assertProperties({
      loading: false,
      loaded: false,
      name: "things",
      tables: [],
    });
  });

  it("makes a row for each table", async function() {
    fakeApi.stubDatabaseDetails("things", {
      table_names: ["foo", "bar"],
    });

    await driver.refreshDatabaseDetails();

    driver.assertProperties({
      loading: false,
      loaded: true,
      name: "things",
      tables: [
        {
          name: "foo",
          details: {
            loading: false,
            loaded: false,
            columnCount: 0,
            indexCount: 0,
            userCount: 0,
            roles: [],
            grants: [],
          },
          stats: {
            loading: false,
            loaded: false,
            replicationSizeInBytes: 0,
            rangeCount: 0,
          },
        },
        {
          name: "bar",
          details: {
            loading: false,
            loaded: false,
            columnCount: 0,
            indexCount: 0,
            userCount: 0,
            roles: [],
            grants: [],
          },
          stats: {
            loading: false,
            loaded: false,
            replicationSizeInBytes: 0,
            rangeCount: 0,
          },
        },
      ],
    });
  });

  it("loads table details", async function() {
    fakeApi.stubDatabaseDetails("things", {
      table_names: ["foo", "bar"],
    });

    fakeApi.stubTableDetails("things", "foo", {
      grants: [
        { user: "admin", privileges: ["CREATE"] },
        { user: "public", privileges: ["SELECT"] },
      ],
      // The actual contents below don't matter to us; we just count them.
      columns: [{}, {}, {}, {}, {}],
      indexes: [{}, {}, {}],
    });

    fakeApi.stubTableDetails("things", "bar", {
      grants: [
        { user: "root", privileges: ["ALL"] },
        { user: "app", privileges: ["INSERT"] },
        { user: "data", privileges: ["SELECT"] },
      ],
      // The actual contents below don't matter to us; we just count them.
      columns: [{}, {}, {}, {}],
      indexes: [{}, {}],
    });

    await driver.refreshDatabaseDetails();
    await driver.refreshTableDetails("foo");
    await driver.refreshTableDetails("bar");

    driver.assertTableDetails("foo", {
      loading: false,
      loaded: true,
      columnCount: 5,
      indexCount: 3,
      userCount: 2,
      roles: ["admin", "public"],
      grants: ["CREATE", "SELECT"],
    });

    driver.assertTableDetails("bar", {
      loading: false,
      loaded: true,
      columnCount: 4,
      indexCount: 2,
      userCount: 3,
      roles: ["root", "app", "data"],
      grants: ["ALL", "SELECT", "INSERT"],
    });
  });

  it("sorts roles meaningfully", async function() {
    fakeApi.stubDatabaseDetails("things", {
      table_names: ["foo"],
    });

    fakeApi.stubTableDetails("things", "foo", {
      grants: [
        { user: "bzuckercorn", privileges: ["ALL"] },
        { user: "bloblaw", privileges: ["ALL"] },
        { user: "jwweatherman", privileges: ["ALL"] },
        { user: "admin", privileges: ["ALL"] },
        { user: "public", privileges: ["ALL"] },
        { user: "root", privileges: ["ALL"] },
      ],
    });

    await driver.refreshDatabaseDetails();
    await driver.refreshTableDetails("foo");

    driver.assertTableRoles("foo", [
      "root",
      "admin",
      "public",
      "bloblaw",
      "bzuckercorn",
      "jwweatherman",
    ]);
  });

  it("sorts grants meaningfully", async function() {
    fakeApi.stubDatabaseDetails("things", {
      table_names: ["foo"],
    });

    fakeApi.stubTableDetails("things", "foo", {
      grants: [
        {
          user: "admin",
          privileges: ["ALL", "CREATE", "DELETE", "DROP", "GRANT"],
        },
        {
          user: "public",
          privileges: ["DROP", "GRANT", "INSERT", "SELECT", "UPDATE"],
        },
      ],
    });

    await driver.refreshDatabaseDetails();
    await driver.refreshTableDetails("foo");

    driver.assertTableGrants("foo", [
      "ALL",
      "CREATE",
      "DROP",
      "GRANT",
      "SELECT",
      "INSERT",
      "UPDATE",
      "DELETE",
    ]);
  });

  it("loads table stats", async function() {
    fakeApi.stubDatabaseDetails("things", {
      table_names: ["foo", "bar"],
    });

    fakeApi.stubTableStats("things", "foo", {
      range_count: new Long(4200),
      approximate_disk_bytes: new Long(44040192),
    });

    fakeApi.stubTableStats("things", "bar", {
      range_count: new Long(1023),
      approximate_disk_bytes: new Long(8675309),
    });

    await driver.refreshDatabaseDetails();
    await driver.refreshTableStats("foo");
    await driver.refreshTableStats("bar");

    driver.assertTableStats("foo", {
      loading: false,
      loaded: true,
      replicationSizeInBytes: 44040192,
      rangeCount: 4200,
    });

    driver.assertTableStats("bar", {
      loading: false,
      loaded: true,
      replicationSizeInBytes: 8675309,
      rangeCount: 1023,
    });
  });
});
