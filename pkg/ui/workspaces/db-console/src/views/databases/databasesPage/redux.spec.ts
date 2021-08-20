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
import { bindActionCreators, Store } from "redux";
import {
  DatabasesPageActions,
  DatabasesPageData,
  DatabasesPageDataDatabase,
  DatabasesPageDataMissingTable,
} from "@cockroachlabs/cluster-ui";

import { AdminUIState, createAdminUIStore } from "src/redux/state";
import * as fakeApi from "src/util/fakeApi";
import { mapStateToProps, mapDispatchToProps } from "./redux";

class TestDriver {
  private readonly actions: DatabasesPageActions;
  private readonly properties: () => DatabasesPageData;

  constructor(store: Store<AdminUIState>) {
    this.actions = bindActionCreators(
      mapDispatchToProps,
      store.dispatch.bind(store),
    );
    this.properties = () => mapStateToProps(store.getState());
  }

  async refreshDatabases() {
    return this.actions.refreshDatabases();
  }

  async refreshDatabaseDetails(database: string) {
    return this.actions.refreshDatabaseDetails(database);
  }

  async refreshTableStats(database: string, table: string) {
    return this.actions.refreshTableStats(database, table);
  }

  assertProperties(expected: DatabasesPageData) {
    assert.deepEqual(this.properties(), expected);
  }

  assertDatabaseProperties(
    database: string,
    expected: DatabasesPageDataDatabase,
  ) {
    assert.deepEqual(this.findDatabase(database), expected);
  }

  assertMissingTableProperties(
    database: string,
    table: string,
    expected: DatabasesPageDataMissingTable,
  ) {
    assert.deepEqual(
      this.findMissingTable(this.findDatabase(database), table),
      expected,
    );
  }

  private findDatabase(name: string) {
    return _.find(this.properties().databases, row => row.name == name);
  }

  private findMissingTable(database: DatabasesPageDataDatabase, name: string) {
    return _.find(database.missingTables, table => table.name == name);
  }
}

describe("Databases Page", function() {
  let driver: TestDriver;

  beforeEach(function() {
    driver = new TestDriver(createAdminUIStore(createMemoryHistory()));
  });

  afterEach(function() {
    fakeApi.restore();
  });

  it("starts in a pre-loading state", async function() {
    driver.assertProperties({
      loading: false,
      loaded: false,
      databases: [],
    });
  });

  it("makes a row for each database", async function() {
    fakeApi.stubDatabases({
      databases: ["system", "test"],
    });

    await driver.refreshDatabases();

    driver.assertProperties({
      loading: false,
      loaded: true,
      databases: [
        {
          loading: false,
          loaded: false,
          name: "system",
          sizeInBytes: 0,
          tableCount: 0,
          rangeCount: 0,
          missingTables: [],
        },
        {
          loading: false,
          loaded: false,
          name: "test",
          sizeInBytes: 0,
          tableCount: 0,
          rangeCount: 0,
          missingTables: [],
        },
      ],
    });
  });

  it("fills in database details", async function() {
    fakeApi.stubDatabases({
      databases: ["system", "test"],
    });

    fakeApi.stubDatabaseDetails("system", {
      table_names: ["foo", "bar"],
      stats: {
        missing_tables: [],
        range_count: new Long(3),
        approximate_disk_bytes: new Long(7168),
      },
    });

    fakeApi.stubDatabaseDetails("test", {
      table_names: ["widgets"],
      stats: {
        missing_tables: [],
        range_count: new Long(42),
        approximate_disk_bytes: new Long(1234),
      },
    });

    await driver.refreshDatabases();
    await driver.refreshDatabaseDetails("system");
    await driver.refreshDatabaseDetails("test");

    driver.assertDatabaseProperties("system", {
      loading: false,
      loaded: true,
      name: "system",
      sizeInBytes: 7168,
      tableCount: 2,
      rangeCount: 3,
      missingTables: [],
    });

    driver.assertDatabaseProperties("test", {
      loading: false,
      loaded: true,
      name: "test",
      sizeInBytes: 1234,
      tableCount: 1,
      rangeCount: 42,
      missingTables: [],
    });
  });

  describe("fallback cases", function() {
    describe("missing tables", function() {
      it("exposes them so the component can refresh them", async function() {
        fakeApi.stubDatabases({
          databases: ["system"],
        });

        fakeApi.stubDatabaseDetails("system", {
          table_names: ["foo", "bar"],
          stats: {
            missing_tables: [{ name: "bar" }],
            range_count: new Long(3),
            approximate_disk_bytes: new Long(7168),
          },
        });

        await driver.refreshDatabases();
        await driver.refreshDatabaseDetails("system");

        driver.assertDatabaseProperties("system", {
          loading: false,
          loaded: true,
          name: "system",
          sizeInBytes: 7168,
          tableCount: 2,
          rangeCount: 3,
          missingTables: [{ loading: false, name: "bar" }],
        });
      });

      it("merges available individual stats into the totals", async function() {
        fakeApi.stubDatabases({
          databases: ["system"],
        });

        fakeApi.stubDatabaseDetails("system", {
          table_names: ["foo", "bar"],
          stats: {
            missing_tables: [{ name: "bar" }],
            range_count: new Long(3),
            approximate_disk_bytes: new Long(7168),
          },
        });

        fakeApi.stubTableStats("system", "bar", {
          range_count: new Long(5),
          approximate_disk_bytes: new Long(1024),
        });

        await driver.refreshDatabases();
        await driver.refreshDatabaseDetails("system");
        await driver.refreshTableStats("system", "bar");

        driver.assertDatabaseProperties("system", {
          loading: false,
          loaded: true,
          name: "system",
          sizeInBytes: 8192,
          tableCount: 2,
          rangeCount: 8,
          missingTables: [],
        });
      });
    });

    describe("missing stats", function() {
      it("builds a list of missing tables", async function() {
        fakeApi.stubDatabases({
          databases: ["system"],
        });

        fakeApi.stubDatabaseDetails("system", {
          table_names: ["foo", "bar"],
        });

        await driver.refreshDatabases();
        await driver.refreshDatabaseDetails("system");

        driver.assertDatabaseProperties("system", {
          loading: false,
          loaded: true,
          name: "system",
          sizeInBytes: 0,
          tableCount: 2,
          rangeCount: 0,
          missingTables: [
            { loading: false, name: "foo" },
            { loading: false, name: "bar" },
          ],
        });
      });

      it("merges individual stats into the totals", async function() {
        fakeApi.stubDatabases({
          databases: ["system"],
        });

        fakeApi.stubDatabaseDetails("system", {
          table_names: ["foo", "bar"],
        });

        fakeApi.stubTableStats("system", "foo", {
          range_count: new Long(3),
          approximate_disk_bytes: new Long(7168),
        });

        fakeApi.stubTableStats("system", "bar", {
          range_count: new Long(5),
          approximate_disk_bytes: new Long(1024),
        });

        await driver.refreshDatabases();
        await driver.refreshDatabaseDetails("system");
        await driver.refreshTableStats("system", "foo");

        driver.assertDatabaseProperties("system", {
          loading: false,
          loaded: true,
          name: "system",
          sizeInBytes: 7168,
          tableCount: 2,
          rangeCount: 3,
          missingTables: [{ loading: false, name: "bar" }],
        });

        await driver.refreshTableStats("system", "bar");

        driver.assertDatabaseProperties("system", {
          loading: false,
          loaded: true,
          name: "system",
          sizeInBytes: 8192,
          tableCount: 2,
          rangeCount: 8,
          missingTables: [],
        });
      });
    });
  });
});
