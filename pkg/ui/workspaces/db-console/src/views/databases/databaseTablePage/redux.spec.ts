// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createMemoryHistory } from "history";
import Long from "long";
import { RouteComponentProps } from "react-router-dom";
import { bindActionCreators, Store } from "redux";
import {
  DatabaseTablePageActions,
  DatabaseTablePageData,
  DatabaseTablePageDataDetails,
  DatabaseTablePageDataStats,
  DatabaseTablePageIndexStats,
  util,
} from "@cockroachlabs/cluster-ui";

import { AdminUIState, createAdminUIStore } from "src/redux/state";
import { databaseNameAttr, tableNameAttr } from "src/util/constants";
import * as fakeApi from "src/util/fakeApi";
import { mapStateToProps, mapDispatchToProps } from "./redux";
import { makeTimestamp } from "src/views/databases/utils";

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

  assertProperties(
    expected: DatabaseTablePageData,
    compareTimestamps: boolean = true,
  ) {
    // Assert moments are equal if not in pre-loading state.
    if (compareTimestamps) {
      expect(this.properties().indexStats.lastReset).toEqual(
        expected.indexStats.lastReset,
      );
    }
    delete this.properties().indexStats.lastReset;
    delete expected.indexStats.lastReset;
    expect(this.properties()).toEqual(expected);
  }

  assertTableDetails(expected: DatabaseTablePageDataDetails) {
    expect(this.properties().details).toEqual(expected);
  }

  assertTableStats(expected: DatabaseTablePageDataStats) {
    expect(this.properties().stats).toEqual(expected);
  }

  assertIndexStats(
    expected: DatabaseTablePageIndexStats,
    compareTimestamps: boolean = true,
  ) {
    // Assert moments are equal if not in pre-loading state.
    if (compareTimestamps) {
      expect(expected.stats[0].lastUsed).toEqual(
        this.properties().indexStats.stats[0].lastUsed,
      );
    }
    delete this.properties().indexStats.stats[0].lastUsed;
    delete expected.stats[0].lastUsed;
    expect(expected.lastReset).toEqual(this.properties().indexStats.lastReset);
    delete this.properties().indexStats.lastReset;
    delete expected.lastReset;

    // Assert objects without moments are equal.
    expect(this.properties().indexStats).toEqual(expected);
  }

  async refreshSettings() {
    return this.actions.refreshSettings();
  }
  async refreshTableDetails() {
    return this.actions.refreshTableDetails(this.database, this.table);
  }

  async refreshTableStats() {
    return this.actions.refreshTableStats(this.database, this.table);
  }

  async refreshIndexStats() {
    return this.actions.refreshIndexStats(this.database, this.table);
  }
}

describe("Database Table Page", function () {
  let driver: TestDriver;

  beforeEach(function () {
    driver = new TestDriver(
      createAdminUIStore(createMemoryHistory()),
      "DATABASE",
      "TABLE",
    );
  });

  afterEach(function () {
    fakeApi.restore();
  });

  it("starts in a pre-loading state", async function () {
    fakeApi.stubClusterSettings({
      key_values: {
        "sql.stats.automatic_collection.enabled": { value: "true" },
      },
    });

    await driver.refreshSettings();

    driver.assertProperties(
      {
        databaseName: "DATABASE",
        name: "TABLE",
        showNodeRegionsSection: false,
        details: {
          loading: false,
          loaded: false,
          createStatement: "",
          replicaCount: 0,
          indexNames: [],
          grants: [],
          statsLastUpdated: null,
          livePercentage: 0,
          liveBytes: 0,
          totalBytes: 0,
        },
        automaticStatsCollectionEnabled: true,
        stats: {
          loading: false,
          loaded: false,
          sizeInBytes: 0,
          rangeCount: 0,
          nodesByRegionString: "",
        },
        indexStats: {
          loading: false,
          loaded: false,
          stats: [],
          lastReset: null,
        },
      },
      false,
    );
  });

  it("loads table details", async function () {
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
      stats_last_created_at: makeTimestamp("0001-01-01T00:00:00Z"),
      data_total_bytes: new Long(456789),
      data_live_bytes: new Long(12345),
      data_live_percentage: 2.0,
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
      statsLastUpdated: util.TimestampToMoment(
        makeTimestamp("0001-01-01T00:00:00Z"),
      ),
      livePercentage: 2.0,
      liveBytes: 12345,
      totalBytes: 456789,
    });
  });

  it("loads table stats", async function () {
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
      nodesByRegionString: "",
    });
  });

  it("loads index stats", async function () {
    fakeApi.stubIndexStats("DATABASE", "TABLE", {
      statistics: [
        {
          statistics: {
            key: {
              table_id: 15,
              index_id: 2,
            },
            stats: {
              total_read_count: new Long(2),
              last_read: makeTimestamp("2021-11-19T23:01:05.167627Z"),
              total_rows_read: new Long(0),
              total_write_count: new Long(0),
              last_write: makeTimestamp("0001-01-01T00:00:00Z"),
              total_rows_written: new Long(0),
            },
          },
          index_name: "jobs_status_created_idx",
          index_type: "secondary",
        },
        {
          statistics: {
            key: {
              table_id: 1,
              index_id: 2,
            },
            stats: {
              total_read_count: new Long(0),
              last_read: makeTimestamp("0001-01-01T00:00:00Z"),
              total_rows_read: new Long(0),
              total_write_count: new Long(0),
              last_write: makeTimestamp("0001-01-01T00:00:00Z"),
              total_rows_written: new Long(0),
            },
          },
          index_name: "index_no_reads_no_resets",
          index_type: "secondary",
          created_at: makeTimestamp("0001-01-01T00:00:00Z"),
        },
      ],
      last_reset: makeTimestamp("0001-01-01T00:00:00Z"),
    });

    await driver.refreshIndexStats();

    driver.assertIndexStats({
      loading: false,
      loaded: true,
      stats: [
        {
          indexName: "jobs_status_created_idx",
          totalReads: 2,
          lastUsed: util.TimestampToMoment(
            makeTimestamp("2021-11-19T23:01:05.167627Z"),
          ),
          lastUsedType: "read",
          indexRecommendations: [],
        },
        {
          indexName: "index_no_reads_no_resets",
          totalReads: 0,
          lastUsed: util.TimestampToMoment(
            makeTimestamp("0001-01-01T00:00:00Z"),
          ),
          lastUsedType: "created",
          indexRecommendations: [],
        },
      ],
      lastReset: util.TimestampToMoment(makeTimestamp("0001-01-01T00:00:00Z")),
    });
  });
});
