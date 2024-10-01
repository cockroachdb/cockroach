// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  DatabaseTablePageActions,
  DatabaseTablePageData,
  DatabaseTablePageDataDetails,
  DatabaseTablePageIndexStats,
  util,
  api as clusterUiApi,
} from "@cockroachlabs/cluster-ui";
import { createMemoryHistory } from "history";
import Long from "long";
import moment from "moment-timezone";
import { RouteComponentProps } from "react-router-dom";
import { bindActionCreators, Store } from "redux";

import { AdminUIState, createAdminUIStore } from "src/redux/state";
import {
  databaseNameAttr,
  indexUnusedDuration,
  tableNameAttr,
} from "src/util/constants";
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

  assertProperties(expected: DatabaseTablePageData, compareTimestamps = true) {
    // Assert moments are equal if not in pre-loading state.
    if (compareTimestamps) {
      expect(this.properties().indexStats.lastReset).toEqual(
        expected.indexStats.lastReset,
      );
    }
    expect(this.properties()).toEqual(expected);
  }

  assertTableDetails(expected: DatabaseTablePageDataDetails) {
    // We destructure the expected and actual payloads to extract the field
    // with Moment type. Moment types cannot be compared using toEqual or toBe,
    // we need to use moment's isSame function.
    const { statsLastUpdated, ...rest } = this.properties().details;
    const { statsLastUpdated: expectedStatsLastUpdated, ...expectedRest } =
      expected;
    expect(rest).toEqual(expectedRest);
    expect(
      // Moments are the same
      moment(statsLastUpdated.stats_last_created_at).isSame(
        expectedStatsLastUpdated.stats_last_created_at,
      ) ||
        // Moments are null.
        (statsLastUpdated.stats_last_created_at ===
          expectedStatsLastUpdated.stats_last_created_at &&
          statsLastUpdated.stats_last_created_at === null),
    ).toBe(true);
  }

  assertIndexStats(
    expected: DatabaseTablePageIndexStats,
    compareTimestamps = true,
  ) {
    // Assert moments are equal if not in pre-loading state.
    if (compareTimestamps) {
      expect(
        expected.stats[0].lastUsed.isSame(
          this.properties().indexStats.stats[0].lastUsed,
        ),
      ).toEqual(true);
    }
    const indexStats = this.properties().indexStats;
    expect(expected.lastReset.isSame(indexStats.lastReset)).toEqual(true);
    // Remove moment objects.
    indexStats.stats.map(stat => {
      delete stat.lastUsed;
    });
    expected.stats.map(stat => {
      delete stat.lastUsed;
    });
    delete indexStats.lastReset;
    delete expected.lastReset;
    // Assert objects without moments are equal.
    expect(indexStats).toEqual(expected);
  }

  async refreshSettings() {
    return this.actions.refreshSettings();
  }
  async refreshTableDetails() {
    return this.actions.refreshTableDetails(
      this.database,
      this.table,
      indexUnusedDuration,
    );
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
        version: { value: "23.1.0" },
      },
    });

    await driver.refreshSettings();

    driver.assertProperties({
      databaseName: "DATABASE",
      name: "TABLE",
      schemaName: "",
      showNodeRegionsSection: false,
      details: {
        loading: false,
        loaded: false,
        requestError: undefined,
        queryError: undefined,
        createStatement: undefined,
        replicaData: undefined,
        spanStats: undefined,
        indexData: undefined,
        grants: {
          all: [],
          error: undefined,
        },
        statsLastUpdated: undefined,
        nodesByRegionString: "",
      },
      automaticStatsCollectionEnabled: true,
      indexUsageStatsEnabled: true,
      showIndexRecommendations: true,
      csIndexUnusedDuration: indexUnusedDuration,
      hasAdminRole: false,
      indexStats: {
        loading: false,
        loaded: false,
        lastError: undefined,
        stats: [],
        lastReset: util.minDate,
      },
      isTenant: false,
    });
  });

  it("loads table details", async function () {
    const mockStatsLastCreatedTimestamp = moment();

    fakeApi.stubSqlApiCall<clusterUiApi.TableDetailsRow>(
      clusterUiApi.createTableDetailsReq(
        "DATABASE",
        "TABLE",
        indexUnusedDuration,
      ),
      [
        // Table ID query
        { rows: [{ table_id: "1" }] },
        // Table grants query
        {
          rows: [
            { user: "admin", privileges: ["CREATE", "DROP"] },
            { user: "public", privileges: ["SELECT"] },
          ],
        },
        // Table schema details query
        {
          rows: [
            {
              columns: ["colA", "colB", "c"],
              indexes: ["primary", "anotha", "one"],
            },
          ],
        },
        // Table create statement query
        { rows: [{ create_statement: "CREATE TABLE foo" }] },
        // Table zone config statement query
        {},
        // Table heuristics query
        { rows: [{ stats_last_created_at: mockStatsLastCreatedTimestamp }] },
        // Table span stats query
        {
          rows: [
            {
              approximate_disk_bytes: 23,
              live_bytes: 45,
              total_bytes: 45,
              range_count: 56,
              live_percentage: 1,
            },
          ],
        },
        // Table index usage statistics query
        {
          rows: [
            {
              last_read: new Date().toISOString(),
              created_at: new Date().toISOString(),
              unused_threshold: "1m",
            },
          ],
        },
        // Table zone config query
        {},
        // Table replicas query
        {
          rows: [{ store_ids: [1, 2, 3, 4, 5], replica_count: 10 }],
        },
      ],
    );

    await driver.refreshTableDetails();

    driver.assertTableDetails({
      loading: false,
      loaded: true,
      requestError: null,
      queryError: undefined,
      createStatement: { create_statement: "CREATE TABLE foo" },
      replicaData: { storeIDs: [1, 2, 3, 4, 5], replicaCount: 10 },
      spanStats: {
        approximate_disk_bytes: 23,
        live_bytes: 45,
        total_bytes: 45,
        range_count: 56,
        live_percentage: 1,
      },
      indexData: {
        columns: ["colA", "colB", "c"],
        indexes: ["primary", "anotha", "one"],
      },
      grants: {
        all: [
          { user: "admin", privileges: ["CREATE", "DROP"] },
          { user: "public", privileges: ["SELECT"] },
        ],
        error: undefined,
      },
      statsLastUpdated: {
        stats_last_created_at: mockStatsLastCreatedTimestamp,
      },
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
              last_read: util.stringToTimestamp("2021-11-19T23:01:05.167627Z"),
              total_rows_read: new Long(0),
              total_write_count: new Long(0),
              last_write: util.stringToTimestamp("0001-01-01T00:00:00Z"),
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
              last_read: util.stringToTimestamp("0001-01-01T00:00:00Z"),
              total_rows_read: new Long(0),
              total_write_count: new Long(0),
              last_write: util.stringToTimestamp("0001-01-01T00:00:00Z"),
              total_rows_written: new Long(0),
            },
          },
          index_name: "index_no_reads_no_resets",
          index_type: "secondary",
          created_at: util.stringToTimestamp("0001-01-01T00:00:00Z"),
        },
      ],
      last_reset: util.stringToTimestamp("0001-01-01T00:00:00Z"),
    });

    await driver.refreshIndexStats();

    driver.assertIndexStats({
      loading: false,
      loaded: true,
      lastError: null,
      stats: [
        {
          indexName: "jobs_status_created_idx",
          totalReads: 2,
          lastUsed: util.TimestampToMoment(
            util.stringToTimestamp("2021-11-19T23:01:05.167627Z"),
          ),
          lastUsedType: "read",
          indexRecommendations: [],
        },
        {
          indexName: "index_no_reads_no_resets",
          totalReads: 0,
          lastUsed: util.minDate,
          lastUsedType: "created",
          indexRecommendations: [],
        },
      ],
      lastReset: util.minDate,
    });
  });
});
