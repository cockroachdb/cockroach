// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  DatabasesPageActions,
  DatabasesPageData,
  DatabasesPageDataDatabase,
  defaultFilters,
  api as clusterUiApi,
} from "@cockroachlabs/cluster-ui";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { createMemoryHistory } from "history";
import find from "lodash/find";
import { bindActionCreators, Store } from "redux";

import { AdminUIState, createAdminUIStore } from "src/redux/state";
import { indexUnusedDuration } from "src/util/constants";
import * as fakeApi from "src/util/fakeApi";

import { mapDispatchToProps, mapStateToProps } from "./redux";

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

  async refreshDatabaseDetails(
    database: string,
    csIndexUnusedDuration: string,
  ) {
    return this.actions.refreshDatabaseDetails(database, csIndexUnusedDuration);
  }

  async refreshNodes() {
    return this.actions.refreshNodes();
  }

  async refreshSettings() {
    return this.actions.refreshSettings();
  }

  assertProperties(expected: DatabasesPageData) {
    expect(this.properties()).toEqual(expected);
  }

  assertDatabaseProperties(
    database: string,
    expected: DatabasesPageDataDatabase,
  ) {
    expect(this.findDatabase(database)).toEqual(expected);
  }

  private findDatabase(name: string) {
    return find(this.properties().databases, row => row.name === name);
  }
}

describe("Databases Page", function () {
  let driver: TestDriver;

  beforeEach(function () {
    driver = new TestDriver(createAdminUIStore(createMemoryHistory()));
  });

  afterEach(function () {
    fakeApi.restore();
  });

  it("starts in a pre-loading state", async function () {
    fakeApi.stubClusterSettings({
      key_values: {
        "sql.stats.automatic_collection.enabled": { value: "true" },
        version: { value: "1000023.1-8" },
      },
    });

    await driver.refreshSettings();

    driver.assertProperties({
      loading: false,
      loaded: false,
      requestError: undefined,
      queryError: undefined,
      databases: [],
      search: null,
      filters: defaultFilters,
      nodeRegions: {},
      isTenant: false,
      sortSetting: { ascending: true, columnTitle: "name" },
      automaticStatsCollectionEnabled: true,
      indexRecommendationsEnabled: true,
      showNodeRegionsColumn: false,
      csIndexUnusedDuration: indexUnusedDuration,
    });
  });

  it("makes a row for each database", async function () {
    // Mock out the fetch query to /databases
    fakeApi.stubSqlApiCall<clusterUiApi.DatabasesColumns>(
      clusterUiApi.databasesRequest,
      [
        {
          rows: [
            {
              database_name: "system",
            },
            {
              database_name: "test",
            },
          ],
        },
      ],
    );
    fakeApi.stubClusterSettings({
      key_values: {
        "sql.stats.automatic_collection.enabled": { value: "true" },
        version: { value: "1000023.1-8" },
      },
    });

    await driver.refreshDatabases();
    await driver.refreshSettings();

    driver.assertProperties({
      loading: false,
      loaded: true,
      requestError: null,
      queryError: undefined,
      databases: [
        {
          detailsLoading: false,
          detailsLoaded: false,
          spanStatsLoading: false,
          spanStatsLoaded: false,
          detailsRequestError: undefined,
          spanStatsRequestError: undefined,
          detailsQueryError: undefined,
          spanStatsQueryError: undefined,
          name: "system",
          nodes: [],
          spanStats: undefined,
          tables: undefined,
          nodesByRegionString: "",
          numIndexRecommendations: 0,
        },
        {
          detailsLoading: false,
          detailsLoaded: false,
          spanStatsLoading: false,
          spanStatsLoaded: false,
          detailsRequestError: undefined,
          spanStatsRequestError: undefined,
          detailsQueryError: undefined,
          spanStatsQueryError: undefined,
          name: "test",
          nodes: [],
          spanStats: undefined,
          tables: undefined,
          nodesByRegionString: "",
          numIndexRecommendations: 0,
        },
      ],
      search: null,
      filters: defaultFilters,
      nodeRegions: {},
      isTenant: false,
      sortSetting: { ascending: true, columnTitle: "name" },
      showNodeRegionsColumn: false,
      indexRecommendationsEnabled: true,
      csIndexUnusedDuration: indexUnusedDuration,
      automaticStatsCollectionEnabled: true,
    });
  });

  it("fills in database details and node/region info", async function () {
    const oldDate = new Date(2020, 12, 25, 0, 0, 0, 0);
    const regions = [
      "gcp-us-east1",
      "gcp-us-east1",
      "gcp-europe-west1",
      "gcp-us-east1",
      "gcp-europe-west2",
      "gcp-europe-west1",
    ];

    const nodes: cockroach.server.serverpb.INodeResponse[] = Array.from(
      Array(regions.length).keys(),
    ).map(node_id => {
      return {
        desc: {
          node_id: node_id + 1, // 1-index offset.
          locality: {
            tiers: [
              {
                key: "region",
                value: regions[node_id],
              },
            ],
          },
        },
        store_statuses: [{ desc: { store_id: node_id + 1 } }],
      };
    });

    fakeApi.stubNodesUI({
      nodes: nodes,
    });

    // Mock out the fetch query to /databases
    fakeApi.stubSqlApiCall<clusterUiApi.DatabasesColumns>(
      clusterUiApi.databasesRequest,
      [
        {
          rows: [
            {
              database_name: "test",
            },
          ],
        },
      ],
    );

    fakeApi.stubSqlApiCall<clusterUiApi.DatabaseDetailsRow>(
      clusterUiApi.createDatabaseDetailsReq({
        database: "test",
        csIndexUnusedDuration: indexUnusedDuration,
      }),
      [
        // Id
        { rows: [] },
        // Grants
        { rows: [] },
        // Tables
        {
          rows: [
            { table_schema: "public", table_name: "foo" },
            { table_schema: "public", table_name: "bar" },
          ],
        },
        // Regions and replicas
        {
          rows: [
            {
              store_ids: [1, 2, 3],
            },
            {
              store_ids: [1, 2, 3],
            },
            {
              store_ids: [1, 2, 3],
            },
          ],
        },
        // Index Usage Stats
        {
          rows: [
            // Generate drop index recommendation
            {
              last_read: oldDate.toISOString(),
              created_at: oldDate.toISOString(),
              unused_threshold: "1s",
            },
          ],
        },
        // Zone Config
        {
          rows: [],
        },
      ],
    );

    await driver.refreshNodes();
    await driver.refreshDatabases();
    await driver.refreshDatabaseDetails("test", indexUnusedDuration);

    driver.assertDatabaseProperties("test", {
      detailsLoading: false,
      detailsLoaded: true,
      spanStatsLoading: false,
      spanStatsLoaded: false,
      detailsRequestError: null,
      spanStatsRequestError: undefined,
      detailsQueryError: undefined,
      spanStatsQueryError: undefined,
      name: "test",
      nodes: [1, 2, 3],
      spanStats: undefined,
      tables: {
        tables: [
          {
            schema: "public",
            table: "foo",
            qualifiedNameWithSchemaAndTable: `"public"."foo"`,
          },
          {
            schema: "public",
            table: "bar",
            qualifiedNameWithSchemaAndTable: `"public"."bar"`,
          },
        ],
      },
      nodesByRegionString: "gcp-europe-west1(n3), gcp-us-east1(n1,n2)",
      numIndexRecommendations: 1,
    });
  });
});
