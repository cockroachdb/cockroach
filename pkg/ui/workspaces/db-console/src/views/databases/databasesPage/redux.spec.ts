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
import _ from "lodash";
import { bindActionCreators, Store } from "redux";
import {
  DatabasesPageActions,
  DatabasesPageData,
  DatabasesPageDataDatabase,
  defaultFilters,
  api as clusterUiApi,
} from "@cockroachlabs/cluster-ui";

import { AdminUIState, createAdminUIStore } from "src/redux/state";
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

  async refreshDatabaseDetails(database: string) {
    return this.actions.refreshDatabaseDetails(database);
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
    return _.find(this.properties().databases, row => row.name == name);
  }
}

describe.only("Databases Page", function () {
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
      },
    });

    await driver.refreshSettings();

    driver.assertProperties({
      loading: false,
      loaded: false,
      lastError: undefined,
      databases: [],
      search: null,
      filters: defaultFilters,
      nodeRegions: {},
      isTenant: false,
      sortSetting: { ascending: true, columnTitle: "name" },
      automaticStatsCollectionEnabled: true,
      showNodeRegionsColumn: false,
    });
  });

  it("makes a row for each database", async function () {
    fakeApi.stubDatabases(["system", "test"]);
    fakeApi.stubClusterSettings({
      key_values: {
        "sql.stats.automatic_collection.enabled": { value: "true" },
      },
    });

    await driver.refreshDatabases();
    await driver.refreshSettings();

    driver.assertProperties({
      loading: false,
      loaded: true,
      lastError: null,
      databases: [
        {
          loading: false,
          loaded: false,
          lastError: undefined,
          name: "system",
          nodes: [],
          sizeInBytes: 0,
          tableCount: 0,
          rangeCount: 0,
          nodesByRegionString: "",
          numIndexRecommendations: 0,
        },
        {
          loading: false,
          loaded: false,
          lastError: undefined,
          name: "test",
          nodes: [],
          sizeInBytes: 0,
          tableCount: 0,
          rangeCount: 0,
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
      automaticStatsCollectionEnabled: true,
    });
  });

  it("fills in database details and node/region info", async function () {
    const regions = [
      "gcp-us-east1",
      "gcp-us-east1",
      "gcp-europe-west1",
      "gcp-us-east1",
      "gcp-europe-west2",
      "gcp-europe-west1",
    ];

    const nodes = Array.from(Array(regions.length).keys()).map(node_id => {
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
      };
    });

    fakeApi.stubNodesUI({
      nodes: nodes,
    });

    fakeApi.stubDatabases(["systeM", "test"]);

    fakeApi.stubSqlApiCall<clusterUiApi.DatabaseDetailsRow>(
      clusterUiApi.createDatabaseDetailsReq("systeM"),
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
        // Ranges
        {
          rows: fakeApi.createMockDatabaseRangesForTable(3, "systeM", "foo", 3),
        },
      ],
    );

    await driver.refreshNodes();
    await driver.refreshDatabases();
    await driver.refreshDatabaseDetails("systeM");

    driver.assertDatabaseProperties("systeM", {
      loading: false,
      loaded: true,
      lastError: null,
      name: "systeM",
      nodes: [1, 2, 3],
      sizeInBytes: 0, // TODO(thomas): fix when we have disk size
      tableCount: 2,
      rangeCount: 3,
      nodesByRegionString: "gcp-us-east1(n1,n2), gcp-europe-west1(n3)",
      numIndexRecommendations: 0,
    });

    fakeApi.stubSqlApiCall<clusterUiApi.DatabaseDetailsRow>(
      clusterUiApi.createDatabaseDetailsReq("test"),
      [
        // Id
        { rows: [] },
        // Grants
        { rows: [] },
        // Tables
        {
          rows: [{ table_schema: "public", table_name: "widgets" }],
        },
        // Ranges
        {
          rows: fakeApi.createMockDatabaseRangesForTable(4, "test", "foo", 6),
        },
      ],
    );

    await driver.refreshDatabaseDetails("test");

    driver.assertDatabaseProperties("test", {
      loading: false,
      loaded: true,
      lastError: null,
      name: "test",
      nodes: [1, 2, 3, 4, 5, 6],
      sizeInBytes: 0, // TODO(thomas): fix when we have disk size
      tableCount: 1,
      rangeCount: 4,
      nodesByRegionString:
        "gcp-us-east1(n1,n2,n4), gcp-europe-west2(n5), gcp-europe-west1(n3,n6)",
      numIndexRecommendations: 0,
    });
  });
});
