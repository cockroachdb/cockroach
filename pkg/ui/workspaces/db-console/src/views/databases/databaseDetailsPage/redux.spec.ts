// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createMemoryHistory } from "history";
import { RouteComponentProps } from "react-router-dom";
import { bindActionCreators, Store } from "redux";
import {
  DatabaseDetailsPageActions,
  DatabaseDetailsPageData,
  DatabaseDetailsPageDataTable,
  defaultFilters,
  ViewMode,
  api as clusterUiApi,
} from "@cockroachlabs/cluster-ui";

import { AdminUIState, createAdminUIStore } from "src/redux/state";
import { databaseNameAttr, indexUnusedDuration } from "src/util/constants";
import * as fakeApi from "src/util/fakeApi";
import { mapStateToProps, mapDispatchToProps } from "./redux";
import moment from "moment-timezone";

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
    expect(this.properties()).toEqual(expected);
  }

  assertTableDetails(name: string, expected: DatabaseDetailsPageDataTable) {
    // We destructure the expected and actual payloads to extract the field
    // with Moment type. Moment types cannot be compared using toEqual or toBe,
    // we need to use moment's isSame function.
    const {
      details: { statsLastUpdated, ...restDetails },
      ...table
    } = this.findTable(name);
    const {
      details: {
        statsLastUpdated: expectedStatsLastUpdated,
        ...expectedRestDetails
      },
      ...expectedTable
    } = expected;
    // Expect table data to be equal (name/loading/loaded/lastError).
    expect(table).toEqual(expectedTable);
    // Expect remaining details fields to be equal.
    expect(restDetails).toEqual(expectedRestDetails);
    // Expect Moment type field to be equal.
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

  assertTableRoles(name: string, expected: string[]) {
    expect(this.findTable(name).details.grants.roles).toEqual(expected);
  }

  assertTableGrants(name: string, expected: string[]) {
    expect(this.findTable(name).details.grants.privileges).toEqual(expected);
  }

  async refreshDatabaseDetails() {
    return this.actions.refreshDatabaseDetails(
      this.database,
      indexUnusedDuration,
    );
  }

  async refreshTableDetails(table: string) {
    return this.actions.refreshTableDetails(
      this.database,
      table,
      indexUnusedDuration,
    );
  }

  async refreshNodes() {
    return this.actions.refreshNodes();
  }

  private findTable(name: string) {
    return this.properties().tables.find(
      t => t.name.qualifiedNameWithSchemaAndTable === name,
    );
  }
}

describe("Database Details Page", function () {
  let driver: TestDriver;

  beforeEach(function () {
    driver = new TestDriver(
      createAdminUIStore(createMemoryHistory()),
      "things",
    );
  });

  afterEach(function () {
    fakeApi.restore();
  });

  it("starts in a pre-loading state", function () {
    driver.assertProperties({
      loading: false,
      loaded: false,
      requestError: undefined,
      queryError: undefined,
      name: "things",
      search: null,
      filters: defaultFilters,
      nodeRegions: {},
      isTenant: false,
      showNodeRegionsColumn: false,
      viewMode: ViewMode.Tables,
      sortSettingTables: { ascending: true, columnTitle: "name" },
      sortSettingGrants: { ascending: true, columnTitle: "name" },
      tables: [],
      showIndexRecommendations: false,
      csIndexUnusedDuration: indexUnusedDuration,
    });
  });

  it("makes a row for each table", async function () {
    fakeApi.stubSqlApiCall<clusterUiApi.DatabaseDetailsRow>(
      clusterUiApi.createDatabaseDetailsReq({
        database: "things",
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
      ],
    );

    await driver.refreshDatabaseDetails();
    driver.assertProperties({
      loading: false,
      loaded: true,
      requestError: null,
      queryError: undefined,
      name: "things",
      search: null,
      filters: defaultFilters,
      nodeRegions: {},
      isTenant: false,
      showNodeRegionsColumn: false,
      showIndexRecommendations: false,
      csIndexUnusedDuration: indexUnusedDuration,
      viewMode: ViewMode.Tables,
      sortSettingTables: { ascending: true, columnTitle: "name" },
      sortSettingGrants: { ascending: true, columnTitle: "name" },
      tables: [
        {
          name: {
            schema: "public",
            table: "foo",
            qualifiedNameWithSchemaAndTable: `"public"."foo"`,
          },
          qualifiedDisplayName: `public.foo`,
          loading: false,
          loaded: false,
          requestError: undefined,
          queryError: undefined,
          details: {
            schemaDetails: undefined,
            grants: {
              error: undefined,
              roles: [],
              privileges: [],
            },
            statsLastUpdated: undefined,
            indexStatRecs: undefined,
            spanStats: undefined,
            nodes: [],
            nodesByRegionString: "",
          },
        },
        {
          name: {
            schema: "public",
            table: "bar",
            qualifiedNameWithSchemaAndTable: `"public"."bar"`,
          },
          qualifiedDisplayName: `public.bar`,
          loading: false,
          loaded: false,
          requestError: undefined,
          queryError: undefined,
          details: {
            schemaDetails: undefined,
            grants: {
              error: undefined,
              roles: [],
              privileges: [],
            },
            statsLastUpdated: undefined,
            indexStatRecs: undefined,
            spanStats: undefined,
            nodes: [],
            nodesByRegionString: "",
          },
        },
      ],
    });
  });

  it("loads table details", async function () {
    fakeApi.stubSqlApiCall<clusterUiApi.DatabaseDetailsRow>(
      clusterUiApi.createDatabaseDetailsReq({
        database: "things",
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
      ],
    );
    const mockStatsLastCreatedTimestamp = moment();

    fakeApi.stubSqlApiCall<clusterUiApi.TableDetailsRow>(
      clusterUiApi.createTableDetailsReq(
        "things",
        `"public"."foo"`,
        indexUnusedDuration,
      ),
      [
        // Table ID query
        { rows: [{ table_id: "1" }] },
        // Table grants query
        {
          rows: [
            { user: "admin", privileges: ["CREATE"] },
            { user: "public", privileges: ["SELECT"] },
          ],
        },
        // Table schema details query
        { rows: [{ columns: ["a", "b", "c"], indexes: ["d", "e"] }] },
        // Table create statement query
        {},
        // Table zone config statement query
        {},
        // Table heuristics query
        { rows: [{ stats_last_created_at: mockStatsLastCreatedTimestamp }] },
        // Table span stats query
        {
          rows: [
            {
              approximate_disk_bytes: 100,
              live_bytes: 200,
              total_bytes: 400,
              range_count: 400,
              live_percentage: 0.5,
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
          rows: [{ store_ids: [1, 2, 3], replica_count: 5 }],
        },
      ],
    );

    fakeApi.stubSqlApiCall<clusterUiApi.TableDetailsRow>(
      clusterUiApi.createTableDetailsReq(
        "things",
        `"public"."bar"`,
        indexUnusedDuration,
      ),
      [
        // Table ID query
        { rows: [{ table_id: "2" }] },
        // Table grants query
        {
          rows: [
            { user: "root", privileges: ["ALL"] },
            { user: "app", privileges: ["INSERT"] },
            { user: "data", privileges: ["SELECT"] },
          ],
        },
        // Table schema details query
        { rows: [{ columns: ["a", "b"], indexes: ["c", "d", "e", "f"] }] },
        // Table create statement query
        {},
        // Table zone config statement query
        {},
        // Table heuristics query
        { rows: [{ stats_last_created_at: null }] },
        // Table span stats query
        {
          rows: [
            {
              approximate_disk_bytes: 10,
              live_bytes: 100,
              total_bytes: 100,
              range_count: 50,
              live_percentage: 1,
            },
          ],
        },
        // Table index usage statistics query
        {
          rows: [],
        },
        // Table zone config query
        {},
        // Table replicas query
        {
          rows: [{ store_ids: [1, 2, 3, 4, 5], replica_count: 5 }],
        },
      ],
    );

    fakeApi.stubNodesUI({
      nodes: [...Array(5).keys()].map(node_id => {
        return {
          desc: {
            node_id: node_id + 1, // 1-index offset.
            locality: {
              tiers: [
                {
                  key: "region",
                  value: "gcp-us-east1",
                },
              ],
            },
          },
          store_statuses: [{ desc: { store_id: node_id + 1 } }],
        };
      }),
    });

    await driver.refreshDatabaseDetails();
    await driver.refreshTableDetails(`"public"."foo"`);
    await driver.refreshTableDetails(`"public"."bar"`);
    await driver.refreshNodes();

    driver.assertTableDetails(`"public"."foo"`, {
      name: {
        schema: "public",
        table: "foo",
        qualifiedNameWithSchemaAndTable: `"public"."foo"`,
      },
      qualifiedDisplayName: `public.foo`,
      loading: false,
      loaded: true,
      requestError: null,
      queryError: undefined,
      details: {
        schemaDetails: {
          columns: ["a", "b", "c"],
          indexes: ["d", "e"],
        },
        grants: {
          error: undefined,
          roles: ["admin", "public"],
          privileges: ["CREATE", "SELECT"],
        },
        statsLastUpdated: {
          stats_last_created_at: mockStatsLastCreatedTimestamp,
        },
        indexStatRecs: { has_index_recommendations: true },
        spanStats: {
          approximate_disk_bytes: 100,
          live_bytes: 200,
          total_bytes: 400,
          range_count: 400,
          live_percentage: 0.5,
        },
        nodes: [1, 2, 3],
        nodesByRegionString: "gcp-us-east1(n1,n2,n3)",
      },
    });

    driver.assertTableDetails(`"public"."bar"`, {
      name: {
        schema: "public",
        table: "bar",
        qualifiedNameWithSchemaAndTable: `"public"."bar"`,
      },
      qualifiedDisplayName: `public.bar`,
      loading: false,
      loaded: true,
      requestError: null,
      queryError: undefined,
      details: {
        schemaDetails: {
          columns: ["a", "b"],
          indexes: ["c", "d", "e", "f"],
        },
        grants: {
          error: undefined,
          roles: ["root", "app", "data"],
          privileges: ["ALL", "SELECT", "INSERT"],
        },
        statsLastUpdated: { stats_last_created_at: null },
        indexStatRecs: { has_index_recommendations: false },
        spanStats: {
          live_percentage: 1,
          live_bytes: 100,
          total_bytes: 100,
          range_count: 50,
          approximate_disk_bytes: 10,
        },
        nodes: [1, 2, 3, 4, 5],
        nodesByRegionString: "gcp-us-east1(n1,n2,n3,n4,n5)",
      },
    });
  });

  it("sorts roles meaningfully", async function () {
    fakeApi.stubSqlApiCall<clusterUiApi.DatabaseDetailsRow>(
      clusterUiApi.createDatabaseDetailsReq({
        database: "things",
        csIndexUnusedDuration: indexUnusedDuration,
      }),
      [
        // Id
        { rows: [] },
        // Grants
        { rows: [] },
        // Tables
        {
          rows: [{ table_schema: "public", table_name: "foo" }],
        },
      ],
    );

    fakeApi.stubSqlApiCall<clusterUiApi.TableDetailsRow>(
      clusterUiApi.createTableDetailsReq(
        "things",
        `"public"."foo"`,
        indexUnusedDuration,
      ),
      [
        // Table ID query
        {},
        // Table grants query
        {
          rows: [
            { user: "bzuckercorn", privileges: ["ALL"] },
            { user: "bloblaw", privileges: ["ALL"] },
            { user: "jwweatherman", privileges: ["ALL"] },
            { user: "admin", privileges: ["ALL"] },
            { user: "public", privileges: ["ALL"] },
            { user: "root", privileges: ["ALL"] },
          ],
        },
      ],
    );

    await driver.refreshDatabaseDetails();
    await driver.refreshTableDetails(`"public"."foo"`);

    driver.assertTableRoles(`"public"."foo"`, [
      "root",
      "admin",
      "public",
      "bloblaw",
      "bzuckercorn",
      "jwweatherman",
    ]);
  });

  it("sorts grants meaningfully", async function () {
    fakeApi.stubSqlApiCall<clusterUiApi.DatabaseDetailsRow>(
      clusterUiApi.createDatabaseDetailsReq({
        database: "things",
        csIndexUnusedDuration: indexUnusedDuration,
      }),
      [
        // Id
        { rows: [] },
        // Grants
        { rows: [] },
        // Tables
        {
          rows: [{ table_schema: "public", table_name: "foo" }],
        },
      ],
    );

    fakeApi.stubSqlApiCall<clusterUiApi.TableDetailsRow>(
      clusterUiApi.createTableDetailsReq(
        "things",
        `"public"."foo"`,
        indexUnusedDuration,
      ),
      [
        // Table ID query
        {},
        // Table grants query
        {
          rows: [
            {
              user: "admin",
              privileges: ["ALL", "CREATE", "DELETE", "DROP", "GRANT"],
            },
            {
              user: "public",
              privileges: ["DROP", "GRANT", "INSERT", "SELECT", "UPDATE"],
            },
          ],
        },
      ],
    );

    await driver.refreshDatabaseDetails();
    await driver.refreshTableDetails(`"public"."foo"`);

    driver.assertTableGrants(`"public"."foo"`, [
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
});
