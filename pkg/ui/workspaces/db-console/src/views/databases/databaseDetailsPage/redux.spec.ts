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
import { databaseNameAttr } from "src/util/constants";
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
      moment(statsLastUpdated).isSame(expectedStatsLastUpdated) ||
        // Moments are null.
        (statsLastUpdated === expectedStatsLastUpdated &&
          statsLastUpdated === null),
    ).toBe(true);
  }

  assertTableRoles(name: string, expected: string[]) {
    expect(this.findTable(name).details.roles).toEqual(expected);
  }

  assertTableGrants(name: string, expected: string[]) {
    expect(this.findTable(name).details.grants).toEqual(expected);
  }

  async refreshDatabaseDetails() {
    return this.actions.refreshDatabaseDetails(this.database);
  }

  async refreshTableDetails(table: string) {
    return this.actions.refreshTableDetails(this.database, table);
  }

  private findTable(name: string) {
    return _.find(this.properties().tables, { name });
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
      lastError: null,
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
    });
  });

  it("makes a row for each table", async function () {
    fakeApi.stubSqlApiCall<clusterUiApi.DatabaseDetailsRow>(
      clusterUiApi.createDatabaseDetailsReq("things"),
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
      lastError: null,
      name: "things",
      search: null,
      filters: defaultFilters,
      nodeRegions: {},
      isTenant: false,
      showNodeRegionsColumn: false,
      viewMode: ViewMode.Tables,
      sortSettingTables: { ascending: true, columnTitle: "name" },
      sortSettingGrants: { ascending: true, columnTitle: "name" },
      tables: [
        {
          name: `"public"."foo"`,
          loading: false,
          loaded: false,
          lastError: undefined,
          details: {
            columnCount: 0,
            indexCount: 0,
            userCount: 0,
            roles: [],
            grants: [],
            statsLastUpdated: null,
            hasIndexRecommendations: false,
            livePercentage: 0,
            liveBytes: 0,
            totalBytes: 0,
            nodes: [],
            replicationSizeInBytes: 0,
            rangeCount: 0,
            nodesByRegionString: "",
          },
        },
        {
          name: `"public"."bar"`,
          loading: false,
          loaded: false,
          lastError: undefined,
          details: {
            columnCount: 0,
            indexCount: 0,
            userCount: 0,
            roles: [],
            grants: [],
            statsLastUpdated: null,
            hasIndexRecommendations: false,
            livePercentage: 0,
            totalBytes: 0,
            liveBytes: 0,
            nodes: [],
            replicationSizeInBytes: 0,
            rangeCount: 0,
            nodesByRegionString: "",
          },
        },
      ],
    });
  });

  it("loads table details", async function () {
    fakeApi.stubSqlApiCall<clusterUiApi.DatabaseDetailsRow>(
      clusterUiApi.createDatabaseDetailsReq("things"),
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
      clusterUiApi.createTableDetailsReq("things", `"public"."foo"`),
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
          rows: [{ replicas: [1, 2, 3] }],
        },
      ],
    );

    fakeApi.stubSqlApiCall<clusterUiApi.TableDetailsRow>(
      clusterUiApi.createTableDetailsReq("things", `"public"."bar"`),
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
          rows: [{ replicas: [1, 2, 3, 4, 5] }],
        },
      ],
    );

    await driver.refreshDatabaseDetails();
    await driver.refreshTableDetails(`"public"."foo"`);
    await driver.refreshTableDetails(`"public"."bar"`);

    driver.assertTableDetails(`"public"."foo"`, {
      name: `"public"."foo"`,
      loading: false,
      loaded: true,
      lastError: null,
      details: {
        columnCount: 3,
        indexCount: 2,
        userCount: 2,
        roles: ["admin", "public"],
        grants: ["CREATE", "SELECT"],
        statsLastUpdated: mockStatsLastCreatedTimestamp,
        hasIndexRecommendations: true,
        liveBytes: 200,
        totalBytes: 400,
        livePercentage: 0.5,
        replicationSizeInBytes: 100,
        rangeCount: 400,
        nodes: [1, 2, 3],
        nodesByRegionString: "undefined(n1,n2,n3)",
      },
    });

    driver.assertTableDetails(`"public"."bar"`, {
      name: `"public"."bar"`,
      loading: false,
      loaded: true,
      lastError: null,
      details: {
        columnCount: 2,
        indexCount: 4,
        userCount: 3,
        roles: ["root", "app", "data"],
        grants: ["ALL", "SELECT", "INSERT"],
        statsLastUpdated: null,
        hasIndexRecommendations: false,
        liveBytes: 100,
        totalBytes: 100,
        livePercentage: 1,
        replicationSizeInBytes: 10,
        rangeCount: 50,
        nodes: [1, 2, 3, 4, 5],
        nodesByRegionString: "undefined(n1,n2,n3,n4,n5)",
      },
    });
  });

  it("sorts roles meaningfully", async function () {
    fakeApi.stubSqlApiCall<clusterUiApi.DatabaseDetailsRow>(
      clusterUiApi.createDatabaseDetailsReq("things"),
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
      clusterUiApi.createTableDetailsReq("things", `"public"."foo"`),
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
      clusterUiApi.createDatabaseDetailsReq("things"),
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
      clusterUiApi.createTableDetailsReq("things", `"public"."foo"`),
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
