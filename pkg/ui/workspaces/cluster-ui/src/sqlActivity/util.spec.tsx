// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import Long from "long";

import { mockStmtStats, Stmt } from "src/api/testUtils";
import { Filters } from "src/queryFilter/filter";
import {
  convertRawStmtsToAggregateStatistics,
  filterStatementsData,
  getAppsFromStmtsResponse,
} from "src/sqlActivity/util";

import { INTERNAL_APP_NAME_PREFIX, unset } from "../util";

describe("filterStatementsData", () => {
  function filterAndCheckStmts(
    stmtsRaw: Stmt[],
    filters: Filters,
    searchString: string | null,
    expectedStmtIDs: number[],
  ) {
    const statements = convertRawStmtsToAggregateStatistics(stmtsRaw);

    const filteredStmts = filterStatementsData(
      filters,
      searchString,
      statements,
      false,
    );

    expect(filteredStmts.length).toEqual(expectedStmtIDs.length);

    expect(
      filteredStmts.map(stmt => Number(stmt.aggregatedFingerprintID)).sort(),
    ).toEqual(expectedStmtIDs);
  }

  it("should filter out statements not matching search string", () => {
    const stmtsRaw = [
      {
        id: 1,
        query: "rhinos",
      },
      {
        id: 2,
        query: "giraffes", // Should match.
      },
      {
        id: 3,
        query: "giraffes are cool", // Should match.
      },
      {
        id: 4,
        query: "elephants cannot jump",
      },
    ].map(stmt =>
      mockStmtStats({
        id: Long.fromInt(stmt.id),
        key: { key_data: { query: stmt.query, query_summary: stmt.query } },
      }),
    );

    const expectedIDs = [2, 3];
    filterAndCheckStmts(stmtsRaw, {}, "giraffe", expectedIDs);
  });

  it("should show internal statements when no app filters are applied", () => {
    const stmtsRaw = [
      { id: 1, app: "hello" },
      { id: 2, app: "$ internal hello" },
      { id: 3, app: "$ internal app" },
      { id: 4, app: "world" },
      { id: 5, app: "great" },
    ].map(stmt =>
      mockStmtStats({
        id: Long.fromInt(stmt.id),
        key: { key_data: { app: stmt.app } },
      }),
    );

    const filters: Filters = {};
    const expected = [1, 2, 3, 4, 5];
    filterAndCheckStmts(stmtsRaw, filters, null, expected);
  });

  it.each([
    {
      stmts: [
        {
          id: 1,
          app: "hello world",
        },
        {
          id: 2,
          app: "giraffe", // Should match.
        },
        {
          id: 3,
          app: "giraffes are cool", // Should NOT match. App names must match exactly.
        },
        {
          id: 4,
          app: "elephants cannot jump",
        },
        {
          id: 5,
          app: "gira",
        },
      ],
      appName: "giraffe",
      expectedIDs: [2],
    },
    {
      stmts: [
        {
          id: 1,
          app: "",
        },
        {
          id: 2,
          app: "",
        },
        {
          id: 3,
          app: "hello",
        },
        {
          id: 4,
          app: "",
        },
        {
          id: 5,
          app: "",
        },
      ],
      appName: unset,
      expectedIDs: [1, 2, 4, 5],
    },
    {
      stmts: [
        {
          id: 1,
          app: "hello world",
        },
        {
          id: 2,
          app: "giraffes", // Should NOT match.
        },
        {
          id: 3,
          app: "giraffe", // Should match.
        },
        {
          id: 4,
          app: "elephant", // Should match.
        },
        {
          id: 5,
          app: "gira", // Should NOT match. Exact matches only.
        },
        {
          id: 6,
          app: "giraffe", // Should match.
        },
        {
          id: 7,
          app: "elephants cannot jump", // Should not match.
        },
      ],
      appName: "giraffe, elephant",
      expectedIDs: [3, 4, 6],
    },
    {
      stmts: [
        {
          id: 1,
          app: "hello world",
        },
        {
          id: 2,
          app: "giraffes", // Should NOT match.
        },
        {
          id: 3,
          app: "giraffe", // Should match.
        },
        {
          id: 4,
          app: "elephant", // Should match.
        },
        {
          id: 5,
          app: "gira", // Should NOT match. Exact matches only.
        },
        {
          id: 6,
          app: "giraffe", // Should match.
        },
        {
          id: 7,
          app: "elephants cannot jump", // Should not match.
        },
        {
          id: 8,
          app: "$ internal-my-app", // Should not match.
        },
      ],
      appName: "aaaaaaaaaaaaaaaaaaaaaaaa",
      expectedIDs: [],
    },
    {
      stmts: [
        {
          id: 1,
          // Should match because it starts with INTERNAL_APP_NAME_PREFIX.
          app: INTERNAL_APP_NAME_PREFIX + "-my-app",
        },
        {
          id: 2,
          // Should match because it starts with INTERNAL_APP_NAME_PREFIX.
          app: INTERNAL_APP_NAME_PREFIX,
        },
        {
          id: 3,
          // Should not match.
          app: "myApp" + INTERNAL_APP_NAME_PREFIX,
        },
        {
          id: 4,
          // Should match because it starts with INTERNAL_APP_NAME_PREFIX.
          app: INTERNAL_APP_NAME_PREFIX + "myApp",
        },
      ],
      appName: INTERNAL_APP_NAME_PREFIX + ",aaaaaaaaaaaaa",
      expectedIDs: [1, 2, 4],
    },
  ])("should filter out statements not matching filter apps", tc => {
    const stmtsRaw = tc.stmts.map(stmt =>
      mockStmtStats({
        id: Long.fromInt(stmt.id),
        key: { key_data: { app: stmt.app } },
      }),
    );

    const filters: Filters = {
      app: tc.appName,
    };
    filterAndCheckStmts(stmtsRaw, filters, null, tc.expectedIDs);
  });

  it("should filter out statements not matching sql type", () => {
    const filters: Filters = {
      sqlType: "DDL, DML",
    };

    const stmtsRaw = [
      {
        id: 1,
        type: "TypeDML",
      },
      {
        id: 2,
        type: "TypeDDL",
      },
      {
        id: 3,
        type: "TypeDCL",
      },
      {
        id: 4,
        type: "TypeDCL",
      },
      {
        id: 5,
        type: "TypeTCL",
      },
      {
        id: 6,
        type: "TypeDDL",
      },
    ].map(stmt =>
      mockStmtStats({
        id: Long.fromInt(stmt.id),
        stats: { sql_type: stmt.type },
      }),
    );

    const expectedIDs = [1, 2, 6];
    filterAndCheckStmts(stmtsRaw, filters, null, expectedIDs);
  });

  it("should filter out statements not matching database (exact match)", () => {
    const filters: Filters = {
      database: "cockroach, coolDB",
    };

    const stmtsRaw = [
      {
        id: 1,
        db: "roachie",
      },
      {
        id: 2,
        db: "uncoolDB",
      },
      {
        id: 3,
        db: "coolDB",
      },
      {
        id: 4,
        db: "cockroach",
      },
      {
        id: 5,
        db: "cockroach",
      },
      {
        id: 6,
        db: "myDB",
      },
    ].map(stmt =>
      mockStmtStats({
        id: Long.fromInt(stmt.id),
        key: { key_data: { database: stmt.db } },
      }),
    );

    const expectedIDs = [3, 4, 5];
    filterAndCheckStmts(stmtsRaw, filters, null, expectedIDs);
  });

  it("should filter out statements with svc_lat less than filter time value", () => {
    // Create stmts with ids matching service_lat in seconds.
    // We will filter out the first half of these stmts.
    const stmtsRaw = [1, 2, 3, 4, 5, 6].map(stmtID =>
      mockStmtStats({
        id: Long.fromInt(stmtID),
        stats: { service_lat: { mean: stmtID, squared_diffs: 0 } },
      }),
    );

    const expectedIDs = [4, 5, 6];

    [
      {
        timeNumber: "4",
        timeUnit: "seconds",
      },
      {
        timeNumber: "0.06",
        timeUnit: "minutes",
      },
      {
        timeNumber: "4000",
        timeUnit: "milliseconds",
      },
    ].forEach(filter => {
      filterAndCheckStmts(stmtsRaw, filter, null, expectedIDs);
    });
  });

  it("should filter out statements not matching ALL filters", () => {
    const filters: Filters = {
      database: "coolestDB",
      app: "coolestApp, " + INTERNAL_APP_NAME_PREFIX,
      timeNumber: "1",
      timeUnit: "seconds",
    };

    const searchTerm = "coolestQuery";

    const stmtsRaw = [
      {
        id: 1,
        db: "lameDB",
        app: "coolestApp",
        svcLatSecs: 1,
        query: "coolestQuery",
      },
      {
        id: 2,
        db: "coolestDb",
        app: "lameApp",
        svcLatSecs: 1,
        query: "coolestQuery",
      },
      {
        id: 3,
        db: "coolestDb",
        app: "coolestApp",
        svcLatSecs: 0.5,
        query: "coolestQuery",
      },
      {
        id: 4,
        db: "coolestDb",
        app: "coolestApp",
        svcLatSecs: 1,
        query: "select lame, query",
      },
      {
        id: 5,
        db: "coolestDB",
        app: "coolestApp",
        svcLatSecs: 1,
        query: `select ${searchTerm}`,
      },
      {
        id: 6,
        db: "coolestDB",
        app: INTERNAL_APP_NAME_PREFIX + "-cool-app",
        svcLatSecs: 1,
        query: `select * from ${searchTerm} where a = 1`,
      },
    ].map(stmt =>
      mockStmtStats({
        id: Long.fromInt(stmt.id),
        key: {
          key_data: {
            app: stmt.app,
            database: stmt.db,
            query: stmt.query,
            query_summary: stmt.query,
          },
        },
        stats: { service_lat: { mean: stmt.svcLatSecs, squared_diffs: 0 } },
      }),
    );

    const expectedIDs = [5, 6];

    filterAndCheckStmts(stmtsRaw, filters, searchTerm, expectedIDs);
  });
});

describe("getAppsFromStmtsResponse", () => {
  it("should extract all app names from stmts", () => {
    const appNames = [
      "cockroachApp",
      "myCoolApp",
      "cockroachApp",
      "cockroachApp",
      "hello_world",
      "hello_world",
      "anotherApp",
    ];

    const stmts = appNames.map((app, i) =>
      mockStmtStats({
        id: Long.fromInt(i),
        key: {
          key_data: {
            app,
          },
        },
      }),
    );

    const extractedApps = getAppsFromStmtsResponse(
      new cockroach.server.serverpb.StatementsResponse({
        statements: stmts,
      }),
    );

    expect(extractedApps.sort()).toEqual(Array.from(new Set(appNames)).sort());
  });

  it("should not throw an error on null data", () => {
    let extractedApps = getAppsFromStmtsResponse(
      new cockroach.server.serverpb.StatementsResponse({
        statements: null,
      }),
    );
    expect(extractedApps).toEqual([]);

    extractedApps = getAppsFromStmtsResponse(null);
    expect(extractedApps).toEqual([]);

    extractedApps = getAppsFromStmtsResponse(
      new cockroach.server.serverpb.StatementsResponse({
        statements: [
          mockStmtStats({ key: { key_data: { app: null } } }),
          mockStmtStats({ key: { key_data: { app: "cockroach" } } }),
        ],
      }),
    );
    expect(extractedApps).toEqual(["cockroach"]);
  });
});
