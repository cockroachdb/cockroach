// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
  convertRawStmtsToAggregateStatistics,
  filteredStatementsData,
  getAppsFromStmtsResponse,
} from "src/sqlActivity/util";
import { mockStmtStats } from "src/api/testUtils";
import { Filters } from "src/queryFilter/filter";
import Long from "long";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

describe("filteredStatementsData", () => {
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

    const expectedCount = 2;
    const expectedIDs = [2, 3];

    const statements = convertRawStmtsToAggregateStatistics(stmtsRaw);

    const filteredStmts = filteredStatementsData(
      {},
      "giraffe",
      statements,
      false,
    );

    expect(filteredStmts.length).toEqual(expectedCount);

    expect(
      filteredStmts.map(stmt => Number(stmt.aggregatedFingerprintID)).sort(),
    ).toEqual(expectedIDs);
  });

  it("should filter out statements not matching filter app", () => {
    const stmtsRaw = [
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
    ].map(stmt =>
      mockStmtStats({
        id: Long.fromInt(stmt.id),
        key: { key_data: { app: stmt.app } },
      }),
    );

    const expectedCount = 1;
    const expectedIDs = [2];

    const statements = convertRawStmtsToAggregateStatistics(stmtsRaw);

    const filters: Filters = {
      app: "giraffe",
    };

    const filteredStmts = filteredStatementsData(
      filters,
      null,
      statements,
      false,
    );

    expect(filteredStmts.length).toEqual(expectedCount);

    expect(
      filteredStmts.map(stmt => Number(stmt.aggregatedFingerprintID)).sort(),
    ).toEqual(expectedIDs);
  });

  it("should filter out statements not matching multiple filter apps", () => {
    const filters: Filters = {
      app: "giraffe, elephant",
    };

    const stmtsRaw = [
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
    ].map(stmt =>
      mockStmtStats({
        id: Long.fromInt(stmt.id),
        key: { key_data: { app: stmt.app } },
      }),
    );

    const expectedCount = 3;
    const expectedIDs = [3, 4, 6];

    const statements = convertRawStmtsToAggregateStatistics(stmtsRaw);

    const filteredStmts = filteredStatementsData(
      filters,
      null,
      statements,
      false,
    );

    expect(filteredStmts.length).toEqual(expectedCount);

    expect(
      filteredStmts.map(stmt => Number(stmt.aggregatedFingerprintID)).sort(),
    ).toEqual(expectedIDs);
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

    const expectedCount = 3;
    const expectedIDs = [1, 2, 6];

    const statements = convertRawStmtsToAggregateStatistics(stmtsRaw);

    const filteredStmts = filteredStatementsData(
      filters,
      null,
      statements,
      false,
    );

    expect(filteredStmts.length).toEqual(expectedCount);

    expect(
      filteredStmts.map(stmt => Number(stmt.aggregatedFingerprintID)).sort(),
    ).toEqual(expectedIDs);
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

    const expectedCount = 3;
    const expectedIDs = [3, 4, 5];

    const statements = convertRawStmtsToAggregateStatistics(stmtsRaw);

    const filteredStmts = filteredStatementsData(
      filters,
      null,
      statements,
      false,
    );

    expect(filteredStmts.length).toEqual(expectedCount);

    expect(
      filteredStmts.map(stmt => Number(stmt.aggregatedFingerprintID)).sort(),
    ).toEqual(expectedIDs);
  });

  it("should filter out statements with svc_lat less than filter time value", () => {
    // Create stmts with ids matching service_lat in seceonds.
    // We will filter out the first half of these stmts.
    const stmtsRaw = [1, 2, 3, 4, 5, 6].map(stmtID =>
      mockStmtStats({
        id: Long.fromInt(stmtID),
        stats: { service_lat: { mean: stmtID, squared_diffs: 0 } },
      }),
    );

    const expectedCount = 3;
    const expectedIDs = [4, 5, 6];

    const statements = convertRawStmtsToAggregateStatistics(stmtsRaw);

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
      const filteredStmts = filteredStatementsData(
        filter,
        null,
        statements,
        false,
      );

      expect(filteredStmts.length).toEqual(expectedCount);

      expect(
        filteredStmts.map(stmt => Number(stmt.aggregatedFingerprintID)).sort(),
      ).toEqual(expectedIDs);
    });
  });

  it("should filter out statements not matching ALL filters", () => {
    const filters: Filters = {
      database: "coolestDB",
      app: "coolestApp",
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

    const expectedCount = 1;
    const expectedIDs = [5];

    const statements = convertRawStmtsToAggregateStatistics(stmtsRaw);

    const filteredStmts = filteredStatementsData(
      filters,
      searchTerm,
      statements,
      false,
    );

    expect(filteredStmts.length).toEqual(expectedCount);

    expect(
      filteredStmts.map(stmt => Number(stmt.aggregatedFingerprintID)).sort(),
    ).toEqual(expectedIDs);
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
