// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { DatabaseRow } from "./databaseTypes";
import { filterDatabases } from "./utils";

describe("filterDatabases", () => {
  const mockDatabases: DatabaseRow[] = [
    {
      name: "DB1",
      id: 1,
      approximateDiskSizeMiB: 8192,
      tableCount: 123,
      rangeCount: 23,
      nodesByRegion: { "us-east-1": [1] },
      schemaInsightsCount: 0,
    },
    {
      name: "db2",
      id: 2,
      approximateDiskSizeMiB: 4096,
      tableCount: 456,
      rangeCount: 45,
      nodesByRegion: { "us-west-1": [2] },
      schemaInsightsCount: 1,
    },
    {
      name: "DB3",
      id: 3,
      approximateDiskSizeMiB: 2048,
      tableCount: 789,
      rangeCount: 67,
      nodesByRegion: { "us-west-2": [3] },
      schemaInsightsCount: 2,
    },
    // Test weird name strings
    {
      name: "My strangely_named db",
      id: 4,
      approximateDiskSizeMiB: 1024,
      tableCount: 1011,
      rangeCount: 89,
      nodesByRegion: { "us-west-2": [4] },
      schemaInsightsCount: 3,
    },
  ];

  it("should return all databases when no filters are applied", () => {
    const filters = {};
    expect(filterDatabases(mockDatabases, filters)).toEqual(mockDatabases);
  });

  it.each([
    [{ regionValues: ["us-east-1"] }, [mockDatabases[0]]],
    [{ regionValues: ["us-west-1"] }, [mockDatabases[1]]],
    [
      { regionValues: ["us-west-1", "us-west-2"] },
      [mockDatabases[1], mockDatabases[2], mockDatabases[3]],
    ],
    [{ regionValues: ["us-west-1", "us-west-2", "us-east-1"] }, mockDatabases],
  ])(
    "should filter databases by region values",
    (filters, expectedDatabases) => {
      expect(filterDatabases(mockDatabases, filters)).toEqual(
        expectedDatabases,
      );
    },
  );

  it.each([
    [{ search: "db1" }, [mockDatabases[0]]],
    [{ search: "3" }, [mockDatabases[2]]],
    [{ search: "MY StRAngELY_NAMED db" }, [mockDatabases[3]]],
    [{ search: "y strangely_named db" }, [mockDatabases[3]]],
    [{ search: "strangely_named" }, [mockDatabases[3]]],
    [{ search: "dbbbb" }, []],
    [{ search: "db" }, mockDatabases],
    [{ search: "strangely_named" }, [mockDatabases[3]]],
  ])(
    "should filter databases by non-case sensitive search string",
    (filters, expectedDatabases) => {
      expect(filterDatabases(mockDatabases, filters)).toEqual(
        expectedDatabases,
      );
    },
  );
});
