// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import "@testing-library/jest-dom";
import { act, fireEvent, render, waitFor } from "@testing-library/react";
import moment from "moment";
import React from "react";
import { MemoryRouter, Route, Switch } from "react-router-dom";
import { SWRConfig } from "swr";

import { ClusterDetailsContext, ClusterDetailsContextType } from "../contexts";

import {
  IndexDetailsPage,
  IndexDetailsPageProps,
  parseSchemaQualifiedTableName,
} from "./indexDetailsPage";

// Mock the SWR-based hooks.
const mockNodeRegionsByID: Record<string, string> = {};
const mockUserSQLRoles = { roles: [] as string[] };
const mockResetIndexStatsApi = jest.fn().mockResolvedValue({});
const mockTableIndexStats = {
  indexStats: {
    tableIndexes: [] as any[],
    lastReset: null as moment.Moment | null,
    databaseID: 1,
  },
  isLoading: false,
  error: null as Error | null,
  refreshIndexStats: jest.fn(),
};

jest.mock("../api/nodesApi", () => ({
  useNodes: () => ({
    isLoading: false,
    error: null as Error | null,
    nodeRegionsByID: mockNodeRegionsByID,
    storeIDToNodeID: {},
  }),
}));

jest.mock("../api/userApi", () => ({
  useUserSQLRoles: () => ({
    data: mockUserSQLRoles,
    isLoading: false,
    error: null as Error | null,
  }),
}));

jest.mock("../api/databases/tableIndexesApi", () => ({
  useTableIndexStats: () => mockTableIndexStats,
  resetIndexStatsApi: (...args: any[]) => mockResetIndexStatsApi(...args),
}));

const defaultClusterContext: ClusterDetailsContextType = {
  isTenant: false,
  clusterId: "test-cluster",
};

describe("parseSchemaQualifiedTableName", () => {
  it.each([
    ['"public"."mytable"', { schema: "public", table: "mytable" }],
    ['"my_schema"."my_table"', { schema: "my_schema", table: "my_table" }],
    ["public.mytable", { schema: "public", table: "mytable" }],
    ["myschema.mytable", { schema: "myschema", table: "mytable" }],
    ["mytable", { schema: "public", table: "mytable" }],
    ["", { schema: "public", table: "" }],
  ])("parses %s", (input, expected) => {
    expect(parseSchemaQualifiedTableName(input)).toEqual(expected);
  });
});

describe("IndexDetailsPage", () => {
  const props: IndexDetailsPageProps = {
    databaseName: "DATABASE",
    tableName: "TABLE",
    indexName: "INDEX",
    timeScale: {
      key: "Past 10 Minutes",
      windowSize: moment.duration(10, "minutes"),
      windowValid: moment.duration(10, "seconds"),
      sampleSize: moment.duration(10, "seconds"),
      fixedWindowEnd: false,
    },
    onTimeScaleChange: jest.fn(),
  };

  const renderWithProviders = (
    ui: React.ReactElement,
    clusterContext: ClusterDetailsContextType = defaultClusterContext,
  ) => {
    return render(
      <SWRConfig value={{ provider: () => new Map() }}>
        <ClusterDetailsContext.Provider value={clusterContext}>
          <MemoryRouter initialEntries={["/"]}>
            <Switch>
              <Route path="/">{ui}</Route>
            </Switch>
          </MemoryRouter>
        </ClusterDetailsContext.Provider>
      </SWRConfig>,
    );
  };

  beforeEach(() => {
    jest.clearAllMocks();
    mockUserSQLRoles.roles = [];
    mockTableIndexStats.indexStats.tableIndexes = [];
    mockTableIndexStats.indexStats.lastReset = null;
    mockTableIndexStats.indexStats.databaseID = 1;
  });

  it("should render the index details page", () => {
    act(() => {
      renderWithProviders(<IndexDetailsPage {...props} />);
    });
  });

  it("should render bread crumbs", () => {
    let container: HTMLElement;
    mockTableIndexStats.indexStats.tableIndexes = [
      {
        indexName: "INDEX",
        dbName: "DATABASE",
        tableName: "TABLE",
        escSchemaQualifiedTableName: '"public"."TABLE"',
        indexType: "secondary",
        createStatement: "",
        tableID: "2",
        indexID: "1",
        lastRead: null,
        totalReads: 0,
        totalRowsRead: 0,
        indexRecs: [],
      },
    ];
    mockTableIndexStats.indexStats.databaseID = 1;

    act(() => {
      const result = renderWithProviders(<IndexDetailsPage {...props} />);
      container = result.container;
    });
    const itemLinks = container.getElementsByClassName("item-link");
    expect(itemLinks).toHaveLength(3);
    expect(itemLinks[0].getAttribute("href")).toEqual("/databases");
    expect(itemLinks[1].getAttribute("href")).toEqual("/databases/1");
    expect(itemLinks[2].getAttribute("href")).toEqual("/table/2");
  });

  describe("reset index stats", () => {
    beforeEach(() => {
      mockUserSQLRoles.roles = ["ADMIN"];
      mockTableIndexStats.indexStats.tableIndexes = [
        {
          indexName: "INDEX",
          dbName: "DATABASE",
          tableName: "TABLE",
          escSchemaQualifiedTableName: '"public"."TABLE"',
          indexType: "secondary",
          createStatement: "",
          tableID: "2",
          indexID: "1",
          lastRead: null,
          totalReads: 0,
          totalRowsRead: 0,
          indexRecs: [],
        },
      ];
    });

    it("should show reset button for admin users", () => {
      let container: HTMLElement;
      act(() => {
        const result = renderWithProviders(<IndexDetailsPage {...props} />);
        container = result.container;
      });
      const resetBtn = container.querySelector(".index-stats__reset-btn");
      expect(resetBtn).not.toBeNull();
    });

    it.each([
      [[], "no roles"],
      [["VIEWACTIVITY"], "VIEWACTIVITY only"],
      [["VIEWACTIVITYREDACTED"], "VIEWACTIVITYREDACTED only"],
      [["VIEWACTIVITY", "VIEWACTIVITYREDACTED"], "non-admin roles combined"],
    ])(
      "should not show reset button for non-admin users with %s (%s)",
      roles => {
        mockUserSQLRoles.roles = roles;
        let container: HTMLElement;
        act(() => {
          const result = renderWithProviders(<IndexDetailsPage {...props} />);
          container = result.container;
        });
        const resetBtn = container.querySelector(".index-stats__reset-btn");
        expect(resetBtn).toBeNull();
      },
    );

    it("should call resetIndexStatsApi and refreshIndexStats on click", async () => {
      let container: HTMLElement;
      act(() => {
        const result = renderWithProviders(<IndexDetailsPage {...props} />);
        container = result.container;
      });

      const resetBtn = container.querySelector(".index-stats__reset-btn");
      await act(async () => {
        fireEvent.click(resetBtn);
      });

      expect(mockResetIndexStatsApi).toHaveBeenCalled();
      expect(mockTableIndexStats.refreshIndexStats).toHaveBeenCalled();
    });

    it("should catch errors from resetIndexStatsApi", async () => {
      const consoleError = jest
        .spyOn(console, "error")
        .mockImplementation(() => {});
      mockResetIndexStatsApi.mockRejectedValueOnce(new Error("network error"));

      let container: HTMLElement;
      act(() => {
        const result = renderWithProviders(<IndexDetailsPage {...props} />);
        container = result.container;
      });

      const resetBtn = container.querySelector(".index-stats__reset-btn");
      await act(async () => {
        fireEvent.click(resetBtn);
      });

      await waitFor(() => {
        expect(consoleError).toHaveBeenCalledWith(
          "Failed to reset index stats:",
          expect.any(Error),
        );
      });
      // refreshIndexStats should NOT be called when reset fails.
      expect(mockTableIndexStats.refreshIndexStats).not.toHaveBeenCalled();

      consoleError.mockRestore();
    });
  });
});
