// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { expect } from "chai";
import * as H from "history";
import { shallow } from "enzyme";

import { defaultFilters } from "../queryFilter";
import { indexUnusedDuration } from "../util";

import { DatabasesPage, DatabasesPageProps } from "./databasesPage";

describe("DatabasesPage", () => {
  const history = H.createHashHistory();
  const props: DatabasesPageProps = {
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
    refreshDatabases: () => {},
    refreshDatabaseDetails: () => {},
    refreshDatabaseSpanStats: () => {},
    refreshSettings: () => {},
    history: history,
    location: history.location,
    match: {
      url: "",
      path: history.location.pathname,
      isExact: false,
      params: {},
    },
  };
  it("should call refreshNodes if isTenant is false", () => {
    const mockCallback = jest.fn(() => {});
    shallow(<DatabasesPage {...props} refreshNodes={mockCallback} />);
    expect(mockCallback.mock.calls).to.have.length(1);
  });
  it("should not call refreshNodes if isTenant is true", () => {
    const mockCallback = jest.fn(() => {});

    shallow(
      <DatabasesPage {...props} refreshNodes={mockCallback} isTenant={true} />,
    );
    expect(mockCallback.mock.calls).to.have.length(0);
  });
});
