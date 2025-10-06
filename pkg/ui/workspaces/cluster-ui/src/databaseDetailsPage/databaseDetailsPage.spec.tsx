// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { expect } from "chai";
import { shallow } from "enzyme";
import * as H from "history";
import React from "react";

import { defaultFilters } from "../queryFilter";
import { indexUnusedDuration } from "../util";

import {
  DatabaseDetailsPage,
  DatabaseDetailsPageProps,
} from "./databaseDetailsPage";
import { ViewMode } from "./types";

describe("DatabaseDetailsPage", () => {
  const history = H.createHashHistory();
  const props: DatabaseDetailsPageProps = {
    loading: false,
    loaded: false,
    requestError: undefined,
    queryError: undefined,
    name: "things",
    search: null,
    filters: defaultFilters,
    nodeRegions: {},
    showNodeRegionsColumn: false,
    viewMode: ViewMode.Tables,
    sortSettingTables: { ascending: true, columnTitle: "name" },
    sortSettingGrants: { ascending: true, columnTitle: "name" },
    tables: [],
    showIndexRecommendations: false,
    csIndexUnusedDuration: indexUnusedDuration,
    isTenant: false,
    refreshTableDetails: () => {},
    refreshNodes: () => {},
    refreshDatabaseDetails: () => {},
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
    shallow(<DatabaseDetailsPage {...props} refreshNodes={mockCallback} />);
    expect(mockCallback.mock.calls).to.have.length(1);
  });
  it("should not call refreshNodes if isTenant is true", () => {
    const mockCallback = jest.fn(() => {});
    shallow(
      <DatabaseDetailsPage
        {...props}
        refreshNodes={mockCallback}
        isTenant={true}
      />,
    );
    expect(mockCallback.mock.calls).to.have.length(0);
  });
});
