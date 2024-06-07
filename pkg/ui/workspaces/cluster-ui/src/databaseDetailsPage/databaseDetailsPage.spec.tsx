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
