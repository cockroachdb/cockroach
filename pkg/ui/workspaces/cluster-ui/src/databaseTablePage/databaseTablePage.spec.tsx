// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { expect } from "chai";
import { shallow } from "enzyme";
import * as H from "history";
import React from "react";

import { util } from "../index";
import { indexUnusedDuration } from "../util";

import { DatabaseTablePage, DatabaseTablePageProps } from "./databaseTablePage";

describe("DatabaseTablePage", () => {
  const history = H.createHashHistory();
  const props: DatabaseTablePageProps = {
    databaseName: "DATABASE",
    name: "TABLE",
    schemaName: "",
    showNodeRegionsSection: false,
    details: {
      loading: false,
      loaded: false,
      requestError: undefined,
      queryError: undefined,
      createStatement: undefined,
      replicaData: undefined,
      spanStats: undefined,
      indexData: undefined,
      grants: {
        all: [],
        error: undefined,
      },
      statsLastUpdated: undefined,
      nodesByRegionString: "",
    },
    automaticStatsCollectionEnabled: true,
    indexUsageStatsEnabled: true,
    showIndexRecommendations: true,
    csIndexUnusedDuration: indexUnusedDuration,
    hasAdminRole: false,
    indexStats: {
      loading: false,
      loaded: false,
      lastError: undefined,
      stats: [],
      lastReset: util.minDate,
    },
    refreshSettings: () => {},
    refreshUserSQLRoles: () => {},
    refreshTableDetails: () => {},
    isTenant: false,
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
    shallow(<DatabaseTablePage {...props} refreshNodes={mockCallback} />);
    expect(mockCallback.mock.calls).to.have.length(1);
  });
  it("should not call refreshNodes if isTenant is true", () => {
    const mockCallback = jest.fn(() => {});

    shallow(
      <DatabaseTablePage
        {...props}
        refreshNodes={mockCallback}
        isTenant={true}
      />,
    );
    expect(mockCallback.mock.calls).to.have.length(0);
  });
});
