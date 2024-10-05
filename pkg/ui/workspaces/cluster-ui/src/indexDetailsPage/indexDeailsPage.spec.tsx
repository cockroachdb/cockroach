// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { expect } from "chai";
import { shallow } from "enzyme";
import { IndexDetailsPage, IndexDetailsPageProps, util } from "../index";
import moment from "moment";

describe("IndexDetailsPage", () => {
  const props: IndexDetailsPageProps = {
    databaseName: "DATABASE",
    tableName: "TABLE",
    indexName: "INDEX",
    nodeRegions: {},
    hasAdminRole: undefined,
    hasViewActivityRedactedRole: undefined,
    timeScale: {
      key: "Past 10 Minutes",
      windowSize: moment.duration(10, "minutes"),
      windowValid: moment.duration(10, "seconds"),
      sampleSize: moment.duration(10, "seconds"),
      fixedWindowEnd: false,
    },
    details: {
      loading: false,
      loaded: false,
      createStatement: "",
      totalReads: 0,
      indexRecommendations: [],
      tableID: undefined,
      indexID: undefined,
      lastRead: util.minDate,
      lastReset: util.minDate,
    },
    breadcrumbItems: null,
    isTenant: false,
    refreshUserSQLRoles: () => {},
    onTimeScaleChange: () => {},
    refreshIndexStats: () => {},
  };
  it("should call refreshNodes if isTenant is false", () => {
    const mockCallback = jest.fn(() => {});
    shallow(<IndexDetailsPage {...props} refreshNodes={mockCallback} />);
    expect(mockCallback.mock.calls).to.have.length(1);
  });
  it("should not call refreshNodes if isTenant is true", () => {
    const mockCallback = jest.fn(() => {});

    shallow(
      <IndexDetailsPage
        {...props}
        refreshNodes={mockCallback}
        isTenant={true}
      />,
    );
    expect(mockCallback.mock.calls).to.have.length(0);
  });
});
