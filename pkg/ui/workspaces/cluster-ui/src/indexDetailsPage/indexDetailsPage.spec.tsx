// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import "@testing-library/jest-dom";
import { render } from "@testing-library/react";
import { shallow } from "enzyme";
import moment from "moment";
import React from "react";
import { MemoryRouter, Route, Switch } from "react-router-dom";

import { IndexDetailsPage, IndexDetailsPageProps, util } from "../index";

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
      tableID: "2",
      indexID: undefined,
      lastRead: util.minDate,
      lastReset: util.minDate,
      databaseID: 1,
    },
    isTenant: false,
    refreshUserSQLRoles: () => {},
    onTimeScaleChange: () => {},
    refreshIndexStats: () => {},
  };
  it("should call refreshNodes if isTenant is false", () => {
    const mockCallback = jest.fn(() => {});
    shallow(<IndexDetailsPage {...props} refreshNodes={mockCallback} />);
    expect(mockCallback.mock.calls).toHaveLength(1);
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
    expect(mockCallback.mock.calls).toHaveLength(0);
  });
  it("should render bread crumbs", () => {
    const { container } = render(
      <MemoryRouter initialEntries={["/"]}>
        <Switch>
          <Route path="/">
            <IndexDetailsPage {...props} isTenant={false} />
          </Route>
        </Switch>
      </MemoryRouter>,
    );
    const itemLinks = container.getElementsByClassName("item-link");
    expect(itemLinks).toHaveLength(3);
    expect(itemLinks[0].getAttribute("href")).toEqual("/databases");
    expect(itemLinks[1].getAttribute("href")).toEqual("/databases/1");
    expect(itemLinks[2].getAttribute("href")).toEqual("/table/2");
  });
});
