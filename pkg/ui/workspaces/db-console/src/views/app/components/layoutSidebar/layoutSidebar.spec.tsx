// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { shallow } from "enzyme";
import { createMemoryHistory, History } from "history";
import React from "react";
import { match as Match } from "react-router";

import { Sidebar } from "./index";

describe("LayoutSidebar", () => {
  let history: History;
  let match: Match;

  beforeEach(() => {
    history = createMemoryHistory();
    match = {
      isExact: true,
      params: {},
      path: "/reports/network",
      url: "",
    };
  });

  it("does not show Network link for single node cluster", () => {
    const wrapper = shallow(
      <Sidebar
        history={history}
        match={match}
        location={history.location}
        isSingleNodeCluster={true}
      />,
    );
    expect(
      wrapper.findWhere(w => w.prop("to") === "/reports/network").exists(),
    ).toBe(false);
  });

  it("shows Network link for multi node cluster", () => {
    const wrapper = shallow(
      <Sidebar
        history={history}
        match={match}
        location={history.location}
        isSingleNodeCluster={false}
      />,
    );
    expect(
      wrapper.findWhere(w => w.prop("to") === "/reports/network").exists(),
    ).toBe(true);
  });
});
