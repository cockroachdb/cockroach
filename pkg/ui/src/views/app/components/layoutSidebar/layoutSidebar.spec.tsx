// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { shallow } from "enzyme";
import { createMemoryHistory, History } from "history";
import { match as Match } from "react-router";
import { assert } from "chai";
import "src/enzymeInit";
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

  it("does not show Network Latency link for single node cluster", () => {
    const wrapper = shallow(
      <Sidebar
        history={history}
        match={match}
        location={history.location}
        isSingleNodeCluster={true}
      />,
    );
    assert.isFalse(
      wrapper.findWhere((w) => w.prop("to") === "/reports/network").exists(),
    );
  });

  it("shows Network Latency link for multi node cluster", () => {
    const wrapper = shallow(
      <Sidebar
        history={history}
        match={match}
        location={history.location}
        isSingleNodeCluster={false}
      />,
    );
    assert.isTrue(
      wrapper.findWhere((w) => w.prop("to") === "/reports/network").exists(),
    );
  });
});
