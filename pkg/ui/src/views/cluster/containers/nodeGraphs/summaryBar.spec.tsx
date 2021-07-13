// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { assert } from "chai";
import { shallow, mount } from "enzyme";
import { MemoryRouter as Router } from "react-router-dom";

import { ClusterNodeTotalsComponent } from "./summaryBar";
import { SummaryStatBreakdown } from "src/views/shared/components/summaryBar";

describe("<ClusterNodeTotals>", () => {
  it("hidden when no data", () => {
    const wrapper = shallow(
      <Router>
        <ClusterNodeTotalsComponent
          nodesSummary={null}
          nodesSummaryEmpty={true}
        />
      </Router>,
    );
    assert.isTrue(wrapper.html() === "");
  });

  it("renders", () => {
    const nodesSummary = {
      nodeSums: {
        nodeCounts: {
          total: 1,
          healthy: 1,
          suspect: 0,
          dead: 0,
          decommissioned: 0,
        },
      },
    };
    const wrapper = shallow(
      <Router>
        <ClusterNodeTotalsComponent
          nodesSummary={nodesSummary as any}
          nodesSummaryEmpty={false}
        />
      </Router>,
    );
    assert.isTrue(wrapper.find(ClusterNodeTotalsComponent).exists());
  });

  it("renders dead nodes", () => {
    const nodesSummary = {
      nodeSums: {
        nodeCounts: {
          total: 2,
          healthy: 0,
          suspect: 1,
          dead: 1,
          decommissioned: 0,
        },
      },
    };
    const wrapper = mount(
      <Router>
        <ClusterNodeTotalsComponent
          nodesSummary={nodesSummary as any}
          nodesSummaryEmpty={false}
        />
      </Router>,
    );
    assert.isTrue(wrapper.find(SummaryStatBreakdown).exists());
  });
});
