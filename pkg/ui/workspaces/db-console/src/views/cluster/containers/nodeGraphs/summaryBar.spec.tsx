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
import { shallow, mount } from "enzyme";
import { MemoryRouter as Router } from "react-router-dom";
import sinon from "sinon";

import { ClusterNodeTotals } from "./summaryBar";
import * as summaryBar from "./summaryBar";
import { SummaryStatBreakdown } from "src/views/shared/components/summaryBar";
import { renderWithProviders } from "src/test-utils/renderWithProviders";
import { NodeSummaryStats } from "src/redux/nodes";
import * as nodes from "src/redux/nodes";

const sandbox = sinon.createSandbox();

describe("<ClusterNodeTotals>", () => {
  let nodeSumsSelectorStubReturn: NodeSummaryStats;
  let component: React.ReactElement;

  beforeEach(() => {
    component = renderWithProviders(
      <Router>
        <ClusterNodeTotals />
      </Router>,
    );
    nodeSumsSelectorStubReturn = {
      capacityAvailable: 0,
      capacityTotal: 0,
      capacityUsable: 0,
      capacityUsed: 0,
      replicas: 0,
      totalRanges: 0,
      unavailableRanges: 0,
      underReplicatedRanges: 0,
      usedBytes: 0,
      usedMem: 0,
      nodeCounts: {
        total: 0,
        healthy: 0,
        suspect: 0,
        dead: 0,
        decommissioned: 0,
      },
    };
  });
  afterEach(() => {
    sandbox.restore();
    sandbox.reset();
  });

  it("hidden when no data", () => {
    sandbox.stub(summaryBar, "selectNodesSummaryEmpty").returns(true);
    const component = renderWithProviders(<ClusterNodeTotals />);
    const wrapper = shallow(<Router>{component}</Router>);
    expect(wrapper.html() === "").toBe(true);
  });

  it("renders", () => {
    sandbox.stub(summaryBar, "selectNodesSummaryEmpty").returns(false);
    sandbox.stub(nodes, "nodeSumsSelector").returns({
      ...nodeSumsSelectorStubReturn,
      nodeCounts: {
        total: 1,
        healthy: 1,
        suspect: 0,
        dead: 0,
        decommissioned: 0,
      },
    });
    const wrapper = mount(component);
    expect(wrapper.find(ClusterNodeTotals).exists()).toBe(true);
  });

  it("renders dead nodes", () => {
    sandbox.stub(summaryBar, "selectNodesSummaryEmpty").returns(false);
    sandbox.stub(nodes, "nodeSumsSelector").returns({
      ...nodeSumsSelectorStubReturn,
      nodeCounts: {
        total: 2,
        healthy: 0,
        suspect: 1,
        dead: 1,
        decommissioned: 0,
      },
    });
    const wrapper = mount(component);
    expect(wrapper.find(SummaryStatBreakdown).exists()).toBe(true);
  });
});
