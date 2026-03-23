// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render, screen } from "@testing-library/react";
import React from "react";
import { MemoryRouter as Router } from "react-router-dom";

import { NodeSummaryStats } from "src/redux/nodes";
import * as nodes from "src/redux/nodes";
import { renderWithProviders } from "src/test-utils/renderWithProviders";

import * as summaryBar from "./summaryBar";
import { ClusterNodeTotals } from "./summaryBar";

describe("<ClusterNodeTotals>", () => {
  let nodeSumsSelectorStubReturn: NodeSummaryStats;

  beforeEach(() => {
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
        draining: 0,
      },
    };
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("hidden when no data", () => {
    jest.spyOn(summaryBar, "selectNodesSummaryEmpty").mockReturnValue(true);
    const { container } = render(
      renderWithProviders(
        <Router>
          <ClusterNodeTotals />
        </Router>,
      ),
    );
    expect(container.innerHTML).toBe("");
  });

  it("renders", () => {
    jest.spyOn(summaryBar, "selectNodesSummaryEmpty").mockReturnValue(false);
    jest.spyOn(nodes, "nodeSumsSelector").mockReturnValue({
      ...nodeSumsSelectorStubReturn,
      nodeCounts: {
        total: 1,
        healthy: 1,
        suspect: 0,
        dead: 0,
        decommissioned: 0,
        draining: 0,
      },
    });
    render(
      renderWithProviders(
        <Router>
          <ClusterNodeTotals />
        </Router>,
      ),
    );
    expect(screen.getByText("Total Nodes")).toBeTruthy();
  });

  it("renders dead nodes", () => {
    jest.spyOn(summaryBar, "selectNodesSummaryEmpty").mockReturnValue(false);
    jest.spyOn(nodes, "nodeSumsSelector").mockReturnValue({
      ...nodeSumsSelectorStubReturn,
      nodeCounts: {
        total: 2,
        healthy: 0,
        suspect: 1,
        dead: 1,
        decommissioned: 0,
        draining: 0,
      },
    });
    render(
      renderWithProviders(
        <Router>
          <ClusterNodeTotals />
        </Router>,
      ),
    );
    expect(screen.getByText("Dead")).toBeTruthy();
    expect(screen.getByText("Suspect")).toBeTruthy();
  });
});
