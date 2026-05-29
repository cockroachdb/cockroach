// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as clusterUi from "@cockroachlabs/cluster-ui";
import { render, screen } from "@testing-library/react";
import React from "react";
import { MemoryRouter as Router } from "react-router-dom";

import { renderWithProviders } from "src/test-utils/renderWithProviders";

import { ClusterNodeTotals } from "./summaryBar";

jest.mock("@cockroachlabs/cluster-ui", () => ({
  ...jest.requireActual("@cockroachlabs/cluster-ui"),
  useNodesSummary: jest.fn(),
}));

const mockedUseNodesSummary = clusterUi.useNodesSummary as jest.MockedFunction<
  typeof clusterUi.useNodesSummary
>;

describe("<ClusterNodeTotals>", () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("hidden when no data", () => {
    mockedUseNodesSummary.mockReturnValue({
      nodeStatuses: [],
      nodeIDs: [],
      nodeStatusByID: {},
      nodeDisplayNameByID: {},
      livenessStatusByNodeID: {},
      livenessByNodeID: {},
      storeIDsByNodeID: {},
      isLoading: false,
      error: undefined,
    });
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
    mockedUseNodesSummary.mockReturnValue({
      nodeStatuses: [
        {
          desc: { node_id: 1 },
          metrics: {},
        },
      ] as any,
      nodeIDs: ["1"],
      nodeStatusByID: {},
      nodeDisplayNameByID: { "1": "n1" },
      livenessStatusByNodeID: {
        "1": 3, // NODE_STATUS_LIVE
      } as any,
      livenessByNodeID: {},
      storeIDsByNodeID: {},
      isLoading: false,
      error: undefined,
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
    mockedUseNodesSummary.mockReturnValue({
      nodeStatuses: [
        {
          desc: { node_id: 1 },
          metrics: {},
        },
        {
          desc: { node_id: 2 },
          metrics: {},
        },
      ] as any,
      nodeIDs: ["1", "2"],
      nodeStatusByID: {},
      nodeDisplayNameByID: { "1": "n1", "2": "n2" },
      livenessStatusByNodeID: {
        "1": 1, // NODE_STATUS_DEAD
        "2": 2, // NODE_STATUS_UNAVAILABLE (suspect)
      } as any,
      livenessByNodeID: {},
      storeIDsByNodeID: {},
      isLoading: false,
      error: undefined,
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
