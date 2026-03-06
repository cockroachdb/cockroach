// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { shallow, mount } from "enzyme";
import React from "react";
import { MemoryRouter as Router } from "react-router-dom";

import { renderWithProviders } from "src/test-utils/renderWithProviders";
import { SummaryStatBreakdown } from "src/views/shared/components/summaryBar";

import { ClusterNodeTotals } from "./summaryBar";

const mockUseNodesSummary = jest.fn();
jest.mock("@cockroachlabs/cluster-ui", () => {
  const actual = jest.requireActual("@cockroachlabs/cluster-ui");
  return {
    ...actual,
    useNodesSummary: (...args: unknown[]) => mockUseNodesSummary(...args),
  };
});

describe("<ClusterNodeTotals>", () => {
  let component: React.ReactElement;

  beforeEach(() => {
    component = renderWithProviders(
      <Router>
        <ClusterNodeTotals />
      </Router>,
    );
  });
  afterEach(() => {
    jest.resetAllMocks();
  });

  it("hidden when no data", () => {
    mockUseNodesSummary.mockReturnValue({
      nodeStatuses: null,
      livenessStatusByNodeID: {},
      isLoading: true,
    });
    const component = renderWithProviders(<ClusterNodeTotals />);
    const wrapper = shallow(<Router>{component}</Router>);
    expect(wrapper.html() === "").toBe(true);
  });

  it("renders", () => {
    mockUseNodesSummary.mockReturnValue({
      nodeStatuses: [
        {
          desc: {
            node_id: 1,
            address: { address_field: "addr1" },
            locality: { tiers: [] },
          },
          metrics: {},
          store_statuses: [],
        },
      ],
      livenessStatusByNodeID: { "1": 1 }, // NODE_STATUS_LIVE
      isLoading: false,
    });
    const wrapper = mount(component);
    expect(wrapper.find(ClusterNodeTotals).exists()).toBe(true);
  });

  it("renders dead nodes", () => {
    mockUseNodesSummary.mockReturnValue({
      nodeStatuses: [
        {
          desc: {
            node_id: 1,
            address: { address_field: "addr1" },
            locality: { tiers: [] },
          },
          metrics: {},
          store_statuses: [],
        },
        {
          desc: {
            node_id: 2,
            address: { address_field: "addr2" },
            locality: { tiers: [] },
          },
          metrics: {},
          store_statuses: [],
        },
      ],
      livenessStatusByNodeID: {
        "1": 3, // NODE_STATUS_UNAVAILABLE (suspect)
        "2": 4, // NODE_STATUS_DEAD
      },
      isLoading: false,
    });
    const wrapper = mount(component);
    expect(wrapper.find(SummaryStatBreakdown).exists()).toBe(true);
  });
});
