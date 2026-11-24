// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render, screen } from "@testing-library/react";
import moment from "moment-timezone";
import React from "react";
import { MemoryRouter as Router } from "react-router-dom";

import * as liveWorkloadApi from "../api/liveWorkloadApi";
import { UseLiveWorkloadResult } from "../api/liveWorkloadApi";

import { ActiveStatementsView } from "./activeStatementsView";

const getLiveWorkloadFixture = (): UseLiveWorkloadResult => ({
  data: {
    statements: [],
    transactions: [],
    clusterLocks: null,
    internalAppNamePrefix: "$ internal",
    maxSizeApiReached: false,
  },
  isLoading: false,
  error: null,
  lastUpdated: moment.utc(),
  refresh: jest.fn(),
});

describe("ActiveStatementsView", () => {
  let mockUseLiveWorkload: jest.SpyInstance;
  let fixture: UseLiveWorkloadResult;

  beforeEach(() => {
    fixture = getLiveWorkloadFixture();
    mockUseLiveWorkload = jest
      .spyOn(liveWorkloadApi, "useLiveWorkload")
      .mockReturnValue(fixture);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("renders without crashing when data is loaded", () => {
    render(
      <Router>
        <ActiveStatementsView />
      </Router>,
    );

    // The search and filter controls should be present.
    expect(screen.getByPlaceholderText(/search/i)).toBeDefined();
  });

  it("shows loading indicator when data is loading", () => {
    fixture.isLoading = true;
    fixture.data.statements = [];
    jest.restoreAllMocks();
    jest.spyOn(liveWorkloadApi, "useLiveWorkload").mockReturnValue(fixture);

    render(
      <Router>
        <ActiveStatementsView />
      </Router>,
    );

    expect(screen.getByLabelText("Loading...")).toBeDefined();
  });

  it("shows error component when an error occurs", () => {
    fixture.error = new Error("sessions failed");
    jest.restoreAllMocks();
    jest.spyOn(liveWorkloadApi, "useLiveWorkload").mockReturnValue(fixture);

    render(
      <Router>
        <ActiveStatementsView />
      </Router>,
    );

    expect(screen.getByText(/sessions failed/)).toBeDefined();
  });

  it("enables polling when auto-refresh is on by default", () => {
    render(
      <Router>
        <ActiveStatementsView />
      </Router>,
    );

    // Auto-refresh defaults to true, so the hook should be called with
    // a non-zero refreshInterval.
    expect(mockUseLiveWorkload).toHaveBeenCalledWith({
      refreshInterval: 10_000,
    });
  });
});
