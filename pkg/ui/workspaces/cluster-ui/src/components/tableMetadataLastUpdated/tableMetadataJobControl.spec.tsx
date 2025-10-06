// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import "@testing-library/jest-dom";
import { render, screen, fireEvent, act } from "@testing-library/react";
import moment from "moment-timezone";
import React from "react";

import * as api from "src/api/databases/tableMetaUpdateJobApi";
import { TimezoneContext } from "src/contexts";

import { TableMetadataJobControl } from "./tableMetadataJobControl";

jest.mock("src/api/databases/tableMetaUpdateJobApi");

describe("TableMetadataJobControl", () => {
  const mockOnJobComplete = jest.fn();
  const mockRefreshJobStatus = jest.fn();
  const mockLastCompletedTime = moment("2024-01-01T12:00:00Z");
  const mockLastUpdatedTime = moment("2024-01-01T12:05:00Z");
  const mockLastStartTime = moment("2024-01-01T11:59:00Z");

  beforeEach(() => {
    jest.useFakeTimers();
    jest.spyOn(api, "useTableMetaUpdateJob").mockReturnValue({
      jobStatus: {
        dataValidDuration: moment.duration(1, "hour"),
        currentStatus: api.TableMetadataJobStatus.NOT_RUNNING,
        progress: 0,
        lastCompletedTime: mockLastCompletedTime,
        lastStartTime: mockLastStartTime,
        lastUpdatedTime: mockLastUpdatedTime,
        automaticUpdatesEnabled: false,
      },
      refreshJobStatus: mockRefreshJobStatus,
      isLoading: false,
    });
    jest.spyOn(api, "triggerUpdateTableMetaJobApi").mockResolvedValue({
      message: "Job triggered",
      jobTriggered: true,
    });
  });

  afterEach(() => {
    jest.useRealTimers();
    jest.clearAllMocks();
  });

  it("renders the relative last refreshed time", () => {
    render(
      <TimezoneContext.Provider value="UTC">
        <TableMetadataJobControl onJobComplete={mockOnJobComplete} />
      </TimezoneContext.Provider>,
    );

    expect(screen.getByText(/Last refreshed:/)).toBeInTheDocument();
    const lastCompletedRelativeTime = mockLastCompletedTime.fromNow();
    expect(
      screen.getByText(new RegExp(lastCompletedRelativeTime)),
    ).toBeInTheDocument();
  });

  it('renders "Never" when lastCompletedTime is null', () => {
    jest.spyOn(api, "useTableMetaUpdateJob").mockReturnValue({
      jobStatus: {
        lastCompletedTime: null,
        dataValidDuration: moment.duration(1, "hour"),
        currentStatus: api.TableMetadataJobStatus.NOT_RUNNING,
        progress: 0,
        lastStartTime: mockLastUpdatedTime,
        lastUpdatedTime: mockLastUpdatedTime,
        automaticUpdatesEnabled: false,
      },
      refreshJobStatus: mockRefreshJobStatus,
      isLoading: false,
    });

    render(
      <TimezoneContext.Provider value="UTC">
        <TableMetadataJobControl onJobComplete={mockOnJobComplete} />
      </TimezoneContext.Provider>,
    );

    expect(screen.getByText(/Never/)).toBeInTheDocument();
  });

  it("triggers update when refresh button is clicked", async () => {
    render(
      <TimezoneContext.Provider value="UTC">
        <TableMetadataJobControl onJobComplete={mockOnJobComplete} />
      </TimezoneContext.Provider>,
    );

    const refreshButton = screen.getByRole("button");
    await act(async () => {
      fireEvent.click(refreshButton);
    });

    expect(api.triggerUpdateTableMetaJobApi).toHaveBeenCalledWith({
      onlyIfStale: false,
    });
    expect(mockRefreshJobStatus).toHaveBeenCalled();
  });

  it("schedules periodic updates", async () => {
    jest.spyOn(global, "setInterval");

    render(
      <TimezoneContext.Provider value="UTC">
        <TableMetadataJobControl onJobComplete={mockOnJobComplete} />
      </TimezoneContext.Provider>,
    );

    expect(setInterval).toHaveBeenCalledWith(expect.any(Function), 10000);

    await act(async () => {
      jest.advanceTimersByTime(10000);
    });

    expect(api.triggerUpdateTableMetaJobApi).toHaveBeenCalledWith({
      onlyIfStale: true,
    });
  });

  it("calls onJobComplete when lastCompletedTime changes", () => {
    const { rerender } = render(
      <TimezoneContext.Provider value="UTC">
        <TableMetadataJobControl onJobComplete={mockOnJobComplete} />
      </TimezoneContext.Provider>,
    );

    // Update the mock to return a new lastCompletedTime
    jest.spyOn(api, "useTableMetaUpdateJob").mockReturnValue({
      jobStatus: {
        lastCompletedTime: moment("2024-01-01T13:00:00Z"),
        lastStartTime: moment("2024-01-01T13:00:00Z"),
        lastUpdatedTime: moment.utc(),
        dataValidDuration: moment.duration(1, "hour"),
        currentStatus: api.TableMetadataJobStatus.NOT_RUNNING,
        progress: 0,
        automaticUpdatesEnabled: false,
      },
      refreshJobStatus: mockRefreshJobStatus,
      isLoading: false,
    });

    // Rerender the component with the updated mock
    rerender(
      <TimezoneContext.Provider value="UTC">
        <TableMetadataJobControl onJobComplete={mockOnJobComplete} />
      </TimezoneContext.Provider>,
    );

    expect(mockOnJobComplete).toHaveBeenCalled();
  });

  it("disables refresh button when job is running", () => {
    jest.spyOn(api, "useTableMetaUpdateJob").mockReturnValue({
      jobStatus: {
        lastCompletedTime: moment("2024-01-01T12:00:00Z"),
        dataValidDuration: moment.duration(1, "hour"),
        currentStatus: api.TableMetadataJobStatus.RUNNING,
        progress: 0,
        lastStartTime: moment.utc(),
        lastUpdatedTime: moment.utc(),
        automaticUpdatesEnabled: false,
      },
      refreshJobStatus: mockRefreshJobStatus,
      isLoading: false,
    });

    render(
      <TimezoneContext.Provider value="UTC">
        <TableMetadataJobControl onJobComplete={mockOnJobComplete} />
      </TimezoneContext.Provider>,
    );

    expect(screen.getByRole("button")).toBeDisabled();
  });
});
