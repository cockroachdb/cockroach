import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import moment from "moment-timezone";
import React from "react";

import * as api from "src/api/databases/tableMetaUpdateJobApi";
import { TimezoneContext } from "src/contexts";

import { TableMetadataJobControl } from "./tableMetadataJobControl";

jest.mock("src/api/databases/tableMetaUpdateJobApi");

describe("TableMetadataJobControl", () => {
  const mockOnJobTriggered = jest.fn();
  const mockLastCompletedTime = moment("2024-01-01T12:00:00Z");
  const mockLastUpdatedTime = moment("2024-01-01T12:05:00Z");
  const mockLastStartTime = moment("2024-01-01T11:59:00Z");

  const defaultJobStatus: api.TableMetaUpdateJobInfo = {
    dataValidDuration: moment.duration(1, "hour"),
    currentStatus: api.TableMetadataJobStatus.NOT_RUNNING,
    progress: 0,
    lastCompletedTime: mockLastCompletedTime,
    lastStartTime: mockLastStartTime,
    lastUpdatedTime: mockLastUpdatedTime,
    automaticUpdatesEnabled: false,
  };

  beforeEach(() => {
    jest.useFakeTimers();
    jest.spyOn(api, "triggerUpdateTableMetaJobApi").mockResolvedValue({
      message: "Job triggered",
      jobTriggered: true,
      jobStatus: defaultJobStatus,
    });
  });

  afterEach(() => {
    jest.useRealTimers();
    jest.clearAllMocks();
  });

  it("renders the last refreshed time", () => {
    render(
      <TimezoneContext.Provider value="UTC">
        <TableMetadataJobControl
          error={null}
          jobStatus={defaultJobStatus}
          onJobTriggered={mockOnJobTriggered}
        />
      </TimezoneContext.Provider>,
    );

    expect(screen.getByText(/Last refreshed:/)).toBeInTheDocument();
    expect(
      screen.getByText(/Jan 01, 2024 at 12:00:00 UTC/),
    ).toBeInTheDocument();
  });

  it('renders "Never" when lastCompletedTime is null', () => {
    render(
      <TimezoneContext.Provider value="UTC">
        <TableMetadataJobControl
          error={null}
          jobStatus={{ ...defaultJobStatus, lastCompletedTime: null }}
          onJobTriggered={mockOnJobTriggered}
        />
      </TimezoneContext.Provider>,
    );

    expect(screen.getByText(/Never/)).toBeInTheDocument();
  });

  it("triggers update when refresh button is clicked", async () => {
    render(
      <TimezoneContext.Provider value="UTC">
        <TableMetadataJobControl
          error={null}
          jobStatus={defaultJobStatus}
          onJobTriggered={mockOnJobTriggered}
        />
      </TimezoneContext.Provider>,
    );

    const refreshButton = screen.getByRole("button");
    userEvent.click(refreshButton);

    expect(api.triggerUpdateTableMetaJobApi).toHaveBeenCalledWith({
      onlyIfStale: false,
    });
    expect(mockOnJobTriggered).toHaveBeenCalled();
  });

  it("disables refresh button when job is running", () => {
    render(
      <TimezoneContext.Provider value="UTC">
        <TableMetadataJobControl
          error={null}
          jobStatus={{
            ...defaultJobStatus,
            currentStatus: api.TableMetadataJobStatus.RUNNING,
          }}
          onJobTriggered={mockOnJobTriggered}
        />
      </TimezoneContext.Provider>,
    );

    expect(screen.getByRole("button")).toBeDisabled();
  });

  it("displays correct tooltip for last refreshed time", async () => {
    render(
      <TimezoneContext.Provider value="UTC">
        <TableMetadataJobControl
          error={null}
          jobStatus={defaultJobStatus}
          onJobTriggered={mockOnJobTriggered}
        />
      </TimezoneContext.Provider>,
    );

    const lastRefreshedElement = screen.getByText(/Last refreshed:/);
    userEvent.hover(lastRefreshedElement);

    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "Data is last refreshed automatically (per cluster setting) or manually.",
    );
  });

  it("displays error message in tooltip when error prop is provided", async () => {
    const errorMessage = "An error occurred";
    render(
      <TimezoneContext.Provider value="UTC">
        <TableMetadataJobControl
          error={errorMessage}
          jobStatus={defaultJobStatus}
          onJobTriggered={mockOnJobTriggered}
        />
      </TimezoneContext.Provider>,
    );

    const lastRefreshedElement = screen.getByText(/Last refreshed:/);
    userEvent.hover(lastRefreshedElement);

    expect(await screen.findByRole("tooltip")).toHaveTextContent(errorMessage);
  });

  it("displays correct tooltip for refresh button when not running", async () => {
    render(
      <TimezoneContext.Provider value="UTC">
        <TableMetadataJobControl
          error={null}
          jobStatus={defaultJobStatus}
          onJobTriggered={mockOnJobTriggered}
        />
      </TimezoneContext.Provider>,
    );

    const refreshButton = screen.getByRole("button");
    userEvent.hover(refreshButton);

    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "Refresh data",
    );
  });

  it("displays correct tooltip for refresh button when running", async () => {
    render(
      <TimezoneContext.Provider value="UTC">
        <TableMetadataJobControl
          error={null}
          jobStatus={{
            ...defaultJobStatus,
            currentStatus: api.TableMetadataJobStatus.RUNNING,
          }}
          onJobTriggered={mockOnJobTriggered}
        />
      </TimezoneContext.Provider>,
    );

    const refreshButton = screen.getByRole("button");
    userEvent.hover(refreshButton);

    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "Data is being refreshed",
    );
  });
});
