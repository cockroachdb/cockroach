// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";
import { render, screen } from "@testing-library/react";
import moment from "moment-timezone";
import React from "react";
import { Provider } from "react-redux";
import { MemoryRouter } from "react-router-dom";
import { createStore } from "redux";

import {
  selectStatementDiagnosticsReports,
  selectStatementByFingerprint,
  statementDiagnosticsReportsInFlight,
} from "src/redux/statements/statementsSelectors";

import StatementDiagnosticsHistoryView from "./index";

// Mock analytics tracking
jest.mock("src/util/analytics", () => ({
  trackDownloadDiagnosticsBundle: jest.fn(),
}));

// Mock Redux selectors
jest.mock("src/redux/statements/statementsSelectors", () => ({
  selectStatementDiagnosticsReports: jest.fn(),
  selectStatementByFingerprint: jest.fn(),
  statementDiagnosticsReportsInFlight: jest.fn(),
}));

// Mock Redux actions
jest.mock("src/redux/apiReducers", () => ({
  invalidateStatementDiagnosticsRequests: jest.fn(() => ({
    type: "INVALIDATE",
  })),
  refreshStatementDiagnosticsRequests: jest.fn(() => ({ type: "REFRESH" })),
}));

jest.mock("src/redux/statements", () => ({
  cancelStatementDiagnosticsReportAction: jest.fn(() => ({ type: "CANCEL" })),
}));

jest.mock("src/redux/analyticsActions", () => ({
  trackCancelDiagnosticsBundleAction: jest.fn(() => ({ type: "TRACK_CANCEL" })),
}));

const mockStore = createStore((state = {}) => state);

describe("StatementDiagnosticsHistoryView", () => {
  const createMockReport = (
    overrides: Partial<clusterUiApi.StatementDiagnosticsReport> = {},
  ): clusterUiApi.StatementDiagnosticsReport => ({
    id: "1",
    statement_fingerprint: "SELECT * FROM table",
    completed: false,
    statement_diagnostics_id: "diag-1",
    requested_at: moment("2023-01-01T10:00:00Z"),
    min_execution_latency: moment.duration(100, "milliseconds"),
    expires_at: moment("2023-01-02T10:00:00Z"),
    ...overrides,
  });

  const defaultProps = {
    loading: false,
    diagnosticsReports: [createMockReport()],
    getStatementByFingerprint: jest.fn(() => ({
      id: 123,
      key: {
        key_data: {
          query: "SELECT * FROM table",
          implicit_txn: "true",
        },
      },
    })),
    onDiagnosticCancelRequest: jest.fn(),
    refresh: jest.fn(),
  };

  beforeEach(() => {
    jest.clearAllMocks();
    // Set up default mock return values
    (
      statementDiagnosticsReportsInFlight as unknown as jest.Mock
    ).mockReturnValue(false);
    (selectStatementDiagnosticsReports as unknown as jest.Mock).mockReturnValue(
      [createMockReport()],
    );
    (selectStatementByFingerprint as unknown as jest.Mock).mockReturnValue({
      id: 123,
      key: {
        key_data: {
          query: "SELECT * FROM table",
          implicit_txn: "true",
        },
      },
    });
  });

  const renderComponent = (props = defaultProps) => {
    return render(
      <Provider store={mockStore}>
        <MemoryRouter>
          <StatementDiagnosticsHistoryView {...props} />
        </MemoryRouter>
      </Provider>,
    );
  };

  it("renders the component with diagnostics reports", () => {
    renderComponent();

    expect(screen.getByText("1 diagnostics bundles")).toBeTruthy();
    expect(screen.getByText("SELECT FROM table")).toBeTruthy();
    expect(screen.getByText("Cancel request")).toBeTruthy();
  });

  it("shows loading state when loading", () => {
    (
      statementDiagnosticsReportsInFlight as unknown as jest.Mock
    ).mockReturnValue(true);
    renderComponent();

    // The SortedTable component shows loading state internally
    expect(screen.getByText("1 diagnostics bundles")).toBeTruthy();
  });

  it("displays empty state when no reports", () => {
    (selectStatementDiagnosticsReports as unknown as jest.Mock).mockReturnValue(
      [],
    );
    renderComponent();

    expect(screen.getByText("0 diagnostics bundles")).toBeTruthy();
    expect(screen.getByText("No statement diagnostics to show")).toBeTruthy();
    expect(
      screen.getByText(/Statement diagnostics can help when troubleshooting/),
    ).toBeTruthy();
  });

  it("shows download button for completed reports", () => {
    const completedReport = createMockReport({
      completed: true,
      statement_diagnostics_id: "completed-diag-1",
    });
    (selectStatementDiagnosticsReports as unknown as jest.Mock).mockReturnValue(
      [completedReport],
    );
    renderComponent();

    expect(screen.getByText("Bundle (.zip)")).toBeTruthy();
    expect(screen.queryByText("Cancel request")).not.toBeTruthy();
  });

  it("shows cancel button for pending reports", () => {
    const pendingReport = createMockReport({
      completed: false,
    });
    (selectStatementDiagnosticsReports as unknown as jest.Mock).mockReturnValue(
      [pendingReport],
    );
    renderComponent();

    expect(screen.getByText("Cancel request")).toBeTruthy();
    expect(screen.queryByText("Bundle (.zip)")).not.toBeTruthy();
  });

  it("displays proper table count for multiple reports", () => {
    const reports = Array.from({ length: 5 }, (_, i) =>
      createMockReport({ id: i.toString() }),
    );

    (selectStatementDiagnosticsReports as unknown as jest.Mock).mockReturnValue(
      reports,
    );
    renderComponent();

    expect(screen.getByText("5 diagnostics bundles")).toBeTruthy();
  });

  it("renders statement as link when statement data is available", () => {
    const mockStatement = {
      id: 123,
      key: {
        key_data: {
          query: "SELECT * FROM table",
          implicit_txn: "true",
        },
      },
    };

    renderComponent({
      ...defaultProps,
      getStatementByFingerprint: jest.fn(() => mockStatement),
    });

    // Should render as a link to statement details
    const linkElement = screen.getByRole("link");
    expect(linkElement.getAttribute("href")).toBe("/statement/true/123");
  });
});
