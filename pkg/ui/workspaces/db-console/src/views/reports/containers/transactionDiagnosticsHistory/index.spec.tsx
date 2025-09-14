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
  transactionDiagnosticsReportsInFlight,
  selectTransactionDiagnosticsReports,
} from "oss/src/redux/statements/statementsSelectors";

import TransactionDiagnosticsHistoryView from "./index";

// Mock analytics tracking
jest.mock("src/util/analytics", () => ({
  trackDownloadDiagnosticsBundle: jest.fn(),
}));

// Mock Redux selectors
jest.mock("src/redux/statements/statementsSelectors", () => ({
  selectTransactionDiagnosticsReports: jest.fn(),
  transactionDiagnosticsReportsInFlight: jest.fn(),
}));

// Mock Redux actions
jest.mock("src/redux/apiReducers", () => ({
  invalidateTransactionDiagnosticsRequests: jest.fn(() => ({
    type: "INVALIDATE",
  })),
  refreshTransactionDiagnosticsRequests: jest.fn(() => ({ type: "REFRESH" })),
}));

jest.mock("src/redux/statements", () => ({
  cancelTransactionDiagnosticsReportAction: jest.fn(() => ({ type: "CANCEL" })),
}));

jest.mock("src/redux/analyticsActions", () => ({
  trackCancelDiagnosticsBundleAction: jest.fn(() => ({ type: "TRACK_CANCEL" })),
}));

const mockStore = createStore((state = {}) => state);

describe("TransactionDiagnosticsHistoryView", () => {
  const createMockReport = (
    overrides: Partial<clusterUiApi.TransactionDiagnosticsReport> = {},
  ): clusterUiApi.TransactionDiagnosticsReport => ({
    id: "1",
    transaction_fingerprint: "Transaction fingerprint",
    transaction_fingerprint_id: new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]),
    statement_fingerprint_ids: [new Uint8Array([1, 2, 3])],
    completed: false,
    transaction_diagnostics_id: "txn-diag-1",
    requested_at: moment("2023-01-01T10:00:00Z"),
    min_execution_latency: moment.duration(200, "milliseconds"),
    expires_at: moment("2023-01-02T10:00:00Z"),
    sampling_probability: 0.1,
    redacted: false,
    username: "testuser",
    ...overrides,
  });

  const defaultProps = {
    loading: false,
    diagnosticsReports: [createMockReport()],
    onDiagnosticCancelRequest: jest.fn(),
    refresh: jest.fn(),
  };

  beforeEach(() => {
    jest.clearAllMocks();

    // Set up default mock return values
    (
      transactionDiagnosticsReportsInFlight as unknown as jest.Mock
    ).mockReturnValue(false);
    (
      selectTransactionDiagnosticsReports as unknown as jest.Mock
    ).mockReturnValue([createMockReport()]);
  });

  const renderComponent = (props = defaultProps) => {
    return render(
      <Provider store={mockStore}>
        <MemoryRouter>
          <TransactionDiagnosticsHistoryView {...props} />
        </MemoryRouter>
      </Provider>,
    );
  };

  it("renders the component with diagnostics reports", () => {
    renderComponent();

    expect(screen.getByText("1 diagnostics bundles")).toBeTruthy();
    expect(screen.getByText("Transaction fingerprint")).toBeTruthy();
    expect(screen.getByText("Cancel request")).toBeTruthy();
  });

  it("shows loading state when loading", () => {
    renderComponent({
      ...defaultProps,
      loading: true,
    });

    expect(screen.getByText("1 diagnostics bundles")).toBeTruthy();
  });

  it("displays empty state when no reports", () => {
    (
      selectTransactionDiagnosticsReports as unknown as jest.Mock
    ).mockReturnValue([]);
    renderComponent();

    expect(screen.getByText("0 diagnostics bundles")).toBeTruthy();
    expect(screen.getByText("No transaction diagnostics to show")).toBeTruthy();
    expect(
      screen.getByText(/Transaction diagnostics can help when troubleshooting/),
    ).toBeTruthy();
  });

  it("shows download button for completed reports", () => {
    const completedReport = createMockReport({
      completed: true,
    });

    (
      selectTransactionDiagnosticsReports as unknown as jest.Mock
    ).mockReturnValue([completedReport]);
    renderComponent();

    expect(screen.getByText("Bundle (.zip)")).toBeTruthy();
    expect(screen.queryByText("Cancel request")).not.toBeTruthy();
  });

  it("shows cancel button for pending reports", () => {
    const pendingReport = createMockReport({
      completed: false,
    });

    (
      selectTransactionDiagnosticsReports as unknown as jest.Mock
    ).mockReturnValue([pendingReport]);
    renderComponent();

    expect(screen.getByText("Cancel request")).toBeTruthy();
    expect(screen.queryByText("Bundle (.zip)")).not.toBeTruthy();
  });

  it("displays proper table count for multiple reports", () => {
    const reports = Array.from({ length: 5 }, (_, i) =>
      createMockReport({ id: i.toString() }),
    );

    (
      selectTransactionDiagnosticsReports as unknown as jest.Mock
    ).mockReturnValue(reports);
    renderComponent();

    expect(screen.getByText("5 diagnostics bundles")).toBeTruthy();
  });

  it("renders transaction as link when fingerprint ID is available", () => {
    const report = createMockReport({
      transaction_fingerprint_id: new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]),
      transaction_fingerprint: "Test transaction",
    });

    (
      selectTransactionDiagnosticsReports as unknown as jest.Mock
    ).mockReturnValue([report]);
    renderComponent();

    const linkElement = screen.getByRole("link");
    expect(linkElement.getAttribute("href")).toBe(
      "/transaction/72623859790382856",
    );
  });

  it("handles missing transaction fingerprint gracefully", () => {
    const reportWithoutFingerprint = createMockReport({
      transaction_fingerprint: "",
      transaction_fingerprint_id: new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]),
    });

    (
      selectTransactionDiagnosticsReports as unknown as jest.Mock
    ).mockReturnValue([reportWithoutFingerprint]);
    renderComponent();

    expect(screen.getByText("Transaction 72623859790382856")).toBeTruthy();
  });

  it("handles missing transaction fingerprint ID gracefully", () => {
    const reportWithoutFingerprintId = createMockReport({
      transaction_fingerprint: "Test transaction",
      transaction_fingerprint_id: new Uint8Array([]),
    });

    (
      selectTransactionDiagnosticsReports as unknown as jest.Mock
    ).mockReturnValue([reportWithoutFingerprintId]);
    renderComponent();

    expect(screen.getByText("Test transaction")).toBeTruthy();
  });
});
