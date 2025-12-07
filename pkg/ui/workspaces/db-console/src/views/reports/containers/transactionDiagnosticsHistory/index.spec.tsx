// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";
import { render, screen } from "@testing-library/react";
import moment from "moment-timezone";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import { TransactionDiagnosticsHistoryView } from "./index";

describe("TransactionDiagnosticsHistoryView", () => {
  const createMockReport = (
    overrides: Partial<clusterUiApi.TransactionDiagnosticsReport> = {},
  ): clusterUiApi.TransactionDiagnosticsReport => ({
    id: "1",
    transaction_fingerprint: "Transaction fingerprint",
    transaction_fingerprint_id: BigInt("72623859790382856"),
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
    onCancelRequest: jest.fn(),
  };

  const renderComponent = (props = defaultProps) => {
    return render(
      <MemoryRouter>
        <TransactionDiagnosticsHistoryView {...props} />
      </MemoryRouter>,
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
    renderComponent({
      ...defaultProps,
      diagnosticsReports: [],
    });

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

    renderComponent({
      ...defaultProps,
      diagnosticsReports: [completedReport],
    });

    expect(screen.getByText("Bundle (.zip)")).toBeTruthy();
    expect(screen.queryByText("Cancel request")).not.toBeTruthy();
  });

  it("shows cancel button for pending reports", () => {
    const pendingReport = createMockReport({
      completed: false,
    });

    renderComponent({
      ...defaultProps,
      diagnosticsReports: [pendingReport],
    });

    expect(screen.getByText("Cancel request")).toBeTruthy();
    expect(screen.queryByText("Bundle (.zip)")).not.toBeTruthy();
  });

  it("displays proper table count for multiple reports", () => {
    const reports = Array.from({ length: 5 }, (_, i) =>
      createMockReport({ id: i.toString() }),
    );

    renderComponent({
      ...defaultProps,
      diagnosticsReports: reports,
    });

    expect(screen.getByText("5 diagnostics bundles")).toBeTruthy();
  });

  it("renders transaction as link when fingerprint ID is available", () => {
    const report = createMockReport({
      transaction_fingerprint_id: BigInt("72623859790382856"),
      transaction_fingerprint: "Test transaction",
    });

    renderComponent({
      ...defaultProps,
      diagnosticsReports: [report],
    });

    const linkElement = screen.getByRole("link");
    expect(linkElement.getAttribute("href")).toBe(
      "/transaction/72623859790382856",
    );
  });

  it("handles missing transaction fingerprint gracefully", () => {
    const reportWithoutFingerprint = createMockReport({
      transaction_fingerprint: "",
      transaction_fingerprint_id: BigInt("72623859790382856"),
    });

    renderComponent({
      ...defaultProps,
      diagnosticsReports: [reportWithoutFingerprint],
    });

    expect(screen.getByText("Transaction 72623859790382856")).toBeTruthy();
  });

  it("handles missing transaction fingerprint ID gracefully", () => {
    const reportWithoutFingerprintId = createMockReport({
      transaction_fingerprint: "Test transaction",
      transaction_fingerprint_id: BigInt(0),
    });

    renderComponent({
      ...defaultProps,
      diagnosticsReports: [reportWithoutFingerprintId],
    });

    expect(screen.getByText("Test transaction")).toBeTruthy();
  });

  it("calls onCancelRequest when cancel button is clicked", () => {
    const pendingReport = createMockReport({
      completed: false,
    });

    renderComponent({
      ...defaultProps,
      diagnosticsReports: [pendingReport],
    });

    const cancelButton = screen.getByText("Cancel request");
    cancelButton.click();

    expect(defaultProps.onCancelRequest).toHaveBeenCalledWith({
      ...pendingReport,
      key: 0,
    });
  });

  it("displays proper pagination info for large datasets", () => {
    const reports = Array.from({ length: 20 }, (_, i) =>
      createMockReport({ id: i.toString() }),
    );

    renderComponent({
      ...defaultProps,
      diagnosticsReports: reports,
    });

    expect(screen.getByText("16 of 20 diagnostics bundles")).toBeTruthy();
  });
});
