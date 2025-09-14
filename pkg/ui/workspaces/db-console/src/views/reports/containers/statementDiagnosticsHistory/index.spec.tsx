// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";
import { render, screen } from "@testing-library/react";
import Long from "long";
import moment from "moment-timezone";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import { StatementDiagnosticsHistoryView } from "./index";

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
      id: Long.fromString("123"),
      key: {
        key_data: {
          query: "SELECT * FROM table",
          implicit_txn: true,
        },
      },
    })),
    onCancelRequest: jest.fn(),
    refresh: jest.fn(),
  };

  const renderComponent = (props = defaultProps) => {
    return render(
      <MemoryRouter>
        <StatementDiagnosticsHistoryView {...props} />
      </MemoryRouter>,
    );
  };

  it("renders the component with diagnostics reports", () => {
    renderComponent();

    expect(screen.getByText("1 diagnostics bundles")).toBeTruthy();
    expect(screen.getByText("SELECT FROM table")).toBeTruthy();
    expect(screen.getByText("Cancel request")).toBeTruthy();
  });

  it("displays empty state when no reports", () => {
    renderComponent({
      ...defaultProps,
      diagnosticsReports: [],
    });

    expect(screen.getByText("0 diagnostics bundles")).toBeTruthy();
    expect(screen.getByText("No statement diagnostics to show")).toBeTruthy();
  });

  it("shows download button for completed reports", () => {
    const completedReport = createMockReport({
      completed: true,
      statement_diagnostics_id: "completed-diag-1",
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

  it("renders statement as link when statement data is available", () => {
    const mockStatement = {
      id: Long.fromString("123"),
      key: {
        key_data: {
          query: "SELECT * FROM table",
          implicit_txn: true,
        },
      },
    };

    renderComponent({
      ...defaultProps,
      getStatementByFingerprint: jest.fn(() => mockStatement),
    });

    const linkElement = screen.getByRole("link");
    expect(linkElement.getAttribute("href")).toBe("/statement/true/123");
  });

  it("calls onCancelRequest when cancel button is clicked", async () => {
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

  it("renders statement as text when statement data is not available", () => {
    renderComponent({
      ...defaultProps,
      getStatementByFingerprint: jest.fn(() => undefined),
    });

    expect(screen.getByText("SELECT FROM table")).toBeTruthy();
    expect(screen.queryByRole("link")).not.toBeTruthy();
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
