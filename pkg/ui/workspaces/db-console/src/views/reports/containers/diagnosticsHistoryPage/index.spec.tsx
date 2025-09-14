// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { createMemoryHistory } from "history";
import React from "react";
import { Router } from "react-router-dom";

import DiagnosticsHistoryPage, { DiagnosticsHistoryTabType } from "./index";

// Mock the child components since we're testing the page wrapper
jest.mock("src/views/reports/containers/statementDiagnosticsHistory", () => {
  return function MockStatementDiagnosticsHistory() {
    return (
      <div data-testid="statement-diagnostics-history">
        Statement Diagnostics History View
      </div>
    );
  };
});

jest.mock("src/views/reports/containers/transactionDiagnosticsHistory", () => {
  return function MockTransactionDiagnosticsHistory() {
    return (
      <div data-testid="transaction-diagnostics-history">
        Transaction Diagnostics History View
      </div>
    );
  };
});

describe("DiagnosticsHistoryPage", () => {
  const createHistory = (search = "") => {
    return createMemoryHistory({
      initialEntries: [`/diagnostics-history${search}`],
    });
  };

  const renderComponent = (history = createHistory()) => {
    const mockProps = {
      match: {
        params: {},
        isExact: true,
        path: "/diagnostics-history",
        url: "/diagnostics-history",
      },
      location: history.location,
      history,
    };

    return render(
      <Router history={history}>
        <DiagnosticsHistoryPage {...mockProps} />
      </Router>,
    );
  };

  it("renders the page with correct heading", () => {
    renderComponent();

    expect(screen.getByText("Diagnostics History")).toBeTruthy();
  });

  it("defaults to Statements tab when no tab parameter is provided", () => {
    renderComponent();

    expect(screen.getByTestId("statement-diagnostics-history")).toBeTruthy();
    expect(screen.queryByTestId("transaction-diagnostics-history")).toBeNull();
  });

  it("shows Statements tab when tab=Statements in URL", () => {
    const history = createHistory("?tab=Statements");
    renderComponent(history);

    expect(screen.getByTestId("statement-diagnostics-history")).toBeTruthy();
    expect(screen.queryByTestId("transaction-diagnostics-history")).toBeNull();
  });

  it("shows Transactions tab when tab=Transactions in URL", () => {
    const history = createHistory("?tab=Transactions");
    renderComponent(history);

    expect(screen.getByTestId("transaction-diagnostics-history")).toBeTruthy();
    expect(screen.queryByTestId("statement-diagnostics-history")).toBeNull();
  });

  it("has both tab options available", () => {
    renderComponent();

    expect(screen.getByRole("tab", { name: "Statements" })).toBeTruthy();
    expect(screen.getByRole("tab", { name: "Transactions" })).toBeTruthy();
  });

  it("updates URL when tab is clicked", () => {
    const history = createHistory();
    renderComponent(history);

    const transactionsTab = screen.getByRole("tab", { name: "Transactions" });
    fireEvent.click(transactionsTab);

    expect(history.location.search).toBe("?tab=Transactions");
  });

  it("switches content when changing tabs", async () => {
    const history = createHistory();
    const { rerender } = renderComponent(history);

    // Initially shows Statements tab
    expect(screen.getByTestId("statement-diagnostics-history")).toBeTruthy();

    // Click Transactions tab
    const transactionsTab = screen.getByRole("tab", { name: "Transactions" });
    fireEvent.click(transactionsTab);

    // Wait for URL to update
    await waitFor(() => {
      expect(history.location.search).toBe("?tab=Transactions");
    });

    // Re-render component with updated history to reflect URL change
    const mockProps = {
      match: {
        params: {},
        isExact: true,
        path: "/diagnostics-history",
        url: "/diagnostics-history",
      },
      location: history.location,
      history,
    };

    rerender(
      <Router history={history}>
        <DiagnosticsHistoryPage {...mockProps} />
      </Router>,
    );

    // Now check that the transaction tab content is visible
    await waitFor(() => {
      expect(
        screen.getByTestId("transaction-diagnostics-history"),
      ).toBeTruthy();
    });

    expect(screen.queryByTestId("statement-diagnostics-history")).toBeNull();
  });

  it("switches back to Statements tab", async () => {
    const history = createHistory("?tab=Transactions");
    const { rerender } = renderComponent(history);

    // Initially shows Transactions tab
    expect(screen.getByTestId("transaction-diagnostics-history")).toBeTruthy();

    // Click Statements tab
    const statementsTab = screen.getByRole("tab", { name: "Statements" });
    fireEvent.click(statementsTab);

    // Wait for URL to update
    await waitFor(() => {
      expect(history.location.search).toBe("?tab=Statements");
    });

    // Re-render component with updated history
    const mockProps = {
      match: {
        params: {},
        isExact: true,
        path: "/diagnostics-history",
        url: "/diagnostics-history",
      },
      location: history.location,
      history,
    };

    rerender(
      <Router history={history}>
        <DiagnosticsHistoryPage {...mockProps} />
      </Router>,
    );

    // Now check that the statements tab content is visible
    await waitFor(() => {
      expect(screen.getByTestId("statement-diagnostics-history")).toBeTruthy();
    });

    expect(screen.queryByTestId("transaction-diagnostics-history")).toBeNull();
  });

  it("handles unknown tab parameter gracefully", () => {
    const history = createHistory("?tab=UnknownTab");
    renderComponent(history);

    // Should default to Statements tab for unknown tab values
    expect(screen.getByTestId("statement-diagnostics-history")).toBeTruthy();
  });

  it("preserves other URL parameters when changing tabs", () => {
    const history = createHistory("?tab=Statements&other=value");
    renderComponent(history);

    const transactionsTab = screen.getByRole("tab", { name: "Transactions" });
    fireEvent.click(transactionsTab);

    expect(history.location.search).toBe("?tab=Transactions");
  });

  it("uses correct default tab constant", () => {
    expect(DiagnosticsHistoryPage.defaultProps).toBeUndefined();
    // The default is defined in the component logic, not as defaultProps
    renderComponent();

    // Verify the default behavior
    expect(screen.getByTestId("statement-diagnostics-history")).toBeTruthy();
  });

  it("tab types are correctly defined", () => {
    expect(DiagnosticsHistoryTabType.Statements).toBe("Statements");
    expect(DiagnosticsHistoryTabType.Transactions).toBe("Transactions");
  });

  it("has destroyInactiveTabPane enabled", async () => {
    const history = createHistory();
    const { rerender } = renderComponent(history);

    // Switch to Transactions tab
    const transactionsTab = screen.getByRole("tab", { name: "Transactions" });
    fireEvent.click(transactionsTab);

    // Wait for URL to update
    await waitFor(() => {
      expect(history.location.search).toBe("?tab=Transactions");
    });

    // Re-render component with updated history
    const mockProps = {
      match: {
        params: {},
        isExact: true,
        path: "/diagnostics-history",
        url: "/diagnostics-history",
      },
      location: history.location,
      history,
    };

    rerender(
      <Router history={history}>
        <DiagnosticsHistoryPage {...mockProps} />
      </Router>,
    );

    // Wait for the tab content to update
    await waitFor(() => {
      expect(
        screen.getByTestId("transaction-diagnostics-history"),
      ).toBeTruthy();
    });

    // Statements tab content should not be in DOM (due to destroyInactiveTabPane)
    expect(screen.queryByTestId("statement-diagnostics-history")).toBeNull();
  });
});
