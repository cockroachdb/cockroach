// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import "@testing-library/jest-dom";
import { render, screen, fireEvent } from "@testing-library/react";
import moment from "moment-timezone";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import { TestStoreProvider } from "src/test-utils";
import { TimeScale } from "src/timeScaleDropdown";

import { StatementDiagnosticsReport } from "../../api";

import { DiagnosticsView } from "./diagnosticsView";

const activateDiagnosticsRef = { current: { showModalFor: jest.fn() } };
const ts: TimeScale = {
  windowSize: moment.duration(20, "day"),
  sampleSize: moment.duration(5, "minutes"),
  fixedWindowEnd: moment.utc("2023.01.5"),
  key: "Custom",
};
const mockSetTimeScale = jest.fn();
const requestTime = moment();

function generateDiagnosticsRequest(
  extendObject: Partial<StatementDiagnosticsReport> = {},
): StatementDiagnosticsReport {
  const requestedAt = moment("2023-01-01 00:00:00");
  const report: StatementDiagnosticsReport = {
    id: "124354678574635",
    statement_fingerprint: "SELECT * FROM table",
    completed: true,
    requested_at: moment(requestedAt),
    min_execution_latency: moment.duration(10),
    expires_at: moment("2023-01-01 00:00:10"),
  };
  Object.assign(report, extendObject);
  return report;
}

describe("DiagnosticsView", () => {
  const statementFingerprint = "some-id";

  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe("With Empty state", () => {
    it("opens the statement diagnostics modal when Activate button is clicked", () => {
      render(
        <MemoryRouter>
          <DiagnosticsView
            activateDiagnosticsRef={activateDiagnosticsRef}
            statementFingerprint={statementFingerprint}
            diagnosticsReports={[]}
            dismissAlertMessage={() => {}}
            requestTime={undefined}
            currentScale={ts}
            onChangeTimeScale={mockSetTimeScale}
            planGists={["gist"]}
          />
        </MemoryRouter>,
      );

      const activateButton = screen.getByRole("button", { name: /activate/i });
      fireEvent.click(activateButton);

      expect(activateDiagnosticsRef.current.showModalFor).toHaveBeenCalledWith(
        statementFingerprint,
        ["gist"],
      );
    });
  });

  describe("With tracing data", () => {
    const diagnosticsRequests: StatementDiagnosticsReport[] = [
      generateDiagnosticsRequest(),
      generateDiagnosticsRequest(),
    ];

    it("renders Table component when diagnostics data is provided", () => {
      render(
        <TestStoreProvider>
          <DiagnosticsView
            activateDiagnosticsRef={activateDiagnosticsRef}
            statementFingerprint={statementFingerprint}
            requestTime={undefined}
            diagnosticsReports={diagnosticsRequests}
            dismissAlertMessage={() => {}}
            currentScale={ts}
            onChangeTimeScale={mockSetTimeScale}
            planGists={["gist"]}
          />
        </TestStoreProvider>,
      );

      expect(screen.getByRole("table")).toBeInTheDocument();
    });

    it("opens the statement diagnostics modal when Activate button is clicked", () => {
      render(
        <TestStoreProvider>
          <DiagnosticsView
            activateDiagnosticsRef={activateDiagnosticsRef}
            statementFingerprint={statementFingerprint}
            requestTime={undefined}
            diagnosticsReports={diagnosticsRequests}
            dismissAlertMessage={() => {}}
            currentScale={ts}
            onChangeTimeScale={mockSetTimeScale}
            planGists={["gist"]}
          />
        </TestStoreProvider>,
      );

      const activateButton = screen.getByRole("button", {
        name: /activate diagnostics/i,
      });
      fireEvent.click(activateButton);

      expect(activateDiagnosticsRef.current.showModalFor).toHaveBeenCalledWith(
        statementFingerprint,
        ["gist"],
      );
    });

    it("Activate button is hidden if diagnostics is requested and waiting query", () => {
      const pendingDiagnosticsRequests: StatementDiagnosticsReport[] = [
        generateDiagnosticsRequest({ completed: false }),
        generateDiagnosticsRequest(),
      ];

      render(
        <TestStoreProvider>
          <DiagnosticsView
            activateDiagnosticsRef={activateDiagnosticsRef}
            statementFingerprint={statementFingerprint}
            diagnosticsReports={pendingDiagnosticsRequests}
            dismissAlertMessage={() => {}}
            currentScale={ts}
            requestTime={requestTime}
            onChangeTimeScale={mockSetTimeScale}
            planGists={["gist"]}
          />
        </TestStoreProvider>,
      );

      expect(
        screen.queryByRole("button", { name: /activate diagnostics/i }),
      ).not.toBeInTheDocument();
    });

    it("Cancel request button shows if diagnostics is requested and waiting query", () => {
      const pendingDiagnosticsRequests: StatementDiagnosticsReport[] = [
        generateDiagnosticsRequest({ completed: false }),
        generateDiagnosticsRequest(),
      ];

      render(
        <TestStoreProvider>
          <DiagnosticsView
            activateDiagnosticsRef={activateDiagnosticsRef}
            statementFingerprint={statementFingerprint}
            diagnosticsReports={pendingDiagnosticsRequests}
            dismissAlertMessage={() => {}}
            currentScale={ts}
            requestTime={requestTime}
            onChangeTimeScale={mockSetTimeScale}
            planGists={["gist"]}
          />
        </TestStoreProvider>,
      );

      expect(
        screen.getByRole("button", { name: /cancel request/i }),
      ).toBeInTheDocument();
    });
  });
});
