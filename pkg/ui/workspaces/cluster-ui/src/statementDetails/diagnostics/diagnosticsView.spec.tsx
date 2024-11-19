// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Button } from "@cockroachlabs/ui-components";
import { assert } from "chai";
import { mount, ReactWrapper } from "enzyme";
import moment from "moment-timezone";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import { SortedTable } from "src/sortedtable";
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
  let wrapper: ReactWrapper;
  const statementFingerprint = "some-id";

  describe("With Empty state", () => {
    beforeEach(() => {
      wrapper = mount(
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
    });

    it("opens the statement diagnostics modal when Activate button is clicked", () => {
      const activateButtonComponent = wrapper.find(Button).first();
      activateButtonComponent.simulate("click");
      expect(activateDiagnosticsRef.current.showModalFor).toBeCalledWith(
        statementFingerprint,
        ["gist"],
      );
    });
  });

  describe("With tracing data", () => {
    beforeEach(() => {
      const diagnosticsRequests: StatementDiagnosticsReport[] = [
        generateDiagnosticsRequest(),
        generateDiagnosticsRequest(),
      ];

      wrapper = mount(
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
    });

    it("renders Table component when diagnostics data is provided", () => {
      assert.isTrue(wrapper.find(SortedTable).exists());
    });

    it("opens the statement diagnostics modal when Activate button is clicked", () => {
      const activateButtonComponent = wrapper
        .findWhere(n => n.prop("children") === "Activate diagnostics")
        .first();
      activateButtonComponent.simulate("click");
      expect(activateDiagnosticsRef.current.showModalFor).toBeCalledWith(
        statementFingerprint,
        ["gist"],
      );
    });

    it("Activate button is hidden if diagnostics is requested and waiting query", () => {
      const diagnosticsRequests: StatementDiagnosticsReport[] = [
        generateDiagnosticsRequest({ completed: false }),
        generateDiagnosticsRequest(),
      ];
      wrapper = mount(
        <TestStoreProvider>
          <DiagnosticsView
            activateDiagnosticsRef={activateDiagnosticsRef}
            statementFingerprint={statementFingerprint}
            diagnosticsReports={diagnosticsRequests}
            dismissAlertMessage={() => {}}
            currentScale={ts}
            requestTime={requestTime}
            onChangeTimeScale={mockSetTimeScale}
            planGists={["gist"]}
          />
        </TestStoreProvider>,
      );
      const activateButtonComponent = wrapper
        .findWhere(n => n.prop("children") === "Activate diagnostics")
        .first();
      assert.isFalse(activateButtonComponent.exists());
    });

    it("Cancel request button shows if diagnostics is requested and waiting query", () => {
      const diagnosticsRequests: StatementDiagnosticsReport[] = [
        generateDiagnosticsRequest({ completed: false }),
        generateDiagnosticsRequest(),
      ];
      wrapper = mount(
        <TestStoreProvider>
          <DiagnosticsView
            activateDiagnosticsRef={activateDiagnosticsRef}
            statementFingerprint={statementFingerprint}
            diagnosticsReports={diagnosticsRequests}
            dismissAlertMessage={() => {}}
            currentScale={ts}
            requestTime={requestTime}
            onChangeTimeScale={mockSetTimeScale}
            planGists={["gist"]}
          />
        </TestStoreProvider>,
      );
      const cancelButtonComponent = wrapper
        .findWhere(n => n.prop("children") === "Cancel request")
        .first();
      assert.isTrue(cancelButtonComponent.exists());
    });
  });
});
