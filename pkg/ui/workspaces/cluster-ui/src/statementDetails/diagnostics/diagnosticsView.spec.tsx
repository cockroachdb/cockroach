// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { assert } from "chai";
import { mount, ReactWrapper } from "enzyme";
import { MemoryRouter } from "react-router-dom";
import { Button } from "@cockroachlabs/ui-components";

import { DiagnosticsView } from "./diagnosticsView";
import { Table } from "src/table";
import { TestStoreProvider } from "src/test-utils";
import { StatementDiagnosticsReport } from "../../api";
import moment from "moment";

const activateDiagnosticsRef = { current: { showModalFor: jest.fn() } };

function generateDiagnosticsRequest(
  extendObject: Partial<StatementDiagnosticsReport> = {},
): StatementDiagnosticsReport {
  const requestedAt = moment.now();
  const report: StatementDiagnosticsReport = {
    id: "124354678574635",
    statement_fingerprint: "SELECT * FROM table",
    completed: true,
    requested_at: moment(requestedAt),
    min_execution_latency: moment.duration(10),
    expires_at: moment(requestedAt + 10),
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
            hasData={false}
            diagnosticsReports={[]}
            dismissAlertMessage={() => {}}
          />
        </MemoryRouter>,
      );
    });

    it("opens the statement diagnostics modal when Activate button is clicked", () => {
      const activateButtonComponent = wrapper.find(Button).first();
      activateButtonComponent.simulate("click");
      expect(activateDiagnosticsRef.current.showModalFor).toBeCalledWith(
        statementFingerprint,
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
            hasData={true}
            diagnosticsReports={diagnosticsRequests}
            dismissAlertMessage={() => {}}
          />
        </TestStoreProvider>,
      );
    });

    it("renders Table component when diagnostics data is provided", () => {
      assert.isTrue(wrapper.find(Table).exists());
    });

    it("opens the statement diagnostics modal when Activate button is clicked", () => {
      const activateButtonComponent = wrapper
        .findWhere(n => n.prop("children") === "Activate diagnostics")
        .first();
      activateButtonComponent.simulate("click");
      expect(activateDiagnosticsRef.current.showModalFor).toBeCalledWith(
        statementFingerprint,
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
            hasData={true}
            diagnosticsReports={diagnosticsRequests}
            dismissAlertMessage={() => {}}
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
            hasData={true}
            diagnosticsReports={diagnosticsRequests}
            dismissAlertMessage={() => {}}
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
