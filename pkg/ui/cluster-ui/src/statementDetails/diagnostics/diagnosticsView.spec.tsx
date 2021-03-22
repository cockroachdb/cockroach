import React from "react";
import { assert } from "chai";
import { mount, ReactWrapper } from "enzyme";
import sinon, { SinonSpy } from "sinon";
import Long from "long";
import { MemoryRouter } from "react-router-dom";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { Button } from "@cockroachlabs/ui-components";

import { DiagnosticsView } from "./diagnosticsView";
import { Table } from "src/table";
import { TestStoreProvider } from "src/test-utils";

type IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;

const sandbox = sinon.createSandbox();

function generateDiagnosticsRequest(
  extendObject: Partial<IStatementDiagnosticsReport> = {},
): IStatementDiagnosticsReport {
  const diagnosticsRequest = {
    statement_fingerprint: "SELECT * FROM table",
    completed: true,
    requested_at: {
      seconds: Long.fromNumber(Date.now()),
      nanos: Math.random() * 1000000,
    },
  };
  Object.assign(diagnosticsRequest, extendObject);
  return diagnosticsRequest;
}

describe("DiagnosticsView", () => {
  let wrapper: ReactWrapper;
  let activateFn: SinonSpy;
  const statementFingerprint = "some-id";

  beforeEach(() => {
    sandbox.reset();
    activateFn = sandbox.spy();
  });

  describe("With Empty state", () => {
    beforeEach(() => {
      wrapper = mount(
        <MemoryRouter>
          <DiagnosticsView
            statementFingerprint={statementFingerprint}
            activate={activateFn}
            hasData={false}
            diagnosticsReports={[]}
            dismissAlertMessage={() => {}}
          />
        </MemoryRouter>,
      );
    });

    it("calls activate callback with statementId when click on Activate button", () => {
      const activateButtonComponent = wrapper.find(Button).first();
      activateButtonComponent.simulate("click");
      activateFn.calledOnceWith(statementFingerprint);
    });
  });

  describe("With tracing data", () => {
    beforeEach(() => {
      const diagnosticsRequests: IStatementDiagnosticsReport[] = [
        generateDiagnosticsRequest(),
        generateDiagnosticsRequest(),
      ];

      wrapper = mount(
        <TestStoreProvider>
          <DiagnosticsView
            statementFingerprint={statementFingerprint}
            activate={activateFn}
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

    it("calls activate callback with statementId when click on Activate button", () => {
      const activateButtonComponent = wrapper
        .findWhere(n => n.prop("children") === "Activate")
        .first();
      activateButtonComponent.simulate("click");
      activateFn.calledOnceWith(statementFingerprint);
    });

    it("Activate button is hidden if diagnostics is requested and waiting query", () => {
      const diagnosticsRequests: IStatementDiagnosticsReport[] = [
        generateDiagnosticsRequest({ completed: false }),
        generateDiagnosticsRequest(),
      ];
      wrapper = mount(
        <TestStoreProvider>
          <DiagnosticsView
            statementFingerprint={statementFingerprint}
            activate={activateFn}
            hasData={true}
            diagnosticsReports={diagnosticsRequests}
            dismissAlertMessage={() => {}}
          />
        </TestStoreProvider>,
      );
      const activateButtonComponent = wrapper
        .findWhere(n => n.prop("children") === "Activate")
        .first();
      assert.isFalse(activateButtonComponent.exists());
    });
  });
});
