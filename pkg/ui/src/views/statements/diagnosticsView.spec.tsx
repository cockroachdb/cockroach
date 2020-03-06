// Copyright 2020 The Cockroach Authors.
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
import sinon, { SinonSpy } from "sinon";
import Long from "long";

import "src/enzymeInit";
import { DiagnosticsView, EmptyDiagnosticsView } from "./diagnosticsView";
import { Table } from "oss/src/components";
import { connectedMount } from "src/test-utils";
import { cockroach } from "src/js/protos";
import IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;

const sandbox = sinon.createSandbox();

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
        <DiagnosticsView
          statementFingerprint={statementFingerprint}
          activate={activateFn}
          hasData={false}
          diagnosticsReports={[]}
        />);
    });

    it("renders EmptyDiagnosticsView component when no diagnostics data provided", () => {
      assert.isTrue(wrapper.find(EmptyDiagnosticsView).exists());
    });

    it("calls activate callback with statementId when click on Activate button", () => {
      const activateButtonComponent = wrapper.find(".crl-button").first();
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

      wrapper = connectedMount(() => (
        <DiagnosticsView
          statementFingerprint={statementFingerprint}
          activate={activateFn}
          hasData={true}
          diagnosticsReports={diagnosticsRequests}
        />),
      );
    });

    it("renders Table component when diagnostics data is provided", () => {
      assert.isTrue(wrapper.find(Table).exists());
    });

    it("calls activate callback with statementId when click on Activate button", () => {
      const activateButtonComponent = wrapper.find(".crl-button").first();
      activateButtonComponent.simulate("click");
      activateFn.calledOnceWith(statementFingerprint);
    });

    it("Activate button is disabled if diagnostics is requested and waiting query", () => {
      const diagnosticsRequests: IStatementDiagnosticsReport[] = [
        generateDiagnosticsRequest({ completed: false }),
        generateDiagnosticsRequest(),
      ];
      wrapper = connectedMount(() => (
        <DiagnosticsView
          statementFingerprint={statementFingerprint}
          activate={activateFn}
          hasData={true}
          diagnosticsReports={diagnosticsRequests}
        />),
      );

      const activateButtonComponent = wrapper.find(".crl-button").first();
      assert.isTrue(wrapper.find(".crl-button.crl-button--disabled").exists());

      activateButtonComponent.simulate("click");
      assert.isTrue(activateFn.notCalled, "Activate button is called when diagnostic is already requested");
    });
  });
});

function generateDiagnosticsRequest(extendObject: Partial<IStatementDiagnosticsReport> = {}): IStatementDiagnosticsReport {
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
