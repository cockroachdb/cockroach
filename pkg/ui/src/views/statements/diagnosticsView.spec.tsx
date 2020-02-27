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

import "src/enzymeInit";
import { DiagnosticsView, EmptyDiagnosticsView } from "./diagnosticsView";
import { Table } from "oss/src/components";
import { connectedMount } from "oss/src/test-utils";

const sandbox = sinon.createSandbox();

describe("DiagnosticsView", () => {
  let wrapper: ReactWrapper;
  let activateFn: SinonSpy;
  const statementId = "some-id";

  beforeEach(() => {
    sandbox.reset();
    activateFn = sandbox.spy();
  });

  describe("With Empty state", () => {
    beforeEach(() => {
      const statement = {
        key: {
          key_data: {
            query: "SELECT key, value, \"lastUpdated\" FROM system.ui WHERE key IN ($1, $2)",
            app: "$ internal-admin-getUIData",
            distSQL: false,
            failed: false,
            opt: true,
            implicit_txn: true,
          },
          node_id: 1,
        },
        stats: {
          count: "1",
          first_attempt_count: "1",
          max_retries: "0",
          legacy_last_err: "",
          num_rows: {
            mean: 0,
            squared_diffs: 0,
          },
          parse_lat: {
            mean: 0,
            squared_diffs: 0,
          },
          plan_lat: {
            mean: 0.000582,
            squared_diffs: 0,
          },
          run_lat: {
            mean: 0.00087,
            squared_diffs: 0,
          },
          service_lat: {
            mean: 0.003969,
            squared_diffs: 0,
          },
          overhead_lat: {
            mean: 0.002517,
            squared_diffs: 0,
          },
          legacy_last_err_redacted: "",
          sensitive_info: {
            last_err: "",
            most_recent_plan_description: {
              name: "scan",
              attrs: [
                {
                  key: "table",
                  value: "ui@primary",
                },
                {
                  key: "spans",
                  value: "2 spans",
                },
                {
                  key: "parallel",
                  value: "",
                },
              ],
            },
            most_recent_plan_timestamp: {
              seconds: "1582809961",
              nanos: 377331000,
            },
          },
          bytes_read: "0",
          rows_read: "0",
        },
      };

      wrapper = mount(
        <DiagnosticsView
          statementId={statementId}
          activate={activateFn}
          hasData={false}
          statement={statement}
        />);
    });

    it("renders EmptyDiagnosticsView component when no diagnostics data provided", () => {
      assert.isTrue(wrapper.find(EmptyDiagnosticsView).exists());
    });

    it("calls activate callback with statementId when click on Activate button", () => {
      const activateButtonComponent = wrapper.find(".crl-button").first();
      activateButtonComponent.simulate("click");
      activateFn.calledOnceWith(statementId);
    });
  });

  describe("With tracing data", () => {
    beforeEach(() => {
      const statement = {
        key: {
          key_data: {
            query: "SELECT key, value, \"lastUpdated\" FROM system.ui WHERE key IN ($1, $2)",
            app: "$ internal-admin-getUIData",
            distSQL: false,
            failed: false,
            opt: true,
            implicit_txn: true,
          },
          node_id: 1,
        },
        stats: {
          count: "1",
          first_attempt_count: "1",
          max_retries: "0",
          legacy_last_err: "",
          num_rows: {
            mean: 0,
            squared_diffs: 0,
          },
          parse_lat: {
            mean: 0,
            squared_diffs: 0,
          },
          plan_lat: {
            mean: 0.000582,
            squared_diffs: 0,
          },
          run_lat: {
            mean: 0.00087,
            squared_diffs: 0,
          },
          service_lat: {
            mean: 0.003969,
            squared_diffs: 0,
          },
          overhead_lat: {
            mean: 0.002517,
            squared_diffs: 0,
          },
          legacy_last_err_redacted: "",
          sensitive_info: {
            last_err: "",
            most_recent_plan_description: {
              name: "scan",
              attrs: [
                {
                  key: "table",
                  value: "ui@primary",
                },
                {
                  key: "spans",
                  value: "2 spans",
                },
                {
                  key: "parallel",
                  value: "",
                },
              ],
            },
            most_recent_plan_timestamp: {
              seconds: "1582809961",
              nanos: 377331000,
            },
          },
          bytes_read: "0",
          rows_read: "0",
        },
        diagnostics: [
          {
            uuid: Date.now().toString(),
            initiated_at: new Date,
            collected_at: new Date,
            status: "READY",
          },
        ],
      };

      wrapper = connectedMount(() => (
        <DiagnosticsView
          statementId={statementId}
          activate={activateFn}
          hasData={true}
          statement={statement}
        />),
      );
    });

    it("renders Table component when diagnostics data is provided", () => {
      assert.isTrue(wrapper.find(Table).exists());
    });

    it("calls activate callback with statementId when click on Activate button", () => {
      const activateButtonComponent = wrapper.find(".crl-button").first();
      activateButtonComponent.simulate("click");
      activateFn.calledOnceWith(statementId);
    });

    it("Activate button is disabled if diagnostics is requested and waiting query", () => {
      const statement = {
        key: {
          key_data: {
            query: "SELECT key, value, \"lastUpdated\" FROM system.ui WHERE key IN ($1, $2)",
            app: "$ internal-admin-getUIData",
            distSQL: false,
            failed: false,
            opt: true,
            implicit_txn: true,
          },
          node_id: 1,
        },
        stats: {
          count: "1",
          first_attempt_count: "1",
          max_retries: "0",
          legacy_last_err: "",
          num_rows: {
            mean: 0,
            squared_diffs: 0,
          },
          parse_lat: {
            mean: 0,
            squared_diffs: 0,
          },
          plan_lat: {
            mean: 0.000582,
            squared_diffs: 0,
          },
          run_lat: {
            mean: 0.00087,
            squared_diffs: 0,
          },
          service_lat: {
            mean: 0.003969,
            squared_diffs: 0,
          },
          overhead_lat: {
            mean: 0.002517,
            squared_diffs: 0,
          },
          legacy_last_err_redacted: "",
          sensitive_info: {
            last_err: "",
            most_recent_plan_description: {
              name: "scan",
              attrs: [
                {
                  key: "table",
                  value: "ui@primary",
                },
                {
                  key: "spans",
                  value: "2 spans",
                },
                {
                  key: "parallel",
                  value: "",
                },
              ],
            },
            most_recent_plan_timestamp: {
              seconds: "1582809961",
              nanos: 377331000,
            },
          },
          bytes_read: "0",
          rows_read: "0",
        },
        diagnostics: [
          {
            uuid: Date.now().toString(),
            initiated_at: new Date,
            status: "WAITING FOR QUERY",
          },
        ],
      };

      wrapper = connectedMount(() => (
        <DiagnosticsView
          statementId={statementId}
          activate={activateFn}
          hasData={true}
          statement={statement}
        />),
      );

      const activateButtonComponent = wrapper.find(".crl-button").first();
      assert.isTrue(wrapper.find(".crl-button.crl-button--disabled").exists());

      activateButtonComponent.simulate("click");
      assert.isTrue(activateFn.notCalled);
    });
  });
});
