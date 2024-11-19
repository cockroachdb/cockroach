// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { PayloadAction } from "@reduxjs/toolkit";
import Long from "long";
import moment from "moment-timezone";
import { expectSaga } from "redux-saga-test-plan";
import * as matchers from "redux-saga-test-plan/matchers";
import {
  EffectProviders,
  StaticProvider,
  throwError,
} from "redux-saga-test-plan/providers";

import { getStatementDetails } from "src/api/statementsApi";

import {
  actions,
  reducer,
  SQLDetailsStatsReducerState,
} from "./statementDetails.reducer";
import {
  refreshSQLDetailsStatsSaga,
  requestSQLDetailsStatsSaga,
} from "./statementDetails.sagas";

const lastUpdated = moment();

export type StatementDetailsRequest =
  cockroach.server.serverpb.StatementDetailsRequest;

describe("SQLDetailsStats sagas", () => {
  let spy: jest.SpyInstance;
  beforeAll(() => {
    spy = jest.spyOn(moment, "utc").mockImplementation(() => lastUpdated);
  });

  afterAll(() => {
    spy.mockRestore();
  });

  const action: PayloadAction<StatementDetailsRequest> = {
    payload: new cockroach.server.serverpb.StatementDetailsRequest({
      fingerprint_id: "SELECT * FROM crdb_internal.node_build_info",
      app_names: ["$ cockroach sql", "newname"],
    }),
    type: "request",
  };
  // /start and /end aren't included in the above payload, but the default value
  // when missing (0 for both) appears anyway.
  const key =
    "SELECT * FROM crdb_internal.node_build_info/$ cockroach sql,newname/0/0";
  const SQLDetailsStatsResponse =
    new cockroach.server.serverpb.StatementDetailsResponse({
      statement: {
        metadata: {
          query: "SELECT * FROM crdb_internal.node_build_info",
          app_names: ["$ cockroach sql", "newname"],
          dist_sql_count: new Long(2),
          implicit_txn: true,
          vec_count: new Long(2),
          full_scan_count: new Long(2),
          databases: ["defaultdb"],
          query_summary: "SELECT * FROM crdb_internal.node_build_info",
          formatted_query: "SELECT * FROM crdb_internal.node_build_info\n",
          stmt_type: "DDL",
          total_count: new Long(3),
        },
        stats: {
          count: new Long(5),
          first_attempt_count: new Long(5),
          max_retries: new Long(0),
          legacy_last_err: "",
          legacy_last_err_redacted: "",
          num_rows: {
            mean: 6,
            squared_diffs: 0,
          },
          parse_lat: {
            mean: 0.0000876,
            squared_diffs: 2.35792e-8,
          },
          plan_lat: {
            mean: 0.008131,
            squared_diffs: 0.00127640837,
          },
          run_lat: {
            mean: 0.0002796,
            squared_diffs: 2.401919999999999e-8,
          },
          service_lat: {
            mean: 0.008522,
            squared_diffs: 0.001298238058,
          },
          overhead_lat: {
            mean: 0.000023799999999999972,
            squared_diffs: 5.492799999999973e-9,
          },
          sensitive_info: {
            last_err: "",
            most_recent_plan_description: {
              name: "virtual table",
              attrs: [
                {
                  key: "Table",
                  value: "node_build_info@primary",
                },
              ],
              children: [],
            },
            most_recent_plan_timestamp: {
              seconds: new Long(1614851546),
              nanos: 956814000,
            },
          },
          bytes_read: {
            mean: 0,
            squared_diffs: 0,
          },
          rows_read: {
            mean: 0,
            squared_diffs: 0,
          },
          rows_written: {
            mean: 0,
            squared_diffs: 0,
          },
          exec_stats: {
            count: new Long(5),
            network_bytes: {
              mean: 0,
              squared_diffs: 0,
            },
            max_mem_usage: {
              mean: 10240,
              squared_diffs: 0,
            },
            contention_time: {
              mean: 0,
              squared_diffs: 0,
            },
            network_messages: {
              mean: 0,
              squared_diffs: 0,
            },
            max_disk_usage: {
              mean: 0,
              squared_diffs: 0,
            },
          },
          sql_type: "TypeDML",
          last_exec_timestamp: {
            seconds: Long.fromInt(1599670290),
            nanos: 111613000,
          },
          nodes: [new Long(1)],
          kv_node_ids: [2],
          plan_gists: ["AgH6////nxkAAA4AAAAGBg=="],
          failure_count: new Long(2),
        },
      },
      statement_statistics_per_aggregated_ts: [
        {
          stats: {
            count: new Long(1),
            first_attempt_count: new Long(1),
            max_retries: new Long(0),
            legacy_last_err: "",
            legacy_last_err_redacted: "",
            num_rows: {
              mean: 6,
              squared_diffs: 0,
            },
            parse_lat: {
              mean: 0.00004,
              squared_diffs: 0,
            },
            plan_lat: {
              mean: 0.000105,
              squared_diffs: 0,
            },
            run_lat: {
              mean: 0.000285,
              squared_diffs: 0,
            },
            service_lat: {
              mean: 0.000436,
              squared_diffs: 0,
            },
            overhead_lat: {
              mean: 0.000006000000000000037,
              squared_diffs: 0,
            },
            sensitive_info: {
              last_err: "",
              most_recent_plan_description: {
                name: "virtual table",
                attrs: [
                  {
                    key: "Table",
                    value: "node_build_info@primary",
                  },
                ],
                children: [],
              },
              most_recent_plan_timestamp: {
                seconds: new Long(1614851546),
                nanos: 956814000,
              },
            },
            bytes_read: {
              mean: 0,
              squared_diffs: 0,
            },
            rows_read: {
              mean: 0,
              squared_diffs: 0,
            },
            rows_written: {
              mean: 0,
              squared_diffs: 0,
            },
            exec_stats: {
              count: new Long(1),
              network_bytes: {
                mean: 0,
                squared_diffs: 0,
              },
              max_mem_usage: {
                mean: 10240,
                squared_diffs: 0,
              },
              contention_time: {
                mean: 0,
                squared_diffs: 0,
              },
              network_messages: {
                mean: 0,
                squared_diffs: 0,
              },
              max_disk_usage: {
                mean: 0,
                squared_diffs: 0,
              },
            },
            sql_type: "TypeDML",
            last_exec_timestamp: {
              seconds: Long.fromInt(1599670292),
              nanos: 111613000,
            },
            nodes: [new Long(1)],
            kv_node_ids: [2],
            plan_gists: ["AgH6////nxkAAA4AAAAGBg=="],
          },
          aggregated_ts: {
            seconds: Long.fromInt(1599670292),
            nanos: 111613000,
          },
        },
        {
          stats: {
            count: new Long(2),
            first_attempt_count: new Long(2),
            max_retries: new Long(0),
            legacy_last_err: "",
            legacy_last_err_redacted: "",
            num_rows: {
              mean: 6,
              squared_diffs: 0,
            },
            parse_lat: {
              mean: 0.000071,
              squared_diffs: 4.050000000000001e-9,
            },
            plan_lat: {
              mean: 0.0001525,
              squared_diffs: 3.960499999999999e-9,
            },
            run_lat: {
              mean: 0.0002255,
              squared_diffs: 1.08045e-8,
            },
            service_lat: {
              mean: 0.0004555,
              squared_diffs: 1.0224500000000002e-8,
            },
            overhead_lat: {
              mean: 0.000006499999999999995,
              squared_diffs: 4.499999999999893e-12,
            },
            sensitive_info: {
              last_err: "",
              most_recent_plan_description: {
                name: "virtual table",
                attrs: [
                  {
                    key: "Table",
                    value: "node_build_info@primary",
                  },
                ],
                children: [],
              },
              most_recent_plan_timestamp: {
                seconds: new Long(1614851546),
                nanos: 956814000,
              },
            },
            bytes_read: {
              mean: 0,
              squared_diffs: 0,
            },
            rows_read: {
              mean: 0,
              squared_diffs: 0,
            },
            rows_written: {
              mean: 0,
              squared_diffs: 0,
            },
            exec_stats: {
              count: new Long(2),
              network_bytes: {
                mean: 0,
                squared_diffs: 0,
              },
              max_mem_usage: {
                mean: 10240,
                squared_diffs: 0,
              },
              contention_time: {
                mean: 0,
                squared_diffs: 0,
              },
              network_messages: {
                mean: 0,
                squared_diffs: 0,
              },
              max_disk_usage: {
                mean: 0,
                squared_diffs: 0,
              },
            },
            sql_type: "TypeDML",
            last_exec_timestamp: {
              seconds: Long.fromInt(1599670292),
              nanos: 111613000,
            },
            nodes: [new Long(1)],
            kv_node_ids: [2],
            plan_gists: ["AgH6////nxkAAA4AAAAGBg=="],
          },
          aggregated_ts: {
            seconds: Long.fromInt(1599670292),
            nanos: 111613000,
          },
        },
        {
          stats: {
            count: new Long(1),
            first_attempt_count: new Long(1),
            max_retries: new Long(0),
            legacy_last_err: "",
            legacy_last_err_redacted: "",
            num_rows: {
              mean: 6,
              squared_diffs: 0,
            },
            parse_lat: {
              mean: 0.000046,
              squared_diffs: 0,
            },
            plan_lat: {
              mean: 0.000159,
              squared_diffs: 0,
            },
            run_lat: {
              mean: 0.000299,
              squared_diffs: 0,
            },
            service_lat: {
              mean: 0.000514,
              squared_diffs: 0,
            },
            overhead_lat: {
              mean: 0.000010000000000000026,
              squared_diffs: 0,
            },
            sensitive_info: {
              last_err: "",
              most_recent_plan_description: {
                name: "virtual table",
                attrs: [
                  {
                    key: "Table",
                    value: "node_build_info@primary",
                  },
                ],
                children: [],
              },
              most_recent_plan_timestamp: {
                seconds: new Long(1614851546),
                nanos: 956814000,
              },
            },
            bytes_read: {
              mean: 0,
              squared_diffs: 0,
            },
            rows_read: {
              mean: 0,
              squared_diffs: 0,
            },
            rows_written: {
              mean: 0,
              squared_diffs: 0,
            },
            exec_stats: {
              count: new Long(1),
              network_bytes: {
                mean: 0,
                squared_diffs: 0,
              },
              max_mem_usage: {
                mean: 10240,
                squared_diffs: 0,
              },
              contention_time: {
                mean: 0,
                squared_diffs: 0,
              },
              network_messages: {
                mean: 0,
                squared_diffs: 0,
              },
              max_disk_usage: {
                mean: 0,
                squared_diffs: 0,
              },
            },
            sql_type: "TypeDML",
            last_exec_timestamp: {
              seconds: Long.fromInt(1599670292),
              nanos: 111613000,
            },
            nodes: [new Long(1)],
            kv_node_ids: [2],
            plan_gists: ["AgH6////nxkAAA4AAAAGBg=="],
          },
          aggregated_ts: {
            seconds: Long.fromInt(1599671292),
            nanos: 111613000,
          },
        },
        {
          stats: {
            count: new Long(1),
            first_attempt_count: new Long(1),
            max_retries: new Long(0),
            legacy_last_err: "",
            legacy_last_err_redacted: "",
            num_rows: {
              mean: 6,
              squared_diffs: 0,
            },
            parse_lat: {
              mean: 0.00021,
              squared_diffs: 0,
            },
            plan_lat: {
              mean: 0.040086,
              squared_diffs: 0,
            },
            run_lat: {
              mean: 0.000363,
              squared_diffs: 0,
            },
            service_lat: {
              mean: 0.040749,
              squared_diffs: 0,
            },
            overhead_lat: {
              mean: 0.0000899999999999998,
              squared_diffs: 0,
            },
            sensitive_info: {
              last_err: "",
              most_recent_plan_description: {
                name: "virtual table",
                attrs: [
                  {
                    key: "Table",
                    value: "node_build_info@primary",
                  },
                ],
                children: [],
              },
              most_recent_plan_timestamp: {
                seconds: new Long(1614851546),
                nanos: 956814000,
              },
            },
            bytes_read: {
              mean: 0,
              squared_diffs: 0,
            },
            rows_read: {
              mean: 0,
              squared_diffs: 0,
            },
            rows_written: {
              mean: 0,
              squared_diffs: 0,
            },
            exec_stats: {
              count: new Long(1),
              network_bytes: {
                mean: 0,
                squared_diffs: 0,
              },
              max_mem_usage: {
                mean: 10240,
                squared_diffs: 0,
              },
              contention_time: {
                mean: 0,
                squared_diffs: 0,
              },
              network_messages: {
                mean: 0,
                squared_diffs: 0,
              },
              max_disk_usage: {
                mean: 0,
                squared_diffs: 0,
              },
            },
            sql_type: "TypeDML",
            last_exec_timestamp: {
              seconds: Long.fromInt(1599670292),
              nanos: 111613000,
            },
            nodes: [new Long(1)],
            kv_node_ids: [2],
            plan_gists: ["AgH6////nxkAAA4AAAAGBg=="],
          },
          aggregated_ts: {
            seconds: Long.fromInt(1599680292),
            nanos: 111613000,
          },
        },
      ],
      statement_statistics_per_plan_hash: [
        {
          stats: {
            count: new Long(5),
            first_attempt_count: new Long(5),
            max_retries: new Long(0),
            legacy_last_err: "",
            legacy_last_err_redacted: "",
            num_rows: {
              mean: 6,
              squared_diffs: 0,
            },
            parse_lat: {
              mean: 0.0000876,
              squared_diffs: 2.35792e-8,
            },
            plan_lat: {
              mean: 0.008131,
              squared_diffs: 0.00127640837,
            },
            run_lat: {
              mean: 0.0002796,
              squared_diffs: 2.401919999999999e-8,
            },
            service_lat: {
              mean: 0.008522,
              squared_diffs: 0.001298238058,
            },
            overhead_lat: {
              mean: 0.000023799999999999972,
              squared_diffs: 5.492799999999973e-9,
            },
            sensitive_info: {
              last_err: "",
              most_recent_plan_description: {
                name: "virtual table",
                attrs: [
                  {
                    key: "Table",
                    value: "node_build_info@primary",
                  },
                ],
                children: [],
              },
              most_recent_plan_timestamp: {
                seconds: new Long(1614851546),
                nanos: 956814000,
              },
            },
            bytes_read: {
              mean: 0,
              squared_diffs: 0,
            },
            rows_read: {
              mean: 0,
              squared_diffs: 0,
            },
            rows_written: {
              mean: 0,
              squared_diffs: 0,
            },
            exec_stats: {
              count: new Long(5),
              network_bytes: {
                mean: 0,
                squared_diffs: 0,
              },
              max_mem_usage: {
                mean: 10240,
                squared_diffs: 0,
              },
              contention_time: {
                mean: 0,
                squared_diffs: 0,
              },
              network_messages: {
                mean: 0,
                squared_diffs: 0,
              },
              max_disk_usage: {
                mean: 0,
                squared_diffs: 0,
              },
            },
            sql_type: "TypeDML",
            last_exec_timestamp: {
              seconds: Long.fromInt(1599670292),
              nanos: 111613000,
            },
            nodes: [new Long(1)],
            kv_node_ids: [2],
            plan_gists: ["AgH6////nxkAAA4AAAAGBg=="],
          },
          explain_plan: "• virtual table\n  table: @primary",
          plan_hash: Long.fromString("14192395335876201826"),
        },
      ],
      internal_app_name_prefix: "$ internal",
    });

  const stmtDetailsStatsAPIProvider: (EffectProviders | StaticProvider)[] = [
    [matchers.call.fn(getStatementDetails), SQLDetailsStatsResponse],
  ];

  describe("refreshSQLDetailsStatsSaga", () => {
    it("dispatches request SQLDetailsStats action", () => {
      return expectSaga(refreshSQLDetailsStatsSaga)
        .put(actions.request())
        .run();
    });
  });

  describe("requestSQLDetailsStatsSaga", () => {
    it("successfully requests statement details", () => {
      return expectSaga(requestSQLDetailsStatsSaga, action)
        .provide(stmtDetailsStatsAPIProvider)
        .put(
          actions.received({
            stmtResponse: SQLDetailsStatsResponse,
            key,
          }),
        )
        .withReducer(reducer)
        .hasFinalState<SQLDetailsStatsReducerState>({
          cachedData: {
            "SELECT * FROM crdb_internal.node_build_info/$ cockroach sql,newname/0/0":
              {
                data: SQLDetailsStatsResponse,
                lastError: null,
                valid: true,
                inFlight: false,
                lastUpdated: lastUpdated,
              },
          },
        })
        .run();
    });

    it("returns error on failed request", () => {
      const error = new Error("Failed request");
      return expectSaga(requestSQLDetailsStatsSaga, action)
        .provide([[matchers.call.fn(getStatementDetails), throwError(error)]])
        .put(
          actions.failed({
            err: error,
            key,
          }),
        )
        .withReducer(reducer)
        .hasFinalState<SQLDetailsStatsReducerState>({
          cachedData: {
            "SELECT * FROM crdb_internal.node_build_info/$ cockroach sql,newname/0/0":
              {
                data: null,
                lastError: error,
                valid: false,
                inFlight: false,
                lastUpdated: lastUpdated,
              },
          },
        })
        .run();
    });
  });
});
