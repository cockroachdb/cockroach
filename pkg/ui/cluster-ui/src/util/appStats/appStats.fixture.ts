// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import Long from "long";
import { CollectedStatementStatistics } from ".";

export const statementsWithSameIdButDifferentNodeId: CollectedStatementStatistics[] = [
  {
    key: {
      key_data: {
        query:
          "UPDATE system.jobs SET claim_session_id = _ WHERE ((claim_session_id != $1) AND (status IN (_, _, __more3__))) AND (NOT crdb_internal.sql_liveness_is_alive(claim_session_id))",
        app: "$ internal-expire-sessions",
        distSQL: false,
        failed: false,
        opt: true,
        implicit_txn: true,
        vec: false,
        full_scan: false,
      },
      node_id: 4,
    },
    stats: {
      count: new Long(25),
      first_attempt_count: new Long(25),
      max_retries: new Long(0),
      legacy_last_err: "",
      num_rows: { mean: 0, squared_diffs: 0 },
      parse_lat: { mean: 0, squared_diffs: 0 },
      plan_lat: { mean: 0.00059666132, squared_diffs: 7.147805595954399e-7 },
      run_lat: { mean: 0.00514530948, squared_diffs: 0.0046229060751506665 },
      service_lat: {
        mean: 0.012356466080000001,
        squared_diffs: 0.019981287541202375,
      },
      overhead_lat: {
        mean: 0.00661449528,
        squared_diffs: 0.005505204936792645,
      },
      legacy_last_err_redacted: "",
      sensitive_info: {
        last_err: "",
        most_recent_plan_description: {
          name: "update",
          attrs: [
            { key: "table", value: "jobs" },
            { key: "set", value: "claim_session_id" },
            { key: "auto commit", value: "" },
          ],
          children: [
            {
              name: "render",
              children: [
                {
                  name: "filter",
                  attrs: [
                    {
                      key: "filter",
                      value:
                        "(claim_session_id != _) AND (NOT crdb_internal.sql_liveness_is_alive(claim_session_id))",
                    },
                  ],
                  children: [
                    {
                      name: "index join",
                      attrs: [{ key: "table", value: "jobs@primary" }],
                      children: [
                        {
                          name: "scan",
                          attrs: [
                            { key: "missing stats", value: "" },
                            {
                              key: "table",
                              value: "jobs@jobs_status_created_idx",
                            },
                            { key: "spans", value: "5 spans" },
                          ],
                        },
                      ],
                    },
                  ],
                },
              ],
            },
          ],
        },
        most_recent_plan_timestamp: {
          seconds: new Long(1614851546),
          nanos: 211741417,
        },
      },
      bytes_read: { mean: 0, squared_diffs: 0 },
      rows_read: { mean: 0, squared_diffs: 0 },
      last_exec_timestamp: {
        seconds: Long.fromInt(1599670290),
        nanos: 111613000,
      },
      nodes: [Long.fromInt(1), Long.fromInt(2)],
      exec_stats: {
        count: new Long(0),
        network_bytes: { mean: 0, squared_diffs: 0 },
        max_mem_usage: { mean: 0, squared_diffs: 0 },
        contention_time: { mean: 0, squared_diffs: 0 },
        network_messages: { mean: 0, squared_diffs: 0 },
        max_disk_usage: { mean: 0, squared_diffs: 0 },
      },
    },
    id: new Long(8717981371097536892),
  },
  {
    key: {
      key_data: {
        query:
          "UPDATE system.jobs SET claim_session_id = _ WHERE ((claim_session_id != $1) AND (status IN (_, _, __more3__))) AND (NOT crdb_internal.sql_liveness_is_alive(claim_session_id))",
        app: "$ internal-expire-sessions",
        distSQL: false,
        failed: false,
        opt: true,
        implicit_txn: true,
        vec: false,
        full_scan: false,
      },
      node_id: 3,
    },
    stats: {
      count: new Long(25),
      first_attempt_count: new Long(25),
      max_retries: new Long(0),
      legacy_last_err: "",
      num_rows: { mean: 0, squared_diffs: 0 },
      parse_lat: { mean: 0, squared_diffs: 0 },
      plan_lat: { mean: 0.00060528624, squared_diffs: 5.477748852385602e-7 },
      run_lat: { mean: 0.0016260668, squared_diffs: 0.000031665565684372014 },
      service_lat: {
        mean: 0.00436566136,
        squared_diffs: 0.00015617540178032176,
      },
      overhead_lat: {
        mean: 0.00213430832,
        squared_diffs: 0.00009059206052710744,
      },
      legacy_last_err_redacted: "",
      sensitive_info: {
        last_err: "",
        most_recent_plan_description: {
          name: "update",
          attrs: [
            { key: "table", value: "jobs" },
            { key: "set", value: "claim_session_id" },
            { key: "auto commit", value: "" },
          ],
          children: [
            {
              name: "render",
              children: [
                {
                  name: "filter",
                  attrs: [
                    {
                      key: "filter",
                      value:
                        "(claim_session_id != _) AND (NOT crdb_internal.sql_liveness_is_alive(claim_session_id))",
                    },
                  ],
                  children: [
                    {
                      name: "index join",
                      attrs: [{ key: "table", value: "jobs@primary" }],
                      children: [
                        {
                          name: "scan",
                          attrs: [
                            { key: "missing stats", value: "" },
                            {
                              key: "table",
                              value: "jobs@jobs_status_created_idx",
                            },
                            { key: "spans", value: "5 spans" },
                          ],
                        },
                      ],
                    },
                  ],
                },
              ],
            },
          ],
        },
        most_recent_plan_timestamp: {
          seconds: new Long(1614851546),
          nanos: 95596942,
        },
      },
      bytes_read: {
        mean: 47.24000000000001,
        squared_diffs: 1338970.5599999998,
      },
      rows_read: {
        mean: 0.07999999999999999,
        squared_diffs: 3.8399999999999994,
      },
      last_exec_timestamp: {
        seconds: Long.fromInt(1599670272),
        nanos: 111613000,
      },
      nodes: [Long.fromInt(1), Long.fromInt(2)],
      exec_stats: {
        count: new Long(0),
        network_bytes: { mean: 0, squared_diffs: 0 },
        max_mem_usage: { mean: 0, squared_diffs: 0 },
        contention_time: { mean: 0, squared_diffs: 0 },
        network_messages: { mean: 0, squared_diffs: 0 },
        max_disk_usage: { mean: 0, squared_diffs: 0 },
      },
    },
    id: new Long(8717981371097536892),
  },
  {
    key: {
      key_data: {
        query:
          "UPDATE system.jobs SET claim_session_id = _ WHERE ((claim_session_id != $1) AND (status IN (_, _, __more3__))) AND (NOT crdb_internal.sql_liveness_is_alive(claim_session_id))",
        app: "$ internal-expire-sessions",
        distSQL: false,
        failed: false,
        opt: true,
        implicit_txn: true,
        vec: false,
        full_scan: false,
      },
      node_id: 6,
    },
    stats: {
      count: new Long(25),
      first_attempt_count: new Long(25),
      max_retries: new Long(0),
      legacy_last_err: "",
      num_rows: { mean: 0, squared_diffs: 0 },
      parse_lat: { mean: 0, squared_diffs: 0 },
      plan_lat: { mean: 0.00062084732, squared_diffs: 4.894461542734397e-7 },
      run_lat: {
        mean: 0.0033482228399999993,
        squared_diffs: 0.0007254204094330012,
      },
      service_lat: {
        mean: 0.007378451560000001,
        squared_diffs: 0.0025513393104186605,
      },
      overhead_lat: {
        mean: 0.0034093813999999997,
        squared_diffs: 0.000581731831513146,
      },
      legacy_last_err_redacted: "",
      sensitive_info: {
        last_err: "",
        most_recent_plan_description: {
          name: "update",
          attrs: [
            { key: "table", value: "jobs" },
            { key: "set", value: "claim_session_id" },
            { key: "auto commit", value: "" },
          ],
          children: [
            {
              name: "render",
              children: [
                {
                  name: "filter",
                  attrs: [
                    {
                      key: "filter",
                      value:
                        "(claim_session_id != _) AND (NOT crdb_internal.sql_liveness_is_alive(claim_session_id))",
                    },
                  ],
                  children: [
                    {
                      name: "index join",
                      attrs: [{ key: "table", value: "jobs@primary" }],
                      children: [
                        {
                          name: "scan",
                          attrs: [
                            { key: "missing stats", value: "" },
                            {
                              key: "table",
                              value: "jobs@jobs_status_created_idx",
                            },
                            { key: "spans", value: "5 spans" },
                          ],
                        },
                      ],
                    },
                  ],
                },
              ],
            },
          ],
        },
        most_recent_plan_timestamp: {
          seconds: new Long(1614851546),
          nanos: 349254376,
        },
      },
      bytes_read: {
        mean: 59.35999999999999,
        squared_diffs: 2114165.7600000007,
      },
      rows_read: {
        mean: 0.07999999999999999,
        squared_diffs: 3.8399999999999994,
      },
      last_exec_timestamp: {
        seconds: Long.fromInt(1599670192),
        nanos: 111613000,
      },
      nodes: [Long.fromInt(1), Long.fromInt(3)],
      exec_stats: {
        count: new Long(0),
        network_bytes: { mean: 0, squared_diffs: 0 },
        max_mem_usage: { mean: 0, squared_diffs: 0 },
        contention_time: { mean: 0, squared_diffs: 0 },
        network_messages: { mean: 0, squared_diffs: 0 },
        max_disk_usage: { mean: 0, squared_diffs: 0 },
      },
    },
    id: new Long(8717981371097536892),
  },
  {
    key: {
      key_data: {
        query:
          "UPDATE system.jobs SET claim_session_id = _ WHERE ((claim_session_id != $1) AND (status IN (_, _, __more3__))) AND (NOT crdb_internal.sql_liveness_is_alive(claim_session_id))",
        app: "$ internal-expire-sessions",
        distSQL: false,
        failed: false,
        opt: true,
        implicit_txn: true,
        vec: false,
        full_scan: false,
      },
      node_id: 7,
    },
    stats: {
      count: new Long(25),
      first_attempt_count: new Long(25),
      max_retries: new Long(0),
      legacy_last_err: "",
      num_rows: { mean: 0, squared_diffs: 0 },
      parse_lat: { mean: 0, squared_diffs: 0 },
      plan_lat: { mean: 0.0006217386, squared_diffs: 9.382557539239999e-7 },
      run_lat: {
        mean: 0.0023451749200000004,
        squared_diffs: 0.00019083215761566384,
      },
      service_lat: {
        mean: 0.00556639388,
        squared_diffs: 0.0003733923276328406,
      },
      overhead_lat: {
        mean: 0.0025994803599999994,
        squared_diffs: 0.00010200473233901374,
      },
      legacy_last_err_redacted: "",
      sensitive_info: {
        last_err: "",
        most_recent_plan_description: {
          name: "update",
          attrs: [
            { key: "table", value: "jobs" },
            { key: "set", value: "claim_session_id" },
            { key: "auto commit", value: "" },
          ],
          children: [
            {
              name: "render",
              children: [
                {
                  name: "filter",
                  attrs: [
                    {
                      key: "filter",
                      value:
                        "(claim_session_id != _) AND (NOT crdb_internal.sql_liveness_is_alive(claim_session_id))",
                    },
                  ],
                  children: [
                    {
                      name: "index join",
                      attrs: [{ key: "table", value: "jobs@primary" }],
                      children: [
                        {
                          name: "scan",
                          attrs: [
                            { key: "missing stats", value: "" },
                            {
                              key: "table",
                              value: "jobs@jobs_status_created_idx",
                            },
                            { key: "spans", value: "5 spans" },
                          ],
                        },
                      ],
                    },
                  ],
                },
              ],
            },
          ],
        },
        most_recent_plan_timestamp: {
          seconds: new Long(1614851546),
          nanos: 459389689,
        },
      },
      bytes_read: { mean: 0, squared_diffs: 0 },
      rows_read: { mean: 0, squared_diffs: 0 },
      last_exec_timestamp: {
        seconds: Long.fromInt(1599670299),
        nanos: 111613000,
      },
      nodes: [Long.fromInt(1)],
      exec_stats: {
        count: new Long(0),
        network_bytes: { mean: 0, squared_diffs: 0 },
        max_mem_usage: { mean: 0, squared_diffs: 0 },
        contention_time: { mean: 0, squared_diffs: 0 },
        network_messages: { mean: 0, squared_diffs: 0 },
        max_disk_usage: { mean: 0, squared_diffs: 0 },
      },
    },
    id: new Long(8717981371097536892),
  },
  {
    key: {
      key_data: {
        query:
          "UPDATE system.jobs SET claim_session_id = _ WHERE ((claim_session_id != $1) AND (status IN (_, _, __more3__))) AND (NOT crdb_internal.sql_liveness_is_alive(claim_session_id))",
        app: "$ internal-expire-sessions",
        distSQL: false,
        failed: false,
        opt: true,
        implicit_txn: true,
        vec: false,
        full_scan: false,
      },
      node_id: 9,
    },
    stats: {
      count: new Long(25),
      first_attempt_count: new Long(25),
      max_retries: new Long(0),
      legacy_last_err: "",
      num_rows: { mean: 0, squared_diffs: 0 },
      parse_lat: { mean: 0, squared_diffs: 0 },
      plan_lat: { mean: 0.00060384184, squared_diffs: 3.7137755547336e-7 },
      run_lat: {
        mean: 0.0013325950800000001,
        squared_diffs: 0.000014319016889955842,
      },
      service_lat: {
        mean: 0.0037200103599999996,
        squared_diffs: 0.00003218580528155976,
      },
      overhead_lat: {
        mean: 0.0017835734399999999,
        squared_diffs: 0.000003415912313800157,
      },
      legacy_last_err_redacted: "",
      sensitive_info: {
        last_err: "",
        most_recent_plan_description: {
          name: "update",
          attrs: [
            { key: "table", value: "jobs" },
            { key: "set", value: "claim_session_id" },
            { key: "auto commit", value: "" },
          ],
          children: [
            {
              name: "render",
              children: [
                {
                  name: "filter",
                  attrs: [
                    {
                      key: "filter",
                      value:
                        "(claim_session_id != _) AND (NOT crdb_internal.sql_liveness_is_alive(claim_session_id))",
                    },
                  ],
                  children: [
                    {
                      name: "index join",
                      attrs: [{ key: "table", value: "jobs@primary" }],
                      children: [
                        {
                          name: "scan",
                          attrs: [
                            { key: "missing stats", value: "" },
                            {
                              key: "table",
                              value: "jobs@jobs_status_created_idx",
                            },
                            { key: "spans", value: "5 spans" },
                          ],
                        },
                      ],
                    },
                  ],
                },
              ],
            },
          ],
        },
        most_recent_plan_timestamp: {
          seconds: new Long(1614851546),
          nanos: 651098561,
        },
      },
      bytes_read: { mean: 0, squared_diffs: 0 },
      rows_read: { mean: 0, squared_diffs: 0 },
      last_exec_timestamp: {
        seconds: Long.fromInt(1599670242),
        nanos: 111613000,
      },
      nodes: [Long.fromInt(1), Long.fromInt(2), Long.fromInt(4)],
      exec_stats: {
        count: new Long(0),
        network_bytes: { mean: 0, squared_diffs: 0 },
        max_mem_usage: { mean: 0, squared_diffs: 0 },
        contention_time: { mean: 0, squared_diffs: 0 },
        network_messages: { mean: 0, squared_diffs: 0 },
        max_disk_usage: { mean: 0, squared_diffs: 0 },
      },
    },
    id: new Long(8717981371097536892),
  },
  {
    key: {
      key_data: {
        query:
          "UPDATE system.jobs SET claim_session_id = _ WHERE ((claim_session_id != $1) AND (status IN (_, _, __more3__))) AND (NOT crdb_internal.sql_liveness_is_alive(claim_session_id))",
        app: "$ internal-expire-sessions",
        distSQL: false,
        failed: false,
        opt: true,
        implicit_txn: true,
        vec: false,
        full_scan: false,
      },
      node_id: 8,
    },
    stats: {
      count: new Long(25),
      first_attempt_count: new Long(25),
      max_retries: new Long(0),
      legacy_last_err: "",
      num_rows: { mean: 0, squared_diffs: 0 },
      parse_lat: { mean: 0, squared_diffs: 0 },
      plan_lat: { mean: 0.00060733856, squared_diffs: 9.9016117651016e-7 },
      run_lat: {
        mean: 0.0016457390799999995,
        squared_diffs: 0.00004348354674075585,
      },
      service_lat: {
        mean: 0.00508726124,
        squared_diffs: 0.0006775265878511066,
      },
      overhead_lat: {
        mean: 0.0028341836000000003,
        squared_diffs: 0.0004969353409856141,
      },
      legacy_last_err_redacted: "",
      sensitive_info: {
        last_err: "",
        most_recent_plan_description: {
          name: "update",
          attrs: [
            { key: "table", value: "jobs" },
            { key: "set", value: "claim_session_id" },
            { key: "auto commit", value: "" },
          ],
          children: [
            {
              name: "render",
              children: [
                {
                  name: "filter",
                  attrs: [
                    {
                      key: "filter",
                      value:
                        "(claim_session_id != _) AND (NOT crdb_internal.sql_liveness_is_alive(claim_session_id))",
                    },
                  ],
                  children: [
                    {
                      name: "index join",
                      attrs: [{ key: "table", value: "jobs@primary" }],
                      children: [
                        {
                          name: "scan",
                          attrs: [
                            { key: "missing stats", value: "" },
                            {
                              key: "table",
                              value: "jobs@jobs_status_created_idx",
                            },
                            { key: "spans", value: "5 spans" },
                          ],
                        },
                      ],
                    },
                  ],
                },
              ],
            },
          ],
        },
        most_recent_plan_timestamp: {
          seconds: new Long(1614851546),
          nanos: 541858988,
        },
      },
      bytes_read: {
        mean: 75.36000000000003,
        squared_diffs: 3407477.7600000002,
      },
      rows_read: {
        mean: 0.07999999999999999,
        squared_diffs: 3.8399999999999994,
      },
      last_exec_timestamp: {
        seconds: Long.fromInt(1599650292),
        nanos: 111613000,
      },
      nodes: [Long.fromInt(1), Long.fromInt(2)],
      exec_stats: {
        count: new Long(0),
        network_bytes: { mean: 0, squared_diffs: 0 },
        max_mem_usage: { mean: 0, squared_diffs: 0 },
        contention_time: { mean: 0, squared_diffs: 0 },
        network_messages: { mean: 0, squared_diffs: 0 },
        max_disk_usage: { mean: 0, squared_diffs: 0 },
      },
    },
    id: new Long(8717981371097536892),
  },
  {
    key: {
      key_data: {
        query:
          "UPDATE system.jobs SET claim_session_id = _ WHERE ((claim_session_id != $1) AND (status IN (_, _, __more3__))) AND (NOT crdb_internal.sql_liveness_is_alive(claim_session_id))",
        app: "$ internal-expire-sessions",
        distSQL: false,
        failed: false,
        opt: true,
        implicit_txn: true,
        vec: false,
        full_scan: false,
      },
      node_id: 1,
    },
    stats: {
      count: new Long(25),
      first_attempt_count: new Long(25),
      max_retries: new Long(0),
      legacy_last_err: "",
      num_rows: { mean: 0, squared_diffs: 0 },
      parse_lat: { mean: 0, squared_diffs: 0 },
      plan_lat: {
        mean: 0.0005529296400000001,
        squared_diffs: 1.2621076480776003e-7,
      },
      run_lat: {
        mean: 0.0010534599600000001,
        squared_diffs: 0.00000299611852526496,
      },
      service_lat: {
        mean: 0.0033479916,
        squared_diffs: 0.000013804212527590004,
      },
      overhead_lat: {
        mean: 0.001741602,
        squared_diffs: 0.000009894811044980005,
      },
      legacy_last_err_redacted: "",
      sensitive_info: {
        last_err: "",
        most_recent_plan_description: {
          name: "update",
          attrs: [
            { key: "table", value: "jobs" },
            { key: "set", value: "claim_session_id" },
            { key: "auto commit", value: "" },
          ],
          children: [
            {
              name: "render",
              children: [
                {
                  name: "filter",
                  attrs: [
                    {
                      key: "filter",
                      value:
                        "(claim_session_id != _) AND (NOT crdb_internal.sql_liveness_is_alive(claim_session_id))",
                    },
                  ],
                  children: [
                    {
                      name: "index join",
                      attrs: [{ key: "table", value: "jobs@primary" }],
                      children: [
                        {
                          name: "scan",
                          attrs: [
                            { key: "missing stats", value: "" },
                            {
                              key: "table",
                              value: "jobs@jobs_status_created_idx",
                            },
                            { key: "spans", value: "5 spans" },
                          ],
                        },
                      ],
                    },
                  ],
                },
              ],
            },
          ],
        },
        most_recent_plan_timestamp: {
          seconds: new Long(1614851545),
          nanos: 886049756,
        },
      },
      bytes_read: { mean: 46.48, squared_diffs: 1296234.24 },
      rows_read: {
        mean: 0.07999999999999999,
        squared_diffs: 3.8399999999999994,
      },
      last_exec_timestamp: {
        seconds: Long.fromInt(1599670282),
        nanos: 111613000,
      },
      nodes: [Long.fromInt(1), Long.fromInt(2)],
      exec_stats: {
        count: new Long(0),
        network_bytes: { mean: 0, squared_diffs: 0 },
        max_mem_usage: { mean: 0, squared_diffs: 0 },
        contention_time: { mean: 0, squared_diffs: 0 },
        network_messages: { mean: 0, squared_diffs: 0 },
        max_disk_usage: { mean: 0, squared_diffs: 0 },
      },
    },
    id: new Long(8717981371097536892),
  },
  {
    key: {
      key_data: {
        query:
          "UPDATE system.jobs SET claim_session_id = _ WHERE ((claim_session_id != $1) AND (status IN (_, _, __more3__))) AND (NOT crdb_internal.sql_liveness_is_alive(claim_session_id))",
        app: "$ internal-expire-sessions",
        distSQL: false,
        failed: false,
        opt: true,
        implicit_txn: true,
        vec: false,
        full_scan: false,
      },
      node_id: 5,
    },
    stats: {
      count: new Long(25),
      first_attempt_count: new Long(25),
      max_retries: new Long(0),
      legacy_last_err: "",
      num_rows: { mean: 0, squared_diffs: 0 },
      parse_lat: { mean: 0, squared_diffs: 0 },
      plan_lat: {
        mean: 0.0006839353200000001,
        squared_diffs: 0.0000027050684666694405,
      },
      run_lat: {
        mean: 0.004587737999999999,
        squared_diffs: 0.002054554101549576,
      },
      service_lat: {
        mean: 0.006800420800000001,
        squared_diffs: 0.0022942135874811503,
      },
      overhead_lat: {
        mean: 0.00152874748,
        squared_diffs: 0.000020610108769158232,
      },
      legacy_last_err_redacted: "",
      sensitive_info: {
        last_err: "",
        most_recent_plan_description: {
          name: "update",
          attrs: [
            { key: "table", value: "jobs" },
            { key: "set", value: "claim_session_id" },
            { key: "auto commit", value: "" },
          ],
          children: [
            {
              name: "render",
              children: [
                {
                  name: "filter",
                  attrs: [
                    {
                      key: "filter",
                      value:
                        "(claim_session_id != _) AND (NOT crdb_internal.sql_liveness_is_alive(claim_session_id))",
                    },
                  ],
                  children: [
                    {
                      name: "index join",
                      attrs: [{ key: "table", value: "jobs@primary" }],
                      children: [
                        {
                          name: "scan",
                          attrs: [
                            { key: "missing stats", value: "" },
                            {
                              key: "table",
                              value: "jobs@jobs_status_created_idx",
                            },
                            { key: "spans", value: "5 spans" },
                          ],
                        },
                      ],
                    },
                  ],
                },
              ],
            },
          ],
        },
        most_recent_plan_timestamp: {
          seconds: new Long(1614851546),
          nanos: 307939903,
        },
      },
      bytes_read: {
        mean: 59.35999999999999,
        squared_diffs: 2114165.7600000007,
      },
      rows_read: {
        mean: 0.07999999999999999,
        squared_diffs: 3.8399999999999994,
      },
      last_exec_timestamp: {
        seconds: Long.fromInt(1599670257),
        nanos: 111613000,
      },
      nodes: [Long.fromInt(1), Long.fromInt(2)],
      exec_stats: {
        count: new Long(0),
        network_bytes: { mean: 0, squared_diffs: 0 },
        max_mem_usage: { mean: 0, squared_diffs: 0 },
        contention_time: { mean: 0, squared_diffs: 0 },
        network_messages: { mean: 0, squared_diffs: 0 },
        max_disk_usage: { mean: 0, squared_diffs: 0 },
      },
    },
    id: new Long(8717981371097536892),
  },
  {
    key: {
      key_data: {
        query:
          "UPDATE system.jobs SET claim_session_id = _ WHERE ((claim_session_id != $1) AND (status IN (_, _, __more3__))) AND (NOT crdb_internal.sql_liveness_is_alive(claim_session_id))",
        app: "$ internal-expire-sessions",
        distSQL: false,
        failed: false,
        opt: true,
        implicit_txn: true,
        vec: false,
        full_scan: false,
      },
      node_id: 2,
    },
    stats: {
      count: new Long(25),
      first_attempt_count: new Long(25),
      max_retries: new Long(0),
      legacy_last_err: "",
      num_rows: { mean: 0, squared_diffs: 0 },
      parse_lat: { mean: 0, squared_diffs: 0 },
      plan_lat: {
        mean: 0.0013118371600000002,
        squared_diffs: 0.0003047812983599774,
      },
      run_lat: { mean: 0.00097797752, squared_diffs: 0.000015702406008938238 },
      service_lat: {
        mean: 0.004671932679999999,
        squared_diffs: 0.0013375429385049276,
      },
      overhead_lat: {
        mean: 0.0023821180000000008,
        squared_diffs: 0.0003571512515438199,
      },
      legacy_last_err_redacted: "",
      sensitive_info: {
        last_err: "",
        most_recent_plan_description: {
          name: "update",
          attrs: [
            { key: "table", value: "jobs" },
            { key: "set", value: "claim_session_id" },
            { key: "auto commit", value: "" },
          ],
          children: [
            {
              name: "render",
              children: [
                {
                  name: "filter",
                  attrs: [
                    {
                      key: "filter",
                      value:
                        "(claim_session_id != _) AND (NOT crdb_internal.sql_liveness_is_alive(claim_session_id))",
                    },
                  ],
                  children: [
                    {
                      name: "index join",
                      attrs: [{ key: "table", value: "jobs@primary" }],
                      children: [
                        {
                          name: "scan",
                          attrs: [
                            { key: "missing stats", value: "" },
                            {
                              key: "table",
                              value: "jobs@jobs_status_created_idx",
                            },
                            { key: "spans", value: "5 spans" },
                          ],
                        },
                      ],
                    },
                  ],
                },
              ],
            },
          ],
        },
        most_recent_plan_timestamp: {
          seconds: new Long(1614851546),
          nanos: 889864,
        },
      },
      bytes_read: { mean: 47.07999999999999, squared_diffs: 1329915.84 },
      rows_read: {
        mean: 0.07999999999999999,
        squared_diffs: 3.8399999999999994,
      },
      last_exec_timestamp: {
        seconds: Long.fromInt(1599670279),
        nanos: 111613000,
      },
      nodes: [Long.fromInt(1), Long.fromInt(2)],
      exec_stats: {
        count: new Long(0),
        network_bytes: { mean: 0, squared_diffs: 0 },
        max_mem_usage: { mean: 0, squared_diffs: 0 },
        contention_time: { mean: 0, squared_diffs: 0 },
        network_messages: { mean: 0, squared_diffs: 0 },
        max_disk_usage: { mean: 0, squared_diffs: 0 },
      },
    },
    id: new Long(8717981371097536892),
  },
];
