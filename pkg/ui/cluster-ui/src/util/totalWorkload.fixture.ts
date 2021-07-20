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
import { StatementStatistics } from "./appStats";
import { ExecStats } from ".";

interface AggregateStatistics {
  label: string;
  implicitTxn: boolean;
  fullScan: boolean;
  stats: StatementStatistics;
}

const execStats: Required<ExecStats> = {
  count: Long.fromNumber(1),
  network_bytes: {
    mean: 4160407,
    squared_diffs: 47880000000000,
  },
  max_mem_usage: {
    mean: 4160407,
    squared_diffs: 47880000000000,
  },
  contention_time: {
    mean: 4160407,
    squared_diffs: 47880000000000,
  },
  network_messages: {
    mean: 4160407,
    squared_diffs: 47880000000000,
  },
  max_disk_usage: {
    mean: 4160407,
    squared_diffs: 47880000000000,
  },
};

const statementStats: any = {
  count: 36958,
  first_attempt_count: Long.fromNumber(36958),
  max_retries: Long.fromNumber(0),
  num_rows: {
    mean: 11.651577466313078,
    squared_diffs: 1493154.3630337175,
  },
  parse_lat: {
    mean: 0,
    squared_diffs: 0,
  },
  plan_lat: {
    mean: 0.00022804377942529385,
    squared_diffs: 0.0030062544511648935,
  },
  run_lat: {
    mean: 0.00098355830943233,
    squared_diffs: 0.04090499253784317,
  },
  service_lat: {
    mean: 0.0013101634016992284,
    squared_diffs: 0.055668241814216965,
  },
  overhead_lat: {
    mean: 0.00009856131284160407,
    squared_diffs: 0.0017520019405651047,
  },
  bytes_read: {
    mean: 4160407,
    squared_diffs: 47880000000000,
  },
  rows_read: {
    mean: 7,
    squared_diffs: 1000000,
  },
  sensitive_info: {
    last_err: "",
    most_recent_plan_description: {
      name: "render",
      attrs: [
        {
          key: "render",
          value: "city",
        },
        {
          key: "render",
          value: "id",
        },
      ],
      children: [
        {
          name: "scan",
          attrs: [
            {
              key: "table",
              value: "vehicles@vehicles_auto_index_fk_city_ref_users",
            },
            {
              key: "spans",
              value: "1 span",
            },
          ],
          children: [],
        },
      ],
    },
  },
  exec_stats: execStats,
};

export const aggStatFix: AggregateStatistics = {
  label: "foo",
  implicitTxn: false,
  fullScan: false,
  stats: statementStats,
};
