// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import Long from "long";
import {createMemoryHistory} from "history";
import {StatementDetailsProps} from "src/views/statements/statementDetails";
import {refreshStatementDiagnosticsRequests, refreshStatements} from "oss/src/redux/apiReducers";

const history = createMemoryHistory({ initialEntries: ["/statements"]});

const statementStats: any = {
  "count": Long.fromNumber(36958),
  "first_attempt_count": Long.fromNumber(36958),
  "max_retries": Long.fromNumber(0),
  "num_rows": {
    "mean": 11.651577466313078,
    "squared_diffs": 1493154.3630337175,
  },
  "parse_lat": {
    "mean": 0,
    "squared_diffs": 0,
  },
  "plan_lat": {
    "mean": 0.00022804377942529385,
    "squared_diffs": 0.0030062544511648935,
  },
  "run_lat": {
    "mean": 0.00098355830943233,
    "squared_diffs": 0.04090499253784317,
  },
  "service_lat": {
    "mean": 0.0013101634016992284,
    "squared_diffs": 0.055668241814216965,
  },
  "overhead_lat": {
    "mean": 0.00009856131284160407,
    "squared_diffs": 0.0017520019405651047,
  },
  "bytes_read": {
    "mean": 4160407,
    "squared_diffs": 47880000000000,
  },
  "rows_read": {
    "mean": 7,
    "squared_diffs": 1000000,
  },
  "sensitive_info": {
    "last_err": "",
    "most_recent_plan_description": {
      "name": "render",
      "attrs": [
        {
          "key": "render",
          "value": "city",
        },
        {
          "key": "render",
          "value": "id",
        },
      ],
      "children": [
        {
          "name": "scan",
          "attrs": [
            {
              "key": "table",
              "value": "vehicles@vehicles_auto_index_fk_city_ref_users",
            },
            {
              "key": "spans",
              "value": "1 span",
            },
          ],
          "children": [],
        },
      ],
    },
  },
};

export const statementDetailsPropsFixture: StatementDetailsProps = {
  history,
  "location": {
    "pathname": "/statement/true/SELECT city%2C id FROM vehicles WHERE city %3D %241",
    "search": "",
    "hash": "",
    "state": null,
  },
  "match": {
    "path": "/statement/:implicitTxn/:statement",
    "url": "/statement/true/SELECT city%2C id FROM vehicles WHERE city %3D %241",
    "isExact": true,
    "params": {
      "implicitTxn": "true",
      "statement": "SELECT city%2C id FROM vehicles WHERE city %3D %241",
    },
  },
  "statement": {
    "statement": "SELECT city, id FROM vehicles WHERE city = $1",
    "stats": statementStats,
    "byNode": [
      {
        "label": "4",
        "implicitTxn": true,
        "stats": statementStats,
      },
      {
        "label": "3",
        "implicitTxn": true,
        "stats": statementStats,
      },
      {
        "label": "2",
        "implicitTxn": true,
        "stats": statementStats,
      },
      {
        "label": "1",
        "implicitTxn": true,
        "stats": statementStats,
      },
    ],
    "app": [
      "movr",
    ],
    "distSQL": {
      "numerator": 0,
      "denominator": 36958,
    },
    "vec": {
      "numerator": 36958,
      "denominator": 36958,
    },
    "opt": {
      "numerator": 36958,
      "denominator": 36958,
    },
    "implicit_txn": {
      "numerator": 36958,
      "denominator": 36958,
    },
    "failed": {
      "numerator": 0,
      "denominator": 36958,
    },
    "node_id": [
      4,
      3,
      2,
      1,
    ],
  },
  "statementsError": null,
  "nodeNames": {
    "1": "127.0.0.1:55529 (n1)",
    "2": "127.0.0.1:55532 (n2)",
    "3": "127.0.0.1:55538 (n3)",
    "4": "127.0.0.1:55546 (n4)",
  },
  "diagnosticsCount": 1,
  nodesSummary: undefined,
  refreshStatements: (() => {}) as (typeof refreshStatements),
  refreshStatementDiagnosticsRequests: (() => {}) as (typeof refreshStatementDiagnosticsRequests),
};
