// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";
import { SqlTxnResult } from "@cockroachlabs/cluster-ui/dist/types/api";
import moment from "moment-timezone";
import * as $protobuf from "protobufjs";

import { cockroach } from "src/js/protos";
import { API_PREFIX, STATUS_PREFIX } from "src/util/api";
import fetchMock from "src/util/fetch-mock";

const { SettingsResponse, TableIndexStatsResponse, NodesResponse } =
  cockroach.server.serverpb;

// These test-time functions provide typesafe wrappers around fetchMock,
// stubbing HTTP responses from the admin API.
//
// Be sure to call `restore()` after each test that uses `fakeApi`,
// so that your stubbed responses won't leak out to other tests.
//
// Example usage:
//
//   describe("The thing I'm testing", function() {
//     it("works like this", function() {
//       // 1. Set up a fake response from the /databases endpoint.
//       fakeApi.stuatabases({
//         databases: ["one", "two", "three"],
//       });
//
//       // 2. Run your code that hits the /databases endpoint.
//       // ...
//
//       // 3. Assert on its data / behavior.
//       assert.deepEqual(myThing.databaseNames(), ["one", "two", "three"]);
//     });
//
//     // 4. Tear down any fake responses we set up.
//     afterEach(function() {
//       fakeApi.restore();
//     });
//   });

export function restore() {
  fetchMock.restore();
}

export function stubClusterSettings(
  response: cockroach.server.serverpb.ISettingsResponse,
) {
  stubGet(
    "/settings?unredacted_values=true",
    SettingsResponse.encode(response),
    API_PREFIX,
  );
}

export function stubNodesUI(
  response: cockroach.server.serverpb.INodesResponseExternal,
) {
  stubGet(`/nodes_ui`, NodesResponse.encode(response), STATUS_PREFIX);
}

export function stubIndexStats(
  database: string,
  table: string,
  response: cockroach.server.serverpb.ITableIndexStatsResponse,
) {
  stubGet(
    `/databases/${database}/tables/${table}/indexstats`,
    TableIndexStatsResponse.encode(response),
    STATUS_PREFIX,
  );
}

function stubGet(path: string, writer: $protobuf.Writer, prefix: string) {
  fetchMock.get(`${prefix}${path}`, writer.finish());
}

export function stubSqlApiCall<T>(
  req: clusterUiApi.SqlExecutionRequest,
  mockTxnResults: mockSqlTxnResult<T>[],
  times = 1,
) {
  const firstError = mockTxnResults.find(mock => mock.error != null)?.error;
  let err: clusterUiApi.SqlExecutionErrorMessage;
  if (firstError != null) {
    err = {
      message: firstError.message,
      code: "123",
      severity: "ERROR",
      source: { file: "myfile.go", line: 111, function: "myFn" },
    };
  }
  const response = buildSqlExecutionResponse(mockTxnResults, err);
  fetchMock.mock({
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-Cockroach-API-Session": "cookie",
    },
    matcher: clusterUiApi.SQL_API_PATH,
    method: "POST",
    response: (_url: string, requestObj: RequestInit) => {
      expect(JSON.parse(requestObj.body.toString())).toEqual({
        ...req,
        application_name:
          req.application_name || clusterUiApi.INTERNAL_SQL_API_APP,
        database: req.database || clusterUiApi.FALLBACK_DB,
      });
      return {
        body: JSON.stringify(response),
      };
    },
    times: times,
  });
}

export function buildSqlExecutionResponse<T>(
  mockTxnResults: mockSqlTxnResult<T>[],
  error?: clusterUiApi.SqlExecutionErrorMessage,
): clusterUiApi.SqlExecutionResponse<T> {
  const sqlTxnResults: clusterUiApi.SqlTxnResult<T>[] = mockTxnResults.map(
    (mock, idx) => {
      mock.statement = idx + 1;
      return buildSqlTxnResult(mock);
    },
  );
  const resp: clusterUiApi.SqlExecutionResponse<T> = {
    execution: {
      retries: 0,
      txn_results: sqlTxnResults,
    },
    error: error,
  };
  return resp;
}

// Same as SqlTxnResult, but all fields are optional.
export type mockSqlTxnResult<RowType> = {
  statement?: number; // Statement index from input array
  tag?: string; // Short stmt tag
  start?: string; // Start timestamp, encoded as RFC3339
  end?: string; // End timestamp, encoded as RFC3339
  rows_affected?: number;
  columns?: clusterUiApi.SqlResultColumn[];
  rows?: RowType[];
  error?: Error;
};

// buildSqlTxnResult provides default values for mandatory fields
// of a SqlTxnResult.
function buildSqlTxnResult<RowType>(
  mock: mockSqlTxnResult<RowType>,
): clusterUiApi.SqlTxnResult<RowType> {
  const statement = mock.statement ? mock.statement : 1;
  const rowsAffected = mock.rows_affected ? mock.rows_affected : 0;
  const startTimestamp = mock.start ? mock.start : new Date().toISOString();
  const endTimestamp = mock.end
    ? mock.end
    : moment(startTimestamp).add(1, "s").toISOString();
  const stmtTag = mock.tag ? mock.tag : "SELECT";
  return {
    statement: statement,
    tag: stmtTag,
    start: startTimestamp,
    end: endTimestamp,
    rows_affected: rowsAffected,
    columns: mock.columns,
    rows: mock.rows,
    error: mock.error,
  };
}

const mockStmtExecErrorResponse = <T>(
  res: Partial<SqlTxnResult<T>>,
): SqlTxnResult<T> => ({
  statement: res?.statement ?? 1,
  tag: "SELECT",
  start: "2022-01-01T00:00:00Z",
  end: "2022-01-01T00:00:01Z",
  error: new Error("error"),
  rows_affected: 0,
  ...res,
});

export const mockExecSQLErrors = <T>(
  statements: number,
): mockSqlTxnResult<T>[] => {
  return Array.from({ length: statements }, (_, i) =>
    mockStmtExecErrorResponse<T>({ statement: i + 1 }),
  );
};
