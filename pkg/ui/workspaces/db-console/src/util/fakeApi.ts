// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as $protobuf from "protobufjs";

import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";
import { cockroach } from "src/js/protos";
import { API_PREFIX, STATUS_PREFIX } from "src/util/api";
import fetchMock from "src/util/fetch-mock";

const {
  DatabaseDetailsResponse,
  SettingsResponse,
  TableDetailsResponse,
  TableStatsResponse,
  TableIndexStatsResponse,
  NodesResponse,
} = cockroach.server.serverpb;

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
//       fakeApi.stubDatabases({
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

export function buildSQLApiDatabasesResponse(databases: string[]) {
  const rows: clusterUiApi.DatabasesColumns[] = databases.map(database => {
    return {
      database_name: database,
      owner: "root",
      primary_region: null,
      secondary_region: null,
      regions: ["gcp-europe-west1", "gcp-europe-west2"],
      survival_goal: null,
    };
  });
  return {
    num_statements: 1,
    execution: {
      txn_results: [
        {
          statement: 1,
          tag: "SHOW DATABASES",
          start: "2022-10-27T17:42:05.582744Z",
          end: "2022-10-27T17:42:05.588454Z",
          rows_affected: 0,
          columns: [
            {
              name: "database_name",
              type: "STRING",
              oid: 25,
            },
            {
              name: "owner",
              type: "NAME",
              oid: 19,
            },
            {
              name: "primary_region",
              type: "STRING",
              oid: 25,
            },
            {
              name: "secondary_region",
              type: "STRING",
              oid: 25,
            },
            {
              name: "regions",
              type: "STRING[]",
              oid: 1009,
            },
            {
              name: "survival_goal",
              type: "STRING",
              oid: 25,
            },
          ],
          rows: rows,
        },
      ],
    },
  };
}

export function buildSQLApiEventsResponse(events: clusterUiApi.EventsResponse) {
  return {
    num_statements: 1,
    execution: {
      txn_results: [
        {
          statement: 1,
          tag: "SELECT",
          start: "2022-11-14T16:26:45.06819Z",
          end: "2022-11-14T16:26:45.073657Z",
          rows_affected: 0,
          columns: [
            {
              name: "timestamp",
              type: "TIMESTAMP",
              oid: 1114,
            },
            {
              name: "eventType",
              type: "STRING",
              oid: 25,
            },
            {
              name: "reportingID",
              type: "INT8",
              oid: 20,
            },
            {
              name: "info",
              type: "STRING",
              oid: 25,
            },
            {
              name: "uniqueID",
              type: "BYTES",
              oid: 17,
            },
          ],
          rows: events,
        },
      ],
    },
  };
}

export function stubDatabases(databases: string[]) {
  const response = buildSQLApiDatabasesResponse(databases);
  fetchMock.mock({
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-Cockroach-API-Session": "cookie",
    },
    matcher: clusterUiApi.SQL_API_PATH,
    method: "POST",
    response: (_url: string, requestObj: RequestInit) => {
      expect(JSON.parse(requestObj.body.toString())).toEqual(
        clusterUiApi.databasesRequest,
      );
      return {
        body: JSON.stringify(response),
      };
    },
  });
}

export function stubDatabaseDetails(
  database: string,
  response: cockroach.server.serverpb.IDatabaseDetailsResponse,
) {
  stubGet(
    `/databases/${database}?include_stats=true`,
    DatabaseDetailsResponse.encode(response),
    API_PREFIX,
  );
}

export function stubNodesUI(
  response: cockroach.server.serverpb.INodesResponseExternal,
) {
  stubGet(`/nodes_ui`, NodesResponse.encode(response), STATUS_PREFIX);
}

export function stubTableDetails(
  database: string,
  table: string,
  response: cockroach.server.serverpb.ITableDetailsResponse,
) {
  stubGet(
    `/databases/${database}/tables/${table}`,
    TableDetailsResponse.encode(response),
    API_PREFIX,
  );
}

export function stubTableStats(
  database: string,
  table: string,
  response: cockroach.server.serverpb.ITableStatsResponse,
) {
  stubGet(
    `/databases/${database}/tables/${table}/stats`,
    TableStatsResponse.encode(response),
    API_PREFIX,
  );
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
