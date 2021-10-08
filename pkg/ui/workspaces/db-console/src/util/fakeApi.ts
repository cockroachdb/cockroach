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

import { cockroach } from "src/js/protos";
import { API_PREFIX, toArrayBuffer } from "src/util/api";
import fetchMock from "src/util/fetch-mock";

const {
  DatabasesResponse,
  DatabaseDetailsResponse,
  TableDetailsResponse,
  TableStatsResponse,
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

export function stubDatabases(
  response: cockroach.server.serverpb.IDatabasesResponse,
) {
  stubGet("/databases", DatabasesResponse.encode(response));
}

export function stubDatabaseDetails(
  database: string,
  response: cockroach.server.serverpb.IDatabaseDetailsResponse,
) {
  stubGet(
    `/databases/${database}?include_stats=true`,
    DatabaseDetailsResponse.encode(response),
  );
}

export function stubTableDetails(
  database: string,
  table: string,
  response: cockroach.server.serverpb.ITableDetailsResponse,
) {
  stubGet(
    `/databases/${database}/tables/${table}`,
    TableDetailsResponse.encode(response),
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
  );
}

function stubGet(path: string, writer: $protobuf.Writer) {
  fetchMock.get(`${API_PREFIX}${path}`, toArrayBuffer(writer.finish()));
}
