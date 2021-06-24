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

function stubGet(path: string, writer: $protobuf.Writer) {
  fetchMock.get(`${API_PREFIX}${path}`, toArrayBuffer(writer.finish()));
}

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
