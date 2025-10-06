// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import { fetchData } from "src/api";

export type UserSQLRolesRequestMessage =
  cockroach.server.serverpb.UserSQLRolesRequest;
export type UserSQLRolesResponseMessage =
  cockroach.server.serverpb.UserSQLRolesResponse;

export function getUserSQLRoles(): Promise<UserSQLRolesResponseMessage> {
  return fetchData(
    cockroach.server.serverpb.UserSQLRolesResponse,
    `_status/sqlroles`,
    null,
    null,
    "30M",
  );
}
