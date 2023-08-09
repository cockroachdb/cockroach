// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
