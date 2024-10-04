// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { fetchData } from "src/api";
import { ADMIN_API_PREFIX } from "./util";

export type SettingsRequestMessage = cockroach.server.serverpb.SettingsRequest;
export type SettingsResponseMessage =
  cockroach.server.serverpb.SettingsResponse;

// getClusterSettings gets all cluster settings. We request unredacted_values, which will attempt
// to obtain all values from the server. The server will only accept to do so if
// the user also happens to have admin privilege.
export function getClusterSettings(
  req: SettingsRequestMessage,
  timeout: string,
): Promise<SettingsResponseMessage> {
  return fetchData(
    cockroach.server.serverpb.SettingsResponse,
    `${ADMIN_API_PREFIX}/settings?unredacted_values=true`,
    cockroach.server.serverpb.SettingsRequest,
    req,
    timeout,
  );
}
