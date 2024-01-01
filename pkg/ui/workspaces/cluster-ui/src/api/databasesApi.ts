// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { fetchData } from "./fetchData";

const DATABASES_PATH = "_admin/v1/databases";

// getDatabasesList fetches databases names from the database.
export async function getDatabasesList(): Promise<cockroach.server.serverpb.DatabasesResponse> {
  return fetchData(
    cockroach.server.serverpb.DatabasesResponse,
    DATABASES_PATH,
    cockroach.server.serverpb.DatabasesRequest,
    null,
    "1M",
  );
}
