// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import { fetchData } from "src/api";

const LIVENESS_PATH = "_admin/v1/liveness";

export const getLiveness =
  (): Promise<cockroach.server.serverpb.LivenessResponse> => {
    return fetchData(cockroach.server.serverpb.LivenessResponse, LIVENESS_PATH);
  };
