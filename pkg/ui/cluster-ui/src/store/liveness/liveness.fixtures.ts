// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import Long from "long";

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { NodeLivenessStatus } from "./nodeLivenessStatus";

export const getLivenessResponse = (): cockroach.server.serverpb.ILivenessResponse => ({
  livenesses: [
    {
      node_id: 1,
      epoch: Long.fromString("5"),
      expiration: {
        wall_time: Long.fromString("1611238408445291000"),
        logical: 0,
      },
    },
  ],
  statuses: {
    "1": NodeLivenessStatus.NODE_STATUS_LIVE,
  },
});
