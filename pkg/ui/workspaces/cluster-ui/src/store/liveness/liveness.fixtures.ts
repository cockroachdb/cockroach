// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import Long from "long";

import { NodeLivenessStatus } from "./nodeLivenessStatus";

export const getLivenessResponse =
  (): cockroach.server.serverpb.ILivenessResponse => ({
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
