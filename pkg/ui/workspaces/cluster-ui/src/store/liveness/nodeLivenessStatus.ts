// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

export const NodeLivenessStatus =
  cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus;
