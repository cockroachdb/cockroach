// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import Long from "long";
import * as protos from "@cockroachlabs/crdb-protobuf-client";

export const defaultJobProperties = {
  username: "root",
  descriptor_ids: [] as number[],
  created: new protos.google.protobuf.Timestamp({
    seconds: new Long(1634648118),
    nanos: 200459000,
  }),
  started: new protos.google.protobuf.Timestamp({
    seconds: new Long(1634648118),
    nanos: 215527000,
  }),
  finished: new protos.google.protobuf.Timestamp({
    seconds: new Long(1634648118),
    nanos: 311522000,
  }),
  modified: new protos.google.protobuf.Timestamp({
    seconds: new Long(1634648118),
    nanos: 310899000,
  }),
  fraction_completed: 1,
  last_run: new protos.google.protobuf.Timestamp({
    seconds: new Long(1634648118),
    nanos: 215527000,
  }),
  next_run: new protos.google.protobuf.Timestamp({
    seconds: new Long(1634648118),
    nanos: 215527100,
  }),
  num_runs: new Long(1),
};
