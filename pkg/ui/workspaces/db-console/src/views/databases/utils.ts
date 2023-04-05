// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// createNodesByRegionMap creates a mapping of regions to nodes,
// based on the nodes list provided and nodeRegions, which is a full
// list of node id to the region it resides.
import * as protos from "src/js/protos";
import Long from "long";

type Timestamp = protos.google.protobuf.ITimestamp;

// makeTimestamp converts a string to a google.protobuf.Timestamp object.
export function makeTimestamp(date: string): Timestamp {
  return new protos.google.protobuf.Timestamp({
    seconds: new Long(new Date(date).getUTCSeconds()),
  });
}
