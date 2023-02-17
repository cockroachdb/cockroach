// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { cockroach } from "src/js";
import KeyVisSamplesResponse = cockroach.server.serverpb.KeyVisSamplesResponse;
import IKeyVisSample = cockroach.server.serverpb.KeyVisSamplesResponse.IKeyVisSample;
import IBucket = cockroach.server.serverpb.KeyVisSamplesResponse.IBucket;

// SampleBucket uses hex encoded UUIDs to look up pretty keys in KeyVisualizerProps.keys.
// The hex encoding happens on the client after KeyVisSamplesResponse is received.
export interface SampleBucket extends IBucket {
  startKeyHex: string;
  endKeyHex: string;
}

export interface KeyVisSample extends IKeyVisSample {
  buckets: SampleBucket[];
}

export interface KeyVisualizerProps {
  // A dictionary of canvas y-offsets for a given pretty key
  yOffsetsForKey: Record<string, number>;

  // Used to compute the relative color of each bucket
  hottestBucket: number;

  samples: KeyVisSample[];

  keys: KeyVisSamplesResponse["pretty_key_for_uuid"];
}
