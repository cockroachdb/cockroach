// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "src/js/protos";

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
