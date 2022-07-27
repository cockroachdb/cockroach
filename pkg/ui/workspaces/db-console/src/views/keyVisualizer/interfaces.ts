// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {cockroach} from "src/js";
import KeyVisSamplesResponse = cockroach.server.serverpb.KeyVisSamplesResponse;

export interface KeyVisualizerProps {


  // A dictionary of canvas y-offsets for a given pretty key
  yOffsetsForKey: Record<string, number>;

  // Used to compute the relative color of each bucket
  highestBucketInCurrentWindow: number;

  samples: KeyVisSamplesResponse["samples"];

  keys: KeyVisSamplesResponse["pretty_key_for_uuid"];
}
