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
