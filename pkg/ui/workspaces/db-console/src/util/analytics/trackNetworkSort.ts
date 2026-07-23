// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { analytics } from "src/redux/analytics";

export const track = (fn: Function) => (sortBy: string) => {
  fn({
    event: "Sort Network Diagnostics",
    properties: {
      sortBy,
    },
  });
};

export default function trackNetworkSort(sortBy: string) {
  const boundTrack = analytics.track.bind(analytics);
  track(boundTrack)(sortBy);
}
