// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { analytics } from "src/redux/analytics";

export const track = (fn: Function) => (metric: string) => {
  fn({
    event: "Custom Metric Selected",
    properties: {
      metric,
    },
  });
};

export default function trackMetricSelected(metric: string) {
  const boundTrack = analytics.track.bind(analytics);
  track(boundTrack)(metric);
}
