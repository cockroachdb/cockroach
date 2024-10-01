// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { analytics } from "src/redux/analytics";

export const track = (fn: Function) => (statement: string) => {
  fn({
    event: "Diagnostics Bundle Cancellation",
    properties: {
      fingerprint: statement,
    },
  });
};

export default function trackCancelDiagnosticsBundle(statement: string) {
  const boundTrack = analytics.track.bind(analytics);
  track(boundTrack)(statement);
}
