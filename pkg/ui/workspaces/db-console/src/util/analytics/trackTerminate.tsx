// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { analytics } from "src/redux/analytics";

export function trackTerminateSession() {
  const boundTrack = analytics.track.bind(analytics);
  (() => {
    boundTrack({
      event: "Terminate Session",
    });
  })();
}

export function trackTerminateQuery() {
  const boundTrack = analytics.track.bind(analytics);
  (() => {
    boundTrack({
      event: "Terminate Query",
    });
  })();
}
