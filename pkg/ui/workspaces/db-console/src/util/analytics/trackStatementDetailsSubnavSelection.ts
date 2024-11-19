// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { analytics } from "src/redux/analytics";

export const track = (fn: Function) => (selection: string) => {
  fn({
    event: "SubNavigation Selection",
    properties: {
      selection,
    },
  });
};

export default function trackSubnavSelection(selection: string) {
  const boundTrack = analytics.track.bind(analytics);
  track(boundTrack)(selection);
}
