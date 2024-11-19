// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { analytics } from "src/redux/analytics";

export const track = (fn: Function) => (scale: string) => {
  fn({
    event: "Time Scale Selected",
    properties: {
      timeScale: scale,
    },
  });
};

export default function trackTimeScaleSelected(scale: string) {
  const boundTrack = analytics.track.bind(analytics);
  track(boundTrack)(scale);
}
