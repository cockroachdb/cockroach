// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { analytics } from "src/redux/analytics";

export const track = (fn: Function) => (direction: string) => {
  fn({
    event: "Time Frame Change",
    properties: {
      direction,
    },
  });
};

export default function trackTimeFrameChange(direction: string) {
  const boundTrack = analytics.track.bind(analytics);
  track(boundTrack)(direction);
}
