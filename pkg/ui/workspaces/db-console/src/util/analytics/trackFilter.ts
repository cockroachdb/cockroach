// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { analytics } from "src/redux/analytics";

export const track = (fn: Function) => (filter: string, value: string) => {
  fn({
    event: `${filter} Filter`,
    properties: {
      selectedFilter: value,
    },
  });
};

export default function trackFilter(filter: string, value: string) {
  const boundTrack = analytics.track.bind(analytics);
  track(boundTrack)(filter, value);
}
