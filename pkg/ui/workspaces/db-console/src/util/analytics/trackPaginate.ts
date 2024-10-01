// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { analytics } from "src/redux/analytics";

export const track = (fn: Function) => (page: number) => {
  fn({
    event: "Paginate",
    properties: {
      selectedPage: page,
    },
  });
};

export default function trackPaginate(pageNumber: number) {
  const boundTrack = analytics.track.bind(analytics);
  track(boundTrack)(pageNumber);
}
