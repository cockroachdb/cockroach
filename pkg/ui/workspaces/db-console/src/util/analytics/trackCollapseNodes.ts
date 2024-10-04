// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { analytics } from "src/redux/analytics";

export const track = (fn: Function) => (collapsed: boolean) => {
  fn({
    event: "Collapse Nodes",
    properties: {
      collapsed: collapsed,
    },
  });
};

export default function trackCollapseNode(collapsed: boolean) {
  const boundTrack = analytics.track.bind(analytics);
  track(boundTrack)(collapsed);
}
