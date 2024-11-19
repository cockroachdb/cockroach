// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { analytics } from "src/redux/analytics";

export const track = (fn: Function) => (targetText: string) => {
  fn({
    event: "Link to Docs",
    properties: {
      linkText: targetText,
    },
  });
};

export default function trackDocsLink(targetText: string) {
  const boundTrack = analytics.track.bind(analytics);
  track(boundTrack)(targetText);
}
