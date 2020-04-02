// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
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
