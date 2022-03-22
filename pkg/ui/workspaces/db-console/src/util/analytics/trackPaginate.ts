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
