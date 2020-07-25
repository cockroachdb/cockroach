// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {analytics} from "src/redux/analytics";

export function trackTerminateSession () {
  const boundTrack = analytics.track.bind(analytics);
  (() => {
    boundTrack({
      event: "Terminate Session",
    });
  })();
}

export function trackTerminateQuery () {
  const boundTrack = analytics.track.bind(analytics);
  (() => {
    boundTrack({
      event: "Terminate Query",
    });
  })();
}
