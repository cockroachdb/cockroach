// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { assert } from "chai";
import moment from "moment";
import { formatDuration } from ".";

describe("Jobs", () => {
  it("format duration", () => {
    assert.equal(formatDuration(moment.duration(0)), "00:00:00");
    assert.equal(formatDuration(moment.duration(5, "minutes")), "00:05:00");
    assert.equal(formatDuration(moment.duration(5, "hours")), "05:00:00");
    assert.equal(formatDuration(moment.duration(110, "hours")), "110:00:00");
    assert.equal(
      formatDuration(moment.duration(12345, "hours")),
      "12345:00:00",
    );
  });
});
