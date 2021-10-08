// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { assert } from "chai";
import { createMemoryHistory, History } from "history";
import { match as Match } from "react-router-dom";
import { parseSplatParams } from "./parseSplatParams";

describe("parseSplatParams", () => {
  let history: History;
  let match: Match;

  beforeEach(() => {
    history = createMemoryHistory({ initialEntries: ["/"] });
    match = {
      path: "/",
      params: {},
      url: "http://localhost/",
      isExact: true,
    };
  });

  it("returns remaining part of location path", () => {
    history.push("/overview/map/region=us-west/zone=a");
    match.path = "/overview/map/";

    assert.equal(
      parseSplatParams(match, history.location),
      "region=us-west/zone=a",
    );
  });

  it("trims out leading / from remaining path", () => {
    history.push("/overview/map/region=us-west/zone=a");
    match.path = "/overview/map";

    assert.equal(
      parseSplatParams(match, history.location),
      "region=us-west/zone=a",
    );
  });

  it("returns empty string if path is fully matched", () => {
    history.push("/overview/map");
    match.path = "/overview/map";

    assert.equal(parseSplatParams(match, history.location), "");
  });
});
