// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

    expect(parseSplatParams(match, history.location)).toEqual(
      "region=us-west/zone=a",
    );
  });

  it("trims out leading / from remaining path", () => {
    history.push("/overview/map/region=us-west/zone=a");
    match.path = "/overview/map";

    expect(parseSplatParams(match, history.location)).toEqual(
      "region=us-west/zone=a",
    );
  });

  it("returns empty string if path is fully matched", () => {
    history.push("/overview/map");
    match.path = "/overview/map";

    expect(parseSplatParams(match, history.location)).toEqual("");
  });
});
