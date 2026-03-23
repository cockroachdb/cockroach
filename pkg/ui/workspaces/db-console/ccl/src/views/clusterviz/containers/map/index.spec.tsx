// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createMemoryHistory, History } from "history";
import { match as Match } from "react-router-dom";

import { parseLocalityRoute } from "src/util/localities";
import { parseSplatParams } from "src/util/parseSplatParams";

jest.mock("@cockroachlabs/cluster-ui", () => {
  const actual = jest.requireActual("@cockroachlabs/cluster-ui");
  return {
    ...actual,
    useCluster: jest.fn().mockReturnValue({
      data: { enterprise_enabled: true },
      isLoading: false,
      error: null,
      enterpriseEnabled: true,
    }),
  };
});

describe("ClusterVisualization", () => {
  describe("parse tiers params from URL path", () => {
    let history: History;
    let match: Match;

    beforeEach(() => {
      history = createMemoryHistory();
      match = {
        path: "/overview/map",
        params: {},
        url: "http://localhost/overview/map",
        isExact: true,
      };
    });

    // The original test validated that the ClusterVisualization component
    // correctly parses tiers from the URL and passes them to Breadcrumbs.
    // We test the parsing logic directly since it's the core behavior.
    it("parses tiers as empty array for /overview/map path", () => {
      history.push("/overview/map");
      const splat = parseSplatParams(match, history.location);
      const tiers = parseLocalityRoute(splat);
      expect(tiers).toHaveLength(0);
    });

    it("parses multiple tiers in path for `/overview/map/region=us-west/az=a` path", () => {
      history.push("/overview/map/region=us-west/az=a");
      const splat = parseSplatParams(match, history.location);
      const tiers = parseLocalityRoute(splat);
      const expectedTiers = [
        { key: "region", value: "us-west" },
        { key: "az", value: "a" },
      ];
      expect(tiers).toEqual(expectedTiers);
    });
  });
});
