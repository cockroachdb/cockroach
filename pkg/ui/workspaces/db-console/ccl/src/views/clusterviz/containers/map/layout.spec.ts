// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { LocalityTree } from "src/redux/localities";
import { LocationTree } from "src/redux/locations";

import { renderAsMap } from "./layout";

const locationTree: LocationTree = {
  zone: {
    "us-west": {
      latitude: 12.3,
      longitude: 45.6,
      locality_key: "zone",
      locality_value: "us-west",
    },
  },
};

function shouldRenderAsCircle(locality: LocalityTree) {
  expect(renderAsMap(locationTree, locality)).toEqual(false);
}

function shouldRenderAsMap(locality: LocalityTree) {
  expect(renderAsMap(locationTree, locality)).toEqual(true);
}

describe("renderAsMap", function () {
  describe("for locality with child nodes", function () {
    it("returns false", function () {
      const locality: LocalityTree = {
        tiers: [],
        localities: {
          zone: {
            "us-west": {
              tiers: [{ key: "zone", value: "us-west" }],
              localities: {},
              nodes: [
                {
                  desc: {
                    node_id: 1,
                  },
                },
              ],
            },
          },
        },
        nodes: [
          {
            desc: {
              node_id: 1,
            },
          },
        ],
      };

      shouldRenderAsCircle(locality);
    });
  });

  describe("when child locality does not have location", function () {
    it("returns false", function () {
      const locality: LocalityTree = {
        tiers: [],
        localities: {
          zone: {
            "us-east": {
              tiers: [{ key: "zone", value: "us-east" }],
              localities: {},
              nodes: [
                {
                  desc: {
                    node_id: 1,
                  },
                },
              ],
            },
          },
        },
        nodes: [],
      };

      shouldRenderAsCircle(locality);
    });
  });

  describe("when child locality has location", function () {
    it("returns true", function () {
      const locality: LocalityTree = {
        tiers: [],
        localities: {
          zone: {
            "us-west": {
              tiers: [{ key: "zone", value: "us-west" }],
              localities: {},
              nodes: [
                {
                  desc: {
                    node_id: 1,
                  },
                },
              ],
            },
          },
        },
        nodes: [],
      };

      shouldRenderAsMap(locality);
    });
  });
});
