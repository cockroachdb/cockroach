// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import { assert } from "chai";

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
  assert.equal(
    renderAsMap(locationTree, locality),
    false,
    `${JSON.stringify(
      locationTree,
    )} should render as a circle, but will render as a map`,
  );
}

function shouldRenderAsMap(locality: LocalityTree) {
  assert.equal(
    renderAsMap(locationTree, locality),
    true,
    `${JSON.stringify(
      locationTree,
    )} should render as a map, but will render as a circle`,
  );
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
