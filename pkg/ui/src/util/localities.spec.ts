import { assert } from "chai";

import * as protos from "src/js/protos";
import { LocalityTier } from "src/redux/localities";
import { generateLocalityRoute, parseLocalityRoute, getNodeLocalityTiers } from "./localities";

describe("parseLocalityRoute", function() {
  describe("with an empty route", function() {
    it("returns an empty array when passed undefined", function() {
      const tiers = parseLocalityRoute(undefined);

      assert.deepEqual(tiers, []);
    });

    it("returns an empty array when passed an empty string", function() {
      const tiers = parseLocalityRoute("");

      assert.deepEqual(tiers, []);
    });
  });

  describe("with a single-segment route", function() {
    it("returns an array with a single tier", function() {
      const key = "region";
      const value = "us-east-1";

      const tiers = parseLocalityRoute(key + "=" + value);

      assert.deepEqual(tiers, [{ key, value }]);
    });
  });

  describe("with a multi-segment route", function() {
    it("returns an array with all the tiers in the route", function() {
      const expectedTiers: LocalityTier[] = [
        { key: "region", value: "us-east" },
        { key: "zone", value: "us-east-1" },
        { key: "datacenter", value: "us-east-1b" },
      ];

      const route = expectedTiers
        .map(({ key, value }) => key + "=" + value)
        .join("/");

      const tiers = parseLocalityRoute(route);

      assert.deepEqual(tiers, expectedTiers);
    });
  });
});

describe("generateLocalityRoute", function() {
  describe("with empty tiers", function() {
    it("returns an empty string", function() {
      const route = generateLocalityRoute([]);

      assert.equal(route, "");
    });
  });

  describe("with a single tier", function() {
    it("returns a route with a single segment", function() {
      const key = "region";
      const value = "us-east-1";

      const route = generateLocalityRoute([{ key, value }]);

      assert.equal(route, key + "=" + value);
    });
  });

  describe("with multiple tiers", function() {
    it("returns a route with a segment for each tier", function() {
      const tiers: LocalityTier[] = [
        { key: "region", value: "us-east" },
        { key: "zone", value: "us-east-1" },
        { key: "datacenter", value: "us-east-1b" },
      ];

      const expectedRoute = tiers
        .map(({ key, value }) => key + "=" + value)
        .join("/");

      const route = generateLocalityRoute(tiers);

      assert.equal(route, expectedRoute);
    });
  });
});

describe("getNodeLocalityTiers", function() {
  it("returns the locality of a node", function() {
    const tiers: protos.cockroach.roachpb.Tier$Properties[] = [
      { key: "region", value: "us-east" },
      { key: "zone", value: "us-east-1" },
      { key: "datacenter", value: "us-east-1b" },
    ];
    const node = {
      desc: {
        locality: {
          tiers: tiers,
        },
      },
    };

    const locality = getNodeLocalityTiers(node);

    assert.deepEqual(locality, tiers);
  });
});
