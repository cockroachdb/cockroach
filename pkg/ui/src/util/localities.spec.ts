import { assert } from "chai";

import { LocalityTier } from "src/redux/localities";
import { parseLocalityRoute } from "./localities";

describe("parseLocalityRoute", function() {
  describe("with an empty route", function() {
    it("returns an empty array when passed undefined", function () {
      const tiers = parseLocalityRoute(undefined);
      assert.deepEqual(tiers, []);
    });

    it("returns an empty array when passed an empty string", function () {
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
