import { assert } from "chai";

import { LocalityTier } from "src/redux/localities";
import { LocationTree } from "src/redux/locations";
import { findMostSpecificLocation } from "./locations";

const nycLocality: LocalityTier[] = [
  { key: "region", value: "us-east-1" },
  { key: "city", value: "nyc" },
];

describe("findMostSpecificLocation", function() {
  it("returns null when location tree is empty", function() {
    const locations: LocationTree = {};

    const location = findMostSpecificLocation(locations, nycLocality);

    assert.equal(location, null);
  });

  it("returns the location of a locality", function() {
    const locations = {
      region: {
        "us-east-1": {
          locality_key: "region",
          locality_value: "us-east-1",
          latitude: 12.3,
          longitude: 45.6,
        },
      },
    };

    const location = findMostSpecificLocation(locations, nycLocality);

    assert.deepEqual(location, locations.region["us-east-1"]);
  });

  it("finds the most specific location for a locality", function() {
    const locations = {
      region: {
        "us-east-1": {
          locality_key: "region",
          locality_value: "us-east-1",
          latitude: 12.3,
          longitude: 45.6,
        },
      },
      city: {
        "nyc": {
          locality_key: "city",
          locality_value: "nyc",
          latitude: 45.6,
          longitude: 78.9,
        },
      },
    };

    const location = findMostSpecificLocation(locations, nycLocality);

    assert.deepEqual(location, locations.city.nyc);
  });
});
