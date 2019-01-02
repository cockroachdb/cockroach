// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import { assert } from "chai";

import { LocalityTier, LocalityTree } from "src/redux/localities";
import { LocationTree } from "src/redux/locations";
import { findMostSpecificLocation, findOrCalculateLocation } from "./locations";

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

describe("findOrCalculateLocation", function() {
  describe("when locality has location", function() {
    it("returns the locality's location", function() {
      const locations = {
        city: {
          "nyc": {
            locality_key: "region",
            locality_value: "us-east-1",
            latitude: 12.3,
            longitude: 45.6,
          },
        },
      };

      const locality: LocalityTree = {
        localities: {},
        nodes: [],
        tiers: nycLocality,
      };

      const location = findOrCalculateLocation(locations, locality);

      assert.deepEqual(location, locations.city.nyc);
    });
  });

  describe("when locality doesn't have location", function() {
    describe("when locality has nodes", function() {
      it("returns null", function() {
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

        const locality: LocalityTree = {
          localities: {},
          nodes: [
            {
              desc: {
                node_id: 1,
                locality: {
                  tiers: nycLocality,
                },
              },
            },
          ],
          tiers: nycLocality,
        };

        const location = findOrCalculateLocation(locations, locality);

        assert.equal(location, null);
      });
    });

    describe("when locality has children without locations", function() {
      it("returns null", function() {
        const locations = {};

        const locality: LocalityTree = {
          localities: {
            city: {
              nyc: {
                localities: {},
                nodes: [],
                tiers: nycLocality,
              },
            },
          },
          nodes: [],
          tiers: [nycLocality[0]],
        };

        const location = findOrCalculateLocation(locations, locality);

        assert.equal(location, null);
      });
    });

    describe("when locality has children with locations", function() {
      // TODO(couchand): actually test the centroid
      it("returns their centroid", function() {
        const locations = {
          city: {
            "nyc": {
              locality_key: "region",
              locality_value: "us-east-1",
              latitude: 12.3,
              longitude: 45.6,
            },
          },
        };

        const locality: LocalityTree = {
          localities: {
            city: {
              nyc: {
                localities: {},
                nodes: [],
                tiers: nycLocality,
              },
            },
          },
          nodes: [],
          tiers: [nycLocality[0]],
        };

        const location = findOrCalculateLocation(locations, locality);

        assert.equal(location.latitude, locations.city.nyc.latitude);
        assert.equal(location.longitude, locations.city.nyc.longitude);
      });
    });
  });
});
