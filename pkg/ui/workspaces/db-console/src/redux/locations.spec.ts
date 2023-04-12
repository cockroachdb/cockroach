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

import * as protos from "src/js/protos";
import { ILocation, selectLocations, selectLocationTree } from "./locations";

function makeStateWithLocations(locationData: ILocation[]) {
  return {
    cachedData: {
      locations: {
        data: new protos.cockroach.server.serverpb.LocationsResponse({
          locations: locationData,
        }),
        inFlight: false,
        valid: true,
      },
    },
  };
}

describe("selectLocations", function() {
  it("returns an empty array if location data is missing", function() {
    const state = {
      cachedData: {
        locations: {
          inFlight: false,
          valid: false,
        },
      },
    };

    assert.deepEqual(selectLocations(state), []);
  });

  // Data must still be returned while the state is invalid to avoid
  // flickering while the data is being refreshed.
  it("returns location data if it exists but is invalid", function() {
    const locationData = [
      {
        locality_key: "city",
        locality_value: "nyc",
        latitude: 123,
        longitude: 456,
      },
    ];
    const state = makeStateWithLocations(locationData);
    state.cachedData.locations.valid = false;

    assert.deepEqual(selectLocations(state), locationData);
  });

  it("returns an empty array if location data is null", function() {
    const state = makeStateWithLocations(null);

    assert.deepEqual(selectLocations(state), []);
  });

  it("returns location data if valid", function() {
    const locationData = [
      {
        locality_key: "city",
        locality_value: "nyc",
        latitude: 123,
        longitude: 456,
      },
    ];
    const state = makeStateWithLocations(locationData);

    const result = selectLocations(state);

    assert.deepEqual(result, locationData);
  });
});

describe("selectLocationTree", function() {
  it("returns an empty object if locations are empty", function() {
    const state = makeStateWithLocations([]);

    assert.deepEqual(selectLocationTree(state), {});
  });

  it("makes a key for each locality tier in locations", function() {
    const tiers = ["region", "city", "data-center", "rack"];
    const locations = tiers.map(tier => ({ locality_key: tier }));
    const state = makeStateWithLocations(locations);

    assert.hasAllKeys(selectLocationTree(state), tiers);
  });

  it("makes a key for each locality value in each tier", function() {
    const cities = ["nyc", "sf", "walla-walla"];
    const dataCenters = ["us-east-1", "us-west-1"];
    const cityLocations = cities.map(city => ({
      locality_key: "city",
      locality_value: city,
    }));
    const dcLocations = dataCenters.map(dc => ({
      locality_key: "data-center",
      locality_value: dc,
    }));
    const state = makeStateWithLocations(cityLocations.concat(dcLocations));

    const tree = selectLocationTree(state);

    assert.hasAllKeys(tree, ["city", "data-center"]);
    assert.hasAllKeys(tree.city, cities);
    assert.hasAllKeys(tree["data-center"], dataCenters);
  });

  it("returns each location under its key and value", function() {
    const us = {
      locality_key: "country",
      locality_value: "US",
      latitude: 1,
      longitude: 2,
    };
    const nyc = {
      locality_key: "city",
      locality_value: "NYC",
      latitude: 3,
      longitude: 4,
    };
    const sf = {
      locality_key: "city",
      locality_value: "SF",
      latitude: 5,
      longitude: 6,
    };
    const locations = [us, nyc, sf];
    const state = makeStateWithLocations(locations);

    const tree = selectLocationTree(state);

    assert.deepEqual(tree.country.US, us);
    assert.deepEqual(tree.city.NYC, nyc);
    assert.deepEqual(tree.city.SF, sf);
  });
});
