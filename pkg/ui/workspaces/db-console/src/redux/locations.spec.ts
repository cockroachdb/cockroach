// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as protos from "@cockroachlabs/crdb-protobuf-client";

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
        unauthorized: false,
      },
    },
  };
}

describe("selectLocations", function () {
  it("returns an empty array if location data is missing", function () {
    const state = {
      cachedData: {
        locations: {
          inFlight: false,
          valid: false,
          unauthorized: false,
        },
      },
    };

    expect(selectLocations(state)).toEqual([]);
  });

  // Data must still be returned while the state is invalid to avoid
  // flickering while the data is being refreshed.
  it("returns location data if it exists but is invalid", function () {
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

    expect(selectLocations(state)).toEqual(locationData);
  });

  it("returns an empty array if location data is null", function () {
    const state = makeStateWithLocations(null);

    expect(selectLocations(state)).toEqual([]);
  });

  it("returns location data if valid", function () {
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

    expect(result).toEqual(locationData);
  });
});

describe("selectLocationTree", function () {
  it("returns an empty object if locations are empty", function () {
    const state = makeStateWithLocations([]);

    expect(selectLocationTree(state)).toEqual({});
  });

  it("makes a key for each locality tier in locations", function () {
    const tiers = ["region", "city", "data-center", "rack"];
    const locations = tiers.map(tier => ({ locality_key: tier }));
    const state = makeStateWithLocations(locations);

    tiers.forEach(tier =>
      expect(Object.keys(selectLocationTree(state))).toContain(tier),
    );
  });

  it("makes a key for each locality value in each tier", function () {
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

    expect(Object.keys(tree)).toContain("city");
    expect(Object.keys(tree)).toContain("data-center");
    cities.forEach(city => expect(Object.keys(tree.city)).toContain(city));
    dataCenters.forEach(dc =>
      expect(Object.keys(tree["data-center"])).toContain(dc),
    );
  });

  it("returns each location under its key and value", function () {
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

    expect(tree.country.US).toEqual(us);
    expect(tree.city.NYC).toEqual(nyc);
    expect(tree.city.SF).toEqual(sf);
  });
});
