// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSelector } from "reselect";

import * as protos from "src/js/protos";
import { AdminUIState } from "src/redux/state";
import { Pick } from "src/util/pick";

export type ILocation =
  protos.cockroach.server.serverpb.LocationsResponse.ILocation;

type LocationState = Pick<AdminUIState, "cachedData", "locations">;

export function selectLocationsRequestStatus(state: LocationState) {
  return state.cachedData.locations;
}

export function selectLocations(state: LocationState) {
  if (!state.cachedData.locations.data) {
    return [];
  }

  return state.cachedData.locations.data.locations;
}

const nestLocations = (data: ILocation[]): LocationTree => {
  return data.reduce((acc: LocationTree, location: ILocation) => {
    const { locality_key, locality_value } = location;

    if (!acc[locality_key]) {
      acc[locality_key] = {};
    }

    acc[locality_key][locality_value] = location;
    return acc;
  }, {});
};

export interface LocationTree {
  [key: string]: {
    [value: string]: ILocation;
  };
}

export const selectLocationTree = createSelector(
  selectLocations,
  (ls: ILocation[]) => nestLocations(ls), // TSLint won't let this be point-free
);
