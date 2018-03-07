import d3 from "d3";
import { createSelector } from "reselect";

import * as protos from "src/js/protos";
import { AdminUIState } from "src/redux/state";
import { Pick } from "src/util/pick";

export type Location = protos.cockroach.server.serverpb.LocationsResponse.Location$Properties;

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

const nestLocations = d3.nest()
  .key((loc: Location) => loc.locality_key)
  .key((loc: Location) => loc.locality_value)
  .rollup((locations) => locations[0]) // cannot collide since ^^ is primary key
  .map;

export interface LocationTree {
  [key: string]: {
    [value: string]: Location,
  };
}

export const selectLocationTree = createSelector(
  selectLocations,
  (ls: Location[]) => nestLocations(ls), // TSLint won't let this be point-free
);
