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

import d3 from "d3";
import { createSelector } from "reselect";

import * as protos from "src/js/protos";
import { AdminUIState } from "src/redux/state";
import { Pick } from "src/util/pick";

export type ILocation = protos.cockroach.server.serverpb.LocationsResponse.ILocation;

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
  .key((loc: ILocation) => loc.locality_key)
  .key((loc: ILocation) => loc.locality_value)
  .rollup((locations) => locations[0]) // cannot collide since ^^ is primary key
  .map;

export interface LocationTree {
  [key: string]: {
    [value: string]: ILocation,
  };
}

export const selectLocationTree = createSelector(
  selectLocations,
  (ls: ILocation[]) => nestLocations(ls), // TSLint won't let this be point-free
);
