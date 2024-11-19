// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AdminUIState } from "src/redux/state";

/**
 * eventsSelector selects the list of events from the store.
 */
export function eventsSelector(state: AdminUIState) {
  return state.cachedData.events?.data?.results;
}

/**
 * eventsValidSelector selects a flag indicating if the events have been loaded successfully.
 */
export function eventsValidSelector(state: AdminUIState) {
  return state.cachedData.events.valid;
}

export function eventsLastErrorSelector(state: AdminUIState) {
  return state.cachedData.events.lastError;
}

export const eventsMaxApiReached = (state: AdminUIState): boolean => {
  return !!state.cachedData.events?.data?.maxSizeReached;
};
