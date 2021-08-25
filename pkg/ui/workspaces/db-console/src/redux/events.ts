// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { AdminUIState } from "src/redux/state";

/**
 * eventsSelector selects the list of events from the store.
 */
export function eventsSelector(state: AdminUIState) {
  return state.cachedData.events.data && state.cachedData.events.data.events;
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
