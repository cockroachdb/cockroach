// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

import { AdminUIState } from "src/redux/state";

/**
 * eventsSelector selects the list of events from the store.
 */
export function eventsSelector (state: AdminUIState) {
  return state.cachedData.events.data && state.cachedData.events.data.events;
}

/**
 * eventsValidSelector selects a flag indicating if the events have been loaded successfully.
 */
export function eventsValidSelector (state: AdminUIState) {
  return state.cachedData.events.valid;
}
