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
