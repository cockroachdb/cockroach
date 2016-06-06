/**
 * This module maintains the state of read-only data about events
 * Data is fetched from the '/_admin/v1/events' endpoint via 'util/api'
 * Currently the data is always refreshed.
 */

import * as _ from "lodash";
import { Dispatch } from "redux";
import { Action, PayloadAction } from "../interfaces/action";
import { getEvents } from "../util/api";

type EventsResponseMessage = cockroach.server.serverpb.EventsResponseMessage;

export const REQUEST = "cockroachui/events/REQUEST";
export const RECEIVE = "cockroachui/events/RECEIVE";
export const ERROR = "cockroachui/events/ERROR";
export const INVALIDATE = "cockroachui/events/INVALIDATE";

export class EventsState {
  data: EventsResponseMessage;
  inFlight = false;
  valid = false;
  lastError: Error;
}

/**
 * Redux reducer which processes actions related to the events list query.
 */
export default function reducer(state: EventsState = new EventsState(), action: Action): EventsState {
  switch (action.type) {
    case REQUEST:
      // A request is in progress.
      state = _.clone(state);
      state.inFlight = true;
      return state;
    case RECEIVE:
      // The results of a request have been received.
      let { payload } = action as PayloadAction<cockroach.server.serverpb.EventsResponseMessage>;
      state = _.clone(state);
      state.inFlight = false;
      state.data = payload;
      state.valid = true;
      state.lastError = null;
      return state;
    case ERROR:
      // A request failed.
      let { payload: error } = action as PayloadAction<Error>;
      state = _.clone(state);
      state.inFlight = false;
      state.lastError = error;
      state.valid = false;
      return state;
    case INVALIDATE:
      state = _.clone(state);
      state.valid = false;
      return state;
    default:
      return state;
  }
}

interface WithID<T> {
  id: string;
  data?: T;
}

export function requestEvents(): Action {
  return {
    type: REQUEST,
  };
}

export function receiveEvents(events: EventsResponseMessage): PayloadAction<EventsResponseMessage> {
  return {
    type: RECEIVE,
    payload: events,
  };
}

export function errorEvents(error: Error): PayloadAction<Error> {
  return {
    type: ERROR,
    payload: error,
  };
}

export function invalidateEvents(): Action {
  return {
    type: INVALIDATE,
  };
}

/**
 * refreshEvents is the primary action creator that should be used with a events
 * list. Dispatching it will attempt to asynchronously refresh the events list
 * if its results are no longer considered valid.
 */
export function refreshEvents() {
  return (dispatch: Dispatch, getState: () => any) => {
    let { events }: {events: EventsState} = getState();

    // TODO: Currently we only refresh when the page is revisited. Eventually
    // we should refresh on a timer like other components.

    // Don't refresh if a request is already in flight
    if (events && events.inFlight) {
      return;
    }

    // Note that a query is currently in flight.
    dispatch(requestEvents());
    // Fetch events list from the servers and convert it to JSON.
    // The promise is returned for testing.
    return getEvents().then((e) => {
      // Dispatch the processed results to the store.
      dispatch(receiveEvents(e));
    }).catch((error: Error) => {
      // If an error occurred during the fetch, dispatch the received error to
      // the store.
      dispatch(errorEvents(error));
    });
  };
}
