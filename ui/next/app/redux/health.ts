/**
 * This module maintains the state of read-only data about health
 * Data is fetched from the '/_admin/v1/health' endpoint via 'util/api'
 * Currently the data is always refreshed.
 */

import * as _ from "lodash";
import { Dispatch } from "redux";
import { Action, PayloadAction } from "../interfaces/action";
import { getHealth } from "../util/api";

type HealthResponseMessage = cockroach.server.serverpb.HealthResponseMessage;

export const REQUEST = "cockroachui/health/REQUEST";
export const RECEIVE = "cockroachui/health/RECEIVE";
export const ERROR = "cockroachui/health/ERROR";
export const INVALIDATE = "cockroachui/health/INVALIDATE";

export class HealthState {
  data: HealthResponseMessage;
  inFlight = false;
  valid = false;
  lastError: Error;
}

/**
 * Redux reducer which processes actions related to the health query.
 */
export default function reducer(state: HealthState = new HealthState(), action: Action): HealthState {
  switch (action.type) {
    case REQUEST:
      // A request is in progress.
      state = _.clone(state);
      state.inFlight = true;
      return state;
    case RECEIVE:
      // The results of a request have been received.
      let { payload } = action as PayloadAction<HealthResponseMessage>;
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

export function requestHealth(): Action {
  return {
    type: REQUEST,
  };
}

export function receiveHealth(health: HealthResponseMessage): PayloadAction<HealthResponseMessage> {
  return {
    type: RECEIVE,
    payload: health,
  };
}

export function errorHealth(error: Error): PayloadAction<Error> {
  return {
    type: ERROR,
    payload: error,
  };
}

export function invalidateHealth(): Action {
  return {
    type: INVALIDATE,
  };
}

/**
 * refreshHealth is the primary action creator that should be used to determine cluster health.
 * Dispatching it will attempt to asynchronously refresh the health
 * if its results are no longer considered valid.
 */
export function refreshHealth() {
  return (dispatch: Dispatch, getState: () => any) => {
    let { health }: {health: HealthState} = getState();

    // TODO: Currently we only refresh when the page is revisited. Eventually
    // we should refresh on a timer like other components.

    // Don't refresh if a request is already in flight
    if (health && (health.inFlight || health.valid)) {
      return;
    }

    // Note that a query is currently in flight.
    dispatch(requestHealth());
    // Fetch health from the servers and convert it to JSON.
    // The promise is returned for testing.
    return getHealth().then((e) => {
      // Dispatch the processed results to the store.
      dispatch(receiveHealth(e));
    }).catch((error: Error) => {
      // If an error occurred during the fetch, dispatch the received error to
      // the store.
      dispatch(errorHealth(error));
    }).then(() => {
      // refresh health every 2 seconds
      setTimeout(() => dispatch(invalidateHealth()), 2000);
    });
  };
}
