/**
 * This module maintains the state of a read-only, periodically refreshed query
 * for the raft debug status of all nodes in the cluster. Data is fetched from
 * the '/_status/raft' endpoint.
 */

import _ = require("lodash");
import { Dispatch } from "redux";
import { Action, PayloadAction } from "../interfaces/action";
import { raftDebug } from "../util/api";

import "isomorphic-fetch";

export const REQUEST = "cockroachui/raft/REQUEST";
export const RECEIVE = "cockroachui/raft/RECEIVE";
export const ERROR = "cockroachui/raft/ERROR";
export const INVALIDATE = "cockroachui/raft/INVALIDATE";

type RaftDebugResponse = cockroach.server.RaftDebugResponse;

/**
 * Represents the current state of a raft debug status query.
 */
export class RaftDebugState {
  // True if an asynchronous fetch is currently in progress.
  inFlight = false;
  // True if the contents of 'statuses' is considered current.
  isValid = false;
  // Holds the last error returned from a failed query.
  lastError: Error;
  // Holds the last set of objects queried from the server.
  statuses: RaftDebugResponse;
}

/**
 * Redux reducer which processes actions related to the raft debug status query.
 */
export default function reducer(state: RaftDebugState = new RaftDebugState(), action: Action): RaftDebugState {
  switch (action.type) {
    case REQUEST:
      // A request is in progress.
      state = _.clone(state);
      state.inFlight = true;
      return state;
    case RECEIVE:
      // The results of a request have been received.
      let { payload: statuses } = action as PayloadAction<RaftDebugResponse>;
      state = _.clone(state);
      state.inFlight = false;
      state.isValid = true;
      state.statuses = statuses;
      state.lastError = null;
      return state;
    case ERROR:
      // A request failed.
      let { payload: error } = action as PayloadAction<Error>;
      state = _.clone(state);
      state.inFlight = false;
      state.isValid = false;
      state.lastError = error;
      return state;
    case INVALIDATE:
      // The currently cached request should no longer be considered valid.
      state = _.clone(state);
      state.isValid = false;
      return state;
    default:
      return state;
  }
}

/**
 * requestRanges indicates that an asynchronous raft status request has been
 * started.
 */
export function requestRanges(): Action {
  return {
    type: REQUEST,
  };
}

/**
 * receiveRanges indicates that raft status has been successfully fetched from
 * the server.
 *
 * @param statuses Status data returned from the server.
 */
export function receiveRanges(statuses: RaftDebugResponse): PayloadAction<RaftDebugResponse> {
  return {
    type: RECEIVE,
    payload: statuses,
  };
}

/**
 * errorRanges indicates that an error occurred while fetching raft status.
 *
 * @param error Error that occurred while fetching.
 */
export function errorRanges(error: Error): PayloadAction<Error> {
  return {
    type: ERROR,
    payload: error,
  };
}

/**
 * invalidateRanges will invalidate the currently stored raft status results.
 */
export function invalidateRanges(): Action {
  return {
    type: INVALIDATE,
  };
}

// The duration for which a status result is considered valid.
const statusValidDurationMS = 10 * 1000;

/**
 * refreshRaft is the primary action creator that should be used with raft.
 * Dispatching it will attempt to asynchronously refresh the raft status if its
 * results are no longer considered valid.
 */
export function refreshRaft() {
  return (dispatch: Dispatch, getState: () => any): void => {
    let { raft }: {raft: RaftDebugState} = getState();

    // If there is a query in flight, or if the most recent results are still
    // valid, do nothing.
    if (raft.inFlight || raft.isValid) {
      return;
    }

    // Note that a query is currently in flight.
    dispatch(requestRanges());

    // Fetch raft status from the servers and convert it to JSON.
    raftDebug().then((response) => {
      // Dispatch the processed results to the store.
      dispatch(receiveRanges(response));

      // Set a timeout which will later invalidate the results.
      setTimeout(() => dispatch(invalidateRanges()), statusValidDurationMS);
    }).catch((error) => {
      // If an error occurred during the fetch, dispatch the received error to
      // the store.
      console.log("error", error);
      dispatch(errorRanges(error));
    });
  };
}

